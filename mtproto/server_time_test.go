package mtproto

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gotd/neo"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/rpc"
	"github.com/gotd/td/tg"
)

// newTimeOffsetConn builds a Conn (via New, so it has a real rpc engine for
// NotifyError) wired with a neo mock clock at the given instant and a shared
// server-time offset store. The msg_id generator is anchored to the server
// clock (clock + offset) just as production wiring does.
func newTimeOffsetConn(t *testing.T, now time.Time) (*Conn, *ServerTimeOffset) {
	t.Helper()
	mockClock := neo.NewTime(now)
	offset := NewServerTimeOffset(0)

	var engine *rpc.Engine
	engine = rpc.New(func(ctx context.Context, msgID int64, seqNo int32, in bin.Encoder) error {
		var b bin.Buffer
		if err := b.Encode(&tg.Config{}); err != nil {
			return err
		}
		return engine.NotifyResult(msgID, &b)
	}, rpc.Options{})

	c := New(nil, Options{
		Logger:     zap.NewNop(),
		Random:     rand.New(rand.NewSource(1)),
		Key:        crypto.Key{}.WithID(),
		Clock:      mockClock,
		TimeOffset: offset,
		MessageID:  proto.NewMessageIDGen(newServerClock(mockClock, offset).Now),
		Handler:    newTestHandler(),
		engine:     engine,
	})
	return c, offset
}

// TestConn_handleSessionCreated_LearnsOffset asserts that new_session_created
// with a first msg_id stamped at now+T teaches the connection a server-time
// offset of ~+T, and that a subsequently generated msg_id lands within ~1s of
// now+T (proving msg_id generation now tracks the server clock).
func TestConn_handleSessionCreated_LearnsOffset(t *testing.T) {
	a := require.New(t)

	now := time.Unix(1_700_000_000, 0)
	const skew = 120 * time.Second

	c, offset := newTimeOffsetConn(t, now)

	serverTime := now.Add(skew)
	msgID := proto.NewMessageID(serverTime, proto.MessageFromClient)

	buf := bin.Buffer{}
	a.NoError(buf.Encode(&mt.NewSessionCreated{
		FirstMsgID: int64(msgID),
		UniqueID:   10,
		ServerSalt: 10,
	}))
	a.NoError(c.handleSessionCreated(&buf))

	// Offset learned ~ +120s (allow 1s rounding from Unix-second truncation).
	a.InDelta(int(skew.Seconds()), offset.Load(), 1, "offset must be learned from first msg id")

	// A freshly generated client msg_id must now sit within ~1s of now+T.
	genTime := proto.MessageID(c.newMessageID()).Time()
	a.InDelta(serverTime.Unix(), genTime.Unix(), 1,
		"generated msg_id time must track server clock after offset learned")
}

// TestConn_handleBadMsg_Code16CorrectsOffset asserts that a bad_msg_notification
// with code 16 (msg_id too low) and a bad_msg_id stamped at now+T re-learns the
// offset to ~+T. Code 16 is an upward correction, so the session_id must NOT
// rotate (offset correction + retry, not session recreate).
func TestConn_handleBadMsg_Code16CorrectsOffset(t *testing.T) {
	a := require.New(t)

	now := time.Unix(1_700_000_000, 0)
	const skew = 90 * time.Second

	c, offset := newTimeOffsetConn(t, now)
	setSession(c, 4242, 7)

	// Server reports our msg_id was too low. The carrying message's envelope
	// msg_id is server-stamped at now+T and is the authoritative server clock.
	serverTime := now.Add(skew)
	serverMsgID := int64(proto.NewMessageID(serverTime, proto.MessageServerResponse))
	// bad_msg_id (our own rejected id) is irrelevant to offset learning.
	badMsgID := int64(proto.NewMessageID(now, proto.MessageFromClient))

	a.NoError(c.handleBadMsg(serverMsgID, encodeBadMsgNotification(t, badMsgID, 16)))

	a.InDelta(int(skew.Seconds()), offset.Load(), 1, "code 16 must re-learn offset from server-stamped envelope")
	a.Equal(int64(4242), c.session().ID, "upward correction must NOT rotate session")
}

// TestConn_handleBadMsg_Code17DownwardRecreatesSession asserts that a code-17
// (msg_id too high) correction that lowers the offset rotates the session
// instead of emitting a lower msg_id mid-session.
func TestConn_handleBadMsg_Code17DownwardRecreatesSession(t *testing.T) {
	a := require.New(t)

	now := time.Unix(1_700_000_000, 0)

	c, offset := newTimeOffsetConn(t, now)
	// Start with a large positive offset so the correction is downward.
	offset.Store(600)
	setSession(c, 9999, 5)

	// The carrying message's envelope is server-stamped at the real server clock
	// (only +60s); our +600s estimate was too high (code 17). bad_msg_id is our
	// own rejected (too-high) id and must not drive the correction.
	serverTime := now.Add(60 * time.Second)
	serverMsgID := int64(proto.NewMessageID(serverTime, proto.MessageServerResponse))
	badMsgID := int64(proto.NewMessageID(now.Add(600*time.Second), proto.MessageFromClient))

	a.NoError(c.handleBadMsg(serverMsgID, encodeBadMsgNotification(t, badMsgID, 17)))

	a.InDelta(60, offset.Load(), 1, "code 17 must lower the offset to server clock")
	a.NotEqual(int64(9999), c.session().ID, "downward correction must recreate session")
	a.Zero(seqno(c), "downward correction must reset seqno")
}

// TestConn_handleBadMsg_Code17ReAnchorsMsgID asserts that after a code-17
// downward correction the NEXT generated client msg_id actually drops to the
// corrected (lower) server clock. The msg_id generator keeps a monotonic
// high-water mark, so once ids have flowed at the too-high offset, lowering the
// offset alone is not enough — recreateSession must also re-anchor the
// generator. Without that, the retried msg_id stays at the old too-high value
// and the server rejects with code 17 again.
func TestConn_handleBadMsg_Code17ReAnchorsMsgID(t *testing.T) {
	a := require.New(t)

	now := time.Unix(1_700_000_000, 0)

	c, offset := newTimeOffsetConn(t, now)
	// Start with a large positive offset so the correction is downward, then
	// generate a msg_id at that high offset to bump the generator's high-water
	// mark to ~now+600s (mimicking ids that already flowed mid-session).
	offset.Store(600)
	setSession(c, 9999, 5)

	highMsgID := proto.MessageID(c.newMessageID())
	a.InDelta(now.Add(600*time.Second).Unix(), highMsgID.Time().Unix(), 1,
		"sanity: generated msg_id tracks the high offset before correction")

	// Server-stamped carrying envelope at the real (only +60s) server clock.
	serverTime := now.Add(60 * time.Second)
	serverMsgID := int64(proto.NewMessageID(serverTime, proto.MessageServerResponse))
	badMsgID := int64(proto.NewMessageID(now.Add(600*time.Second), proto.MessageFromClient))

	a.NoError(c.handleBadMsg(serverMsgID, encodeBadMsgNotification(t, badMsgID, 17)))

	// The next generated msg_id must drop to the corrected (~now+60s) clock, not
	// stay pinned just above the old ~now+600s high-water mark.
	nextMsgID := proto.MessageID(c.newMessageID())
	a.InDelta(serverTime.Unix(), nextMsgID.Time().Unix(), 1,
		"next msg_id must re-anchor to the corrected server clock after downward correction")
	a.Less(int64(nextMsgID), int64(highMsgID),
		"next msg_id must be lower than the pre-correction high-water id")
}
