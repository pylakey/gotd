package mtproto

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
)

func newRotateTestClient() *Conn {
	return newTestClient(func(msgID int64, seqNo int32, body bin.Encoder) (bin.Encoder, error) {
		return &tg.Config{}, nil
	})
}

func setSession(c *Conn, id int64, seq int32) {
	c.sessionMux.Lock()
	c.sessionID = id
	c.sessionMux.Unlock()
	c.reqMux.Lock()
	c.sentContentMessages = seq
	c.reqMux.Unlock()
}

func seqno(c *Conn) int32 {
	c.reqMux.Lock()
	defer c.reqMux.Unlock()
	return c.sentContentMessages
}

// TestConn_recreateSession verifies the atomic session rotation primitive:
// session_id changes, seqno resets to 0 and replay protection is reset.
func TestConn_recreateSession(t *testing.T) {
	a := require.New(t)

	c := newRotateTestClient()
	setSession(c, 123456, 42)

	a.Equal(int64(123456), c.session().ID)

	a.NoError(c.recreateSession())

	a.NotEqual(int64(123456), c.session().ID, "session_id must rotate")
	a.NotZero(c.session().ID, "session_id must be non-zero")
	a.Zero(seqno(c), "seqno must reset to 0")
}

// TestConn_handleBadMsg_RotateTriggers verifies that the rotate-on-trigger
// bad_msg codes recreate the session (new session_id, seqno reset), while the
// salt-only code 48 leaves the session_id and seqno untouched. Codes 16/17
// (msg_id too low/high) are intentionally absent: they are clock-error codes
// corrected by re-learning the server-time offset (covered by the
// TestConn_handleBadMsg_Code1{6,7}* tests), and only rotate the session on a
// downward correction.
func TestConn_handleBadMsg_RotateTriggers(t *testing.T) {
	rotateCodes := []struct {
		name string
		code int
	}{
		{"Code19_ContainerMsgIDDup", 19},
		{"Code32_SeqNoTooLow", 32},
		{"Code33_SeqNoTooHigh", 33},
		{"Code64_InvalidContainer", 64},
	}

	for _, tt := range rotateCodes {
		t.Run(tt.name, func(t *testing.T) {
			a := require.New(t)

			c := newRotateTestClient()
			setSession(c, 777, 17)

			a.NoError(c.handleBadMsg(555, encodeBadMsgNotification(t, 555, tt.code)))

			a.NotEqual(int64(777), c.session().ID, "session_id must rotate on code %d", tt.code)
			a.NotZero(c.session().ID)
			a.Zero(seqno(c), "seqno must reset on code %d", tt.code)
		})
	}

	t.Run("Code48_SaltOnly_NoRotate", func(t *testing.T) {
		a := require.New(t)

		c := newRotateTestClient()
		setSession(c, 999, 9)

		a.NoError(c.handleBadMsg(555, encodeBadServerSalt(t, 555, codeIncorrectServerSalt, 0xDEAD)))

		a.Equal(int64(999), c.session().ID, "session_id must NOT rotate on bad_server_salt (code 48)")
		a.Equal(int32(9), seqno(c), "seqno must NOT reset on code 48")
	})
}

// setSessionAtomic sets session_id and seqno together under both mutexes held in
// the canonical (reqMux -> sessionMux) order, so the test's own setup never
// introduces a torn state that could mask the bug under test.
func setSessionAtomic(c *Conn, id int64, seq int32) {
	c.reqMux.Lock()
	c.sessionMux.Lock()
	c.sentContentMessages = seq
	c.sessionID = id
	c.sessionMux.Unlock()
	c.reqMux.Unlock()
}

// TestConn_recreateSession_AtomicUnderConcurrency stresses the rotation against
// concurrent session() snapshots. recreateSession resets seqno, swaps the
// session_id and resets the replay buffer; its doc comment promises these change
// together. A session() snapshot reads both the seqno and the session_id, so it
// must never observe a torn state: the fresh seqno (0) under the still-old
// session_id, or a stale non-zero seqno under the freshly rotated session_id.
//
// Run with -race to also catch the data race between the two lock acquisitions.
func TestConn_recreateSession_AtomicUnderConcurrency(t *testing.T) {
	const (
		origID  = int64(0x0BADF00D)
		origSeq = int32(7)
	)

	c := newRotateTestClient()
	// The default test client uses a non-thread-safe math/rand source; production
	// uses crypto/rand (thread-safe). Swap to a thread-safe source so this test
	// exercises the session-field race, not a rand-source race.
	c.rand = crypto.DefaultRand()

	var wg sync.WaitGroup

	// Restorer: repeatedly re-establish the known consistent (origID, origSeq)
	// state, racing against the rotators.
	wg.Go(func() {
		for range 20000 {
			setSessionAtomic(c, origID, origSeq)
		}
	})

	// Rotators: keep minting fresh sessions (seqno reset to 0, fresh id).
	for range 3 {
		wg.Go(func() {
			for range 8000 {
				require.NoError(t, c.recreateSession())
			}
		})
	}

	// Observers: snapshot the session and assert internal consistency. A valid
	// snapshot is either fully original (origID, origSeq) or fully rotated
	// (some non-origID, seqno 0) — never a torn mix.
	for range 4 {
		wg.Go(func() {
			for range 20000 {
				s := c.session()
				if s.ID == origID && s.SeqNo != origSeq {
					t.Errorf("torn snapshot: original session_id %#x with seqno %d (want %d)",
						s.ID, s.SeqNo, origSeq)
					return
				}
				if s.ID != origID && s.SeqNo != 0 {
					t.Errorf("torn snapshot: rotated session_id %#x carries stale seqno %d (want 0)",
						s.ID, s.SeqNo)
					return
				}
			}
		})
	}

	wg.Wait()
}

func encodeBadMsgNotification(t *testing.T, badMsgID int64, code int) *bin.Buffer {
	t.Helper()
	b := &bin.Buffer{}
	require.NoError(t, b.Encode(&mt.BadMsgNotification{
		BadMsgID:    badMsgID,
		BadMsgSeqno: 0,
		ErrorCode:   code,
	}))
	return b
}

func encodeBadServerSalt(t *testing.T, badMsgID int64, code int, newSalt int64) *bin.Buffer {
	t.Helper()
	b := &bin.Buffer{}
	require.NoError(t, b.Encode(&mt.BadServerSalt{
		BadMsgID:      badMsgID,
		BadMsgSeqno:   0,
		ErrorCode:     code,
		NewServerSalt: newSalt,
	}))
	return b
}
