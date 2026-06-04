package mtproto

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
)

// encryptServerMessage builds a server->client encrypted MTProto frame the way
// tgtest/read_test do, so consumeMessage exercises the REAL decrypt path
// (session check, msg_id bounds, replay buffer).
func encryptServerMessage(t *testing.T, key crypto.AuthKey, sessionID, msgID int64) []byte {
	t.Helper()

	var msg bin.Buffer
	require.NoError(t, msg.Encode(&testPayload{Data: []byte("retransmit")}))
	length := msg.Len()
	data := msg.Copy()

	var out bin.Buffer
	serverCipher := crypto.NewServerCipher(rand.Reader)
	require.NoError(t, serverCipher.Encrypt(key, crypto.EncryptedMessageData{
		SessionID:              sessionID,
		MessageID:              msgID,
		SeqNo:                  0,
		MessageDataLen:         int32(length),
		MessageDataWithPadding: data,
	}, &out))
	return out.Raw()
}

func newStaleIDTestConn(t *testing.T) *Conn {
	t.Helper()
	c := newTestClient(func(msgID int64, seqNo int32, body bin.Encoder) (bin.Encoder, error) {
		return &tg.Config{}, nil
	})
	setSession(c, 123456, 0)
	return c
}

// TestConsumeMessage_DuplicateMsgID_NoRotate is the regression test for the
// lost-updates bug: the server retransmits an unacked message with the SAME
// msg_id (e.g. the ack was lost in a network blip). Per the MTProto security
// guidelines such a message "is to be ignored"; the official Android client
// (tgnet ConnectionsManager.cpp:999-1003) ignores it without touching the
// session. Rotating the session here makes the server silently drop every
// push update still addressed to the old session_id.
func TestConsumeMessage_DuplicateMsgID_NoRotate(t *testing.T) {
	a := require.New(t)
	c := newStaleIDTestConn(t)
	key := c.session().Key

	msgID := int64(proto.NewMessageID(time.Now(), proto.MessageFromServer))
	raw := encryptServerMessage(t, key, 123456, msgID)

	// First delivery: accepted and processed.
	buf1 := &bin.Buffer{Buf: append([]byte(nil), raw...)}
	a.NoError(c.consumeMessage(context.Background(), buf1))
	a.Equal(int64(123456), c.session().ID)

	// Server retransmit with the SAME msg_id: must be ignored, NOT rotate.
	buf2 := &bin.Buffer{Buf: append([]byte(nil), raw...)}
	a.NoError(c.consumeMessage(context.Background(), buf2))
	a.Equal(int64(123456), c.session().ID,
		"duplicate inbound msg_id must be ignored per MTProto spec, not rotate the session")
}

// TestConsumeMessage_TooOldMsgID_Rotates pins the behavior we KEEP: a msg_id
// created >300s in the past is a genuine clock/window desync signal and still
// rotates the session (read.go checkMessageID "created too far in past").
func TestConsumeMessage_TooOldMsgID_Rotates(t *testing.T) {
	a := require.New(t)
	c := newStaleIDTestConn(t)
	key := c.session().Key

	msgID := int64(proto.NewMessageID(time.Now().Add(-10*time.Minute), proto.MessageFromServer))
	raw := encryptServerMessage(t, key, 123456, msgID)

	a.NoError(c.consumeMessage(context.Background(), &bin.Buffer{Buf: append([]byte(nil), raw...)}))
	a.NotEqual(int64(123456), c.session().ID,
		"msg_id created too far in past must still rotate the session")
}
