package mtproto

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/gotd/td/bin"
)

func (c *Conn) writeContentMessage(ctx context.Context, msgID int64, seqNo int32, message bin.Encoder) error {
	return c.write(ctx, msgID, seqNo, message)
}

func (c *Conn) writeServiceMessage(ctx context.Context, message bin.Encoder) error {
	msgID, seqNo := c.nextMsgSeq(false)
	return c.write(ctx, msgID, seqNo, message)
}

var bufPool = bin.NewPool(0)

func (c *Conn) write(ctx context.Context, msgID int64, seqNo int32, message bin.Encoder) error {
	// Grab shared lock for writing.
	// It prevents message sending during key regeneration if server forgot current auth key.
	c.exchangeLock.RLock()
	defer c.exchangeLock.RUnlock()

	b := bufPool.Get()
	defer bufPool.Put(b)

	if err := c.newEncryptedMessage(msgID, seqNo, message, b); err != nil {
		return err
	}

	if err := c.conn.Send(ctx, b); err != nil {
		// A transport write failure means the socket is dead. The official Telegram
		// client reconnects on ANY socket error — a failed write tears the socket
		// down exactly like a failed read — instead of failing the request against a
		// dead connection. Mirror that: promote a real write error to a connection
		// teardown so conn.Run returns and the reconnect loop redials in place,
		// rather than letting the dead socket limp until the much slower pong-miss /
		// read watchdog while subsequent writes pile up broken-pipe errors. All
		// writes funnel through here (rpc engine, ping, ack, salt), so this is the
		// single point that turns any write-side death into a reconnect.
		//
		// A ctx-cancelled write is the caller's own cancellation (or our deadline
		// push during an in-progress teardown), NOT a transport death — never tear
		// the shared connection down for that.
		if ctx.Err() == nil {
			c.abortDeadSocket()
			// Surface a recognisable transport-death sentinel (not the bare net
			// error) so the store-and-resend layer above the connection can replay
			// THIS request — the one whose write hit the dead socket — on the
			// reconnected conn, instead of failing it with a broken-pipe error.
			return errors.Wrap(ErrConnDead, err.Error())
		}
		return err
	}

	return nil
}

func (c *Conn) nextMsgSeq(content bool) (msgID int64, seqNo int32) {
	c.reqMux.Lock()
	defer c.reqMux.Unlock()

	msgID = c.newMessageID()

	// Computing current sequence number (seqno).
	// This should be serialized with new message id generation.
	//
	// See https://github.com/gotd/td/issues/245 for reference.
	seqNo = c.sentContentMessages * 2
	if content {
		seqNo++
		c.sentContentMessages++
	}

	return
}
