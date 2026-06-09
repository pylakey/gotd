package mtproto

import (
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
)

func (c *Conn) handleSessionCreated(b *bin.Buffer) error {
	var s mt.NewSessionCreated
	if err := s.Decode(b); err != nil {
		return errors.Wrap(err, "decode")
	}
	c.gotSession.Signal()

	created := proto.MessageID(s.FirstMsgID).Time()
	now := c.serverNow()
	c.log.Debug("Session created",
		zap.Int64("unique_id", s.UniqueID),
		zap.Int64("first_msg_id", s.FirstMsgID),
		zap.Time("first_msg_time", created.Local()),
	)

	// new_session_created carries the server's first msg_id, whose time is the
	// server clock. Learn the server-time offset from it (clamped by the store)
	// instead of only warning, so subsequent msg_id generation and inbound
	// bounds track the server even under local clock skew. This mirrors the
	// official client deriving timeDifference from server message time.
	if (created.Before(now) && now.Sub(created) > maxPast) || created.Sub(now) > maxFuture {
		offset := c.learnTimeOffset(created)
		c.log.Warn("Local clock needs synchronization",
			zap.Time("first_msg_time", created),
			zap.Time("local", now),
			zap.Duration("time_difference", now.Sub(created)),
			zap.Int("server_time_offset_seconds", offset),
		)
	}

	c.storeSalt(s.ServerSalt)
	if err := c.handler.OnSession(c.session()); err != nil {
		return errors.Wrap(err, "handler.OnSession")
	}
	return nil
}
