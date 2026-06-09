package mtproto

import "github.com/gotd/td/proto"

func (c *Conn) newMessageID() int64 {
	return c.messageID.New(proto.MessageFromClient)
}

// messageIDResetter is the optional capability of a MessageIDSource to drop its
// monotonic high-water mark so the next generated id re-anchors to the current
// clock. *proto.MessageIDGen implements it.
type messageIDResetter interface {
	Reset()
}

// resetMessageID re-anchors the msg_id generator to the current (server) clock
// if the source supports it. Called as part of session rotation: a fresh
// session must not inherit the previous session's msg_id high-water mark, and a
// downward server-time correction (lower offset => lower clock) would otherwise
// keep emitting ids above the stale high-water mark and be rejected again.
//
// The caller must hold the locks that guard rotation so the re-anchor is
// observed together with the session_id/seqno reset.
func (c *Conn) resetMessageID() {
	if r, ok := c.messageID.(messageIDResetter); ok {
		r.Reset()
	}
}
