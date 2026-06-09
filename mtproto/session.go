package mtproto

import (
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
)

// Session represents connection state.
type Session struct {
	ID   int64
	Key  crypto.AuthKey
	Salt int64
	// SeqNo is the count of content messages sent within this session
	// (sentContentMessages). It is carried out so a reconnect to the same DC
	// can continue the sequence instead of resetting it.
	SeqNo int32
	// PermKey is non-zero only in PFS mode and is used for bindTempAuthKey.
	PermKey crypto.AuthKey
}

// Session returns current connection session info.
//
// The seqno (sentContentMessages) and the session_id must be read as a
// consistent pair: recreateSession swaps them together, so reading them under
// separate lock acquisitions could observe a torn (new-seqno, old-session_id)
// state. We therefore hold both mutexes for the snapshot, taking them in the
// canonical reqMux -> sessionMux order shared with recreateSession to avoid
// lock-order inversion. updateSalt() is called first because it acquires
// sessionMux itself.
func (c *Conn) session() Session {
	c.updateSalt()

	c.reqMux.Lock()
	c.sessionMux.RLock()
	defer func() {
		c.sessionMux.RUnlock()
		c.reqMux.Unlock()
	}()
	return Session{
		Key:     c.authKey,
		Salt:    c.salt,
		ID:      c.sessionID,
		SeqNo:   c.sentContentMessages,
		PermKey: c.permKey,
	}
}

// Session returns a snapshot of the current connection session, including the
// live content-message seqno (sentContentMessages). It is safe to call after
// Run returns and is used to carry the live seqno into the next reconnect so
// the session sequence continues instead of resuming from a stale snapshot.
func (c *Conn) Session() Session {
	return c.session()
}

// newSessionID sets session id to random value.
func (c *Conn) newSessionID() error {
	id, err := crypto.RandInt64(c.rand)
	if err != nil {
		return err
	}

	c.sessionMux.Lock()
	defer c.sessionMux.Unlock()
	c.sessionID = id

	return nil
}

// recreateSession atomically rotates the MTProto session: it mints a new
// session_id, resets the content message sequence (seqno) to zero, resets the
// inbound replay-protection buffer and re-anchors the msg_id generator to the
// current server clock. These must always change together, otherwise a partial
// reset can make a desync permanent (e.g. continuing seqno under a fresh
// session_id, accepting stale inbound ids under a new session, or — after a
// downward server-time correction — re-emitting msg_ids above the stale
// high-water mark that the server already rejected).
//
// The rotation is performed under both reqMux and sessionMux held together, in
// the canonical reqMux -> sessionMux order shared with session(), so no
// concurrent send (nextMsgSeq + session()) or read path can observe a torn
// (new-seqno, old-session_id) state.
//
// This is invoked only on the recreate triggers: bad_msg_notification codes
// 19/32/33/64, a downward server-time correction on codes 16/17 (a lower
// offset would otherwise emit a lower msg_id mid-session), and receipt of an
// already-processed / too-old inbound msg_id. A plain TCP reconnect must never
// call it.
func (c *Conn) recreateSession() error {
	id, err := crypto.RandInt64(c.rand)
	if err != nil {
		return err
	}

	c.reqMux.Lock()
	c.sessionMux.Lock()
	c.sentContentMessages = 0
	c.sessionID = id
	c.messageIDBuf = proto.NewMessageIDBuf(100)
	// Re-anchor the msg_id generator so the fresh session does not inherit the
	// previous high-water mark. This is what makes a downward server-time
	// correction effective: the next msg_id tracks the corrected (lower) clock
	// instead of staying pinned above the old, too-high mark.
	c.resetMessageID()
	c.sessionMux.Unlock()
	c.reqMux.Unlock()

	return nil
}
