package pool

import (
	"sync"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
)

// Session represents DC session.
type Session struct {
	DC      int
	AuthKey crypto.AuthKey
	Salt    int64
	// SessionID is the MTProto session_id. It is carried across plain
	// reconnects to the same DC so the session (and its seqno window) is
	// continued instead of recreated. Zero means "mint a new one".
	SessionID int64
	// SeqNo is the count of content messages sent within SessionID
	// (sentContentMessages). Carried alongside SessionID to continue the
	// sequence after a reconnect. Meaningless without a non-zero SessionID.
	SeqNo int32
}

// SyncSession is synchronization helper for Session.
type SyncSession struct {
	data Session
	mux  sync.RWMutex
}

// NewSyncSession creates new SyncSession.
func NewSyncSession(data Session) *SyncSession {
	return &SyncSession{
		data: data,
	}
}

// Store saves given Session.
func (s *SyncSession) Store(data Session) {
	s.mux.Lock()
	s.data = data
	s.mux.Unlock()
}

// StoreCarry saves the given session while preserving seqno monotonicity within
// a session: when data continues the currently stored session_id, the higher of
// the two SeqNo values is kept. Within one MTProto session the content seqno
// only ever moves forward, so a snapshot must never lower it. This makes the
// live-seqno carry robust against the ordering race between the dying
// connection's final snapshot and the reconnected connection's
// new_session_created event (which may arrive in either order). A changed (or
// zero) session_id is a fresh session and is stored verbatim.
func (s *SyncSession) StoreCarry(data Session) {
	s.mux.Lock()
	if data.SessionID != 0 && data.SessionID == s.data.SessionID && s.data.SeqNo > data.SeqNo {
		data.SeqNo = s.data.SeqNo
	}
	s.data = data
	s.mux.Unlock()
}

// Migrate changes current DC and its addr, zeroes AuthKey and Salt.
//
// Migration is a session-recreate trigger: the new DC has no knowledge of the
// previous session_id, so SessionID and SeqNo are zeroed to force minting a
// fresh session on the next connect.
func (s *SyncSession) Migrate(dc int) {
	s.mux.Lock()
	s.data.DC = dc
	s.data.AuthKey = crypto.AuthKey{}
	s.data.Salt = 0
	s.data.SessionID = 0
	s.data.SeqNo = 0
	s.mux.Unlock()
}

// Options fills Key and Salt field of given Options using stored session and returns it.
func (s *SyncSession) Options(opts mtproto.Options) (mtproto.Options, Session) {
	s.mux.RLock()
	data := s.data
	s.mux.RUnlock()

	if opts.EnablePFS {
		// Stored key in pool/session remains backward-compatible single "AuthKey".
		// In PFS mode this persisted key is treated as permanent key, while
		// temporary key is always generated per runtime connection.
		opts.PermKey = data.AuthKey
		opts.Key = crypto.AuthKey{}
		// In PFS the per-connection temporary key bind is a re-handshake that
		// mints a fresh session_id, so session_id/seqno continuity does not
		// apply: leave them zero and let the handshake assign a new session.
	} else {
		opts.Key = data.AuthKey
		// Carry session_id/seqno so a plain reconnect to the same DC continues
		// the existing MTProto session instead of recreating it.
		opts.SessionID = data.SessionID
		opts.SeqNo = data.SeqNo
	}
	opts.Salt = data.Salt
	return opts, data
}

// Load gets session and returns it.
func (s *SyncSession) Load() (data Session) {
	s.mux.RLock()
	data = s.data
	s.mux.RUnlock()

	return
}
