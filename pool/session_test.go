package pool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
)

func TestSyncSessionOptions(t *testing.T) {
	a := require.New(t)

	session := NewSyncSession(Session{
		DC:      2,
		AuthKey: crypto.Key{1}.WithID(),
		Salt:    42,
	})
	opts, data := session.Options(mtproto.Options{})

	a.Equal(2, data.DC)
	a.Equal(int64(42), opts.Salt)
	a.Equal(data.AuthKey, opts.Key)
	a.True(opts.PermKey.Zero())
}

func TestSyncSessionOptionsPFS(t *testing.T) {
	a := require.New(t)

	session := NewSyncSession(Session{
		DC:      2,
		AuthKey: crypto.Key{1}.WithID(),
		Salt:    42,
	})
	opts, data := session.Options(mtproto.Options{
		EnablePFS: true,
	})

	// PFS mode must move persisted key into PermKey and force runtime Key=zero.
	a.Equal(2, data.DC)
	a.Equal(int64(42), opts.Salt)
	a.True(opts.Key.Zero())
	a.Equal(data.AuthKey, opts.PermKey)
}

func TestSyncSessionOptionsSessionContinuity(t *testing.T) {
	a := require.New(t)

	session := NewSyncSession(Session{
		DC:        2,
		AuthKey:   crypto.Key{1}.WithID(),
		Salt:      42,
		SessionID: 555,
		SeqNo:     12,
	})

	// Non-PFS: session_id and seqno are carried into the mtproto options so a
	// plain reconnect continues the session.
	opts, _ := session.Options(mtproto.Options{})
	a.Equal(int64(555), opts.SessionID)
	a.Equal(int32(12), opts.SeqNo)

	// PFS: continuity does not apply (temp key bind re-handshakes), so they
	// must stay zero regardless of stored values.
	pfsOpts, _ := session.Options(mtproto.Options{EnablePFS: true})
	a.Zero(pfsOpts.SessionID)
	a.Zero(pfsOpts.SeqNo)
}

func TestSyncSessionStoreCarry(t *testing.T) {
	t.Run("MonotonicSameSession", func(t *testing.T) {
		a := require.New(t)

		s := NewSyncSession(Session{DC: 2, SessionID: 555, SeqNo: 12})

		// A lower seqno for the same session_id must not regress the stored value:
		// within a session the content seqno only moves forward.
		s.StoreCarry(Session{DC: 2, SessionID: 555, SeqNo: 5})
		a.Equal(int32(12), s.Load().SeqNo, "seqno must not regress for the same session_id")

		// A higher seqno for the same session_id advances it.
		s.StoreCarry(Session{DC: 2, SessionID: 555, SeqNo: 20})
		a.Equal(int32(20), s.Load().SeqNo, "seqno must advance for the same session_id")
	})

	t.Run("ChangedSessionTakesNewSeqNo", func(t *testing.T) {
		a := require.New(t)

		s := NewSyncSession(Session{DC: 2, SessionID: 555, SeqNo: 30})

		// A rotated session_id starts a fresh sequence: the (lower) new seqno is
		// taken verbatim, the old high-water mark does not leak across sessions.
		s.StoreCarry(Session{DC: 2, SessionID: 777, SeqNo: 0})
		a.Equal(int64(777), s.Load().SessionID)
		a.Zero(s.Load().SeqNo, "rotated session must reset seqno")
	})

	t.Run("ZeroSessionVerbatim", func(t *testing.T) {
		a := require.New(t)

		s := NewSyncSession(Session{DC: 2, SessionID: 555, SeqNo: 30})

		// A zero session_id means "mint a new one": stored verbatim, no carry.
		s.StoreCarry(Session{DC: 2, SessionID: 0, SeqNo: 0})
		a.Zero(s.Load().SessionID)
		a.Zero(s.Load().SeqNo)
	})
}

func TestSyncSessionMigrateZeroesSession(t *testing.T) {
	a := require.New(t)

	session := NewSyncSession(Session{
		DC:        2,
		AuthKey:   crypto.Key{1}.WithID(),
		Salt:      42,
		SessionID: 555,
		SeqNo:     12,
	})

	// Migration is a recreate trigger: the new DC has no knowledge of the
	// previous session, so session_id/seqno (and key/salt) must be zeroed.
	session.Migrate(4)

	data := session.Load()
	a.Equal(4, data.DC)
	a.Zero(data.SessionID)
	a.Zero(data.SeqNo)
	a.True(data.AuthKey.Zero())
	a.Zero(data.Salt)
}
