package telegram

import (
	"context"
	"fmt"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
)

func (c *Client) restoreConnection(ctx context.Context) error {
	if c.storage == nil {
		return nil
	}

	data, err := c.storage.Load(ctx)
	if errors.Is(err, session.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "load")
	}

	// If file does not contain DC ID, so we use DC from options.
	prev := c.session.Load()
	if data.DC == 0 {
		data.DC = prev.DC
	}

	// Restoring persisted auth key.
	var key crypto.AuthKey
	copy(key.Value[:], data.AuthKey)
	copy(key.ID[:], data.AuthKeyID)

	if key.Value.ID() != key.ID {
		return errors.New("corrupted key")
	}

	// Seed per-DC initConnection versions so a restarted process can skip
	// re-sending initConnection to DCs it already initialized (Android
	// lastInitVersion persistence).
	c.initVersions.Restore(data.InitVersions)

	// Seed the persistent server-time offset so the first msg_id after restart
	// is already aligned with the server clock (Android timeDifference).
	c.timeOffset.Store(data.TimeOffset)

	// Re-initializing connection from persisted state.
	c.log.Info("Connection restored from state",
		zap.String("addr", data.Addr),
		zap.String("key_id", fmt.Sprintf("%x", data.AuthKeyID)),
	)

	c.connMux.Lock()
	c.session.Store(pool.Session{
		DC:      data.DC,
		AuthKey: key,
		Salt:    data.Salt,
	})
	c.conn = c.createPrimaryConn(nil)
	c.connMux.Unlock()

	return nil
}

func (c *Client) saveSession(cfg tg.Config, s mtproto.Session) error {
	if c.storage == nil {
		return nil
	}

	data, err := c.storage.Load(c.ctx)
	if errors.Is(err, session.ErrNotFound) {
		// Initializing new state.
		err = nil
		data = &session.Data{}
	}
	if err != nil {
		return errors.Wrap(err, "load")
	}

	// Updating previous data.
	data.Config = session.ConfigFromTG(cfg)
	keyToSave := s.Key
	if !s.PermKey.Zero() {
		// Persist permanent key in PFS mode: temporary key is expected to rotate.
		keyToSave = s.PermKey
	}
	data.AuthKey = keyToSave.Value[:]
	data.AuthKeyID = keyToSave.ID[:]
	data.DC = cfg.ThisDC
	data.Salt = s.Salt
	// Persist per-DC initConnection versions (Android lastInitVersion).
	if snap := c.initVersions.Snapshot(); snap != nil {
		data.InitVersions = snap
	}
	// Persist server-time offset (Android timeDifference) so a restarted process
	// generates its first msg_id aligned with the server clock.
	data.TimeOffset = c.timeOffset.Load()

	if err := c.storage.Save(c.ctx, data); err != nil {
		return errors.Wrap(err, "save")
	}

	c.log.Debug("Data saved",
		zap.String("key_id", fmt.Sprintf("%x", data.AuthKeyID)),
	)
	return nil
}

func (c *Client) onSession(cfg tg.Config, s mtproto.Session) error {
	sessionData := dcSessionFromMTProto(cfg.ThisDC, s)
	// Track per-DC session in memory for pool reconnections/migrations.
	c.storeDCSess(c.sessions, sessionData)

	primaryDC := c.session.Load().DC
	// Do not save session for non-primary DC.
	if cfg.ThisDC != 0 && primaryDC != 0 && primaryDC != cfg.ThisDC {
		return nil
	}

	c.connMux.Lock()
	// StoreCarry keeps the seqno monotonic for a continued session_id so the
	// live-seqno carry and new_session_created event cannot regress it.
	c.session.StoreCarry(sessionData)
	c.cfg.Store(cfg)
	c.onReady()
	c.connMux.Unlock()

	// Store-and-resend: the new conn is installed and ready, so re-issue any
	// in-flight requests parked by a transport drop on the previous conn. Done
	// after connMux.Unlock so replay RPCs do not run under connMux.
	c.replayLiveRequests()

	if err := c.saveSession(cfg, s); err != nil {
		return errors.Wrap(err, "save")
	}

	return nil
}

func (c *Client) onCDNSession(cfg tg.Config, s mtproto.Session) error {
	// CDN sessions are isolated from regular DC map because lifecycle and reset
	// triggers differ (fingerprint misses, CDN-specific reconnects).
	c.storeDCSess(c.cdnSessions, dcSessionFromMTProto(cfg.ThisDC, s))
	return nil
}

func (c *Client) storeDCSess(target map[int]*pool.SyncSession, data pool.Session) {
	c.sessionsMux.Lock()
	if existing, ok := target[data.DC]; ok {
		// Keep seqno monotonic for a continued session_id (see StoreCarry).
		existing.StoreCarry(data)
		c.sessionsMux.Unlock()
		return
	}
	target[data.DC] = pool.NewSyncSession(data)
	c.sessionsMux.Unlock()
}

func dcSessionFromMTProto(dc int, s mtproto.Session) pool.Session {
	keyToStore := s.Key
	if !s.PermKey.Zero() {
		// Keep in-memory/persisted key format backward-compatible: one key slot.
		// In PFS mode temp key rotates, so we pin permanent key.
		keyToStore = s.PermKey
	}

	return pool.Session{
		DC:      dc,
		Salt:    s.Salt,
		AuthKey: keyToStore,
		// Carry MTProto session_id and seqno in memory so the next reconnect to
		// this DC continues the session instead of recreating it. Not persisted
		// to disk: like the official client, these live only in memory.
		SessionID: s.ID,
		SeqNo:     s.SeqNo,
	}
}
