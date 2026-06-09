package manager

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type protoConn interface {
	Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error
	Run(ctx context.Context, f func(ctx context.Context) error) error
	Ping(ctx context.Context) error
	Session() mtproto.Session
}

//go:generate go run -modfile=../../../_tools/go.mod golang.org/x/tools/cmd/stringer -type=ConnMode

// ConnMode represents connection mode.
type ConnMode byte

const (
	// ConnModeUpdates is update connection mode.
	ConnModeUpdates ConnMode = iota
	// ConnModeData is data connection mode.
	ConnModeData
	// ConnModeCDN is CDN connection mode.
	ConnModeCDN
)

// Conn is a Telegram client connection.
type Conn struct {
	// Connection parameters.
	mode ConnMode // immutable
	dc   int      // immutable
	// MTProto connection.
	proto protoConn // immutable

	// InitConnection parameters.
	appID  int          // immutable
	device DeviceConfig // immutable

	// setup is callback which called after initConnection, but before ready signaling.
	// This is necessary to transfer auth from previous connection to another DC.
	setup SetupCallback // nilable

	// onDead is called on connection death.
	onDead func(error)

	// initCache, if non-nil, holds per-DC negotiated init versions so reconnects
	// to an already-initialized DC skip re-sending the full initConnection.
	initCache *InitVersionCache // nilable

	// Wrappers for external world, like logs or PRNG.
	// Should be immutable.
	clock clock.Clock // immutable
	log   *zap.Logger // immutable

	// Handler passed by client.
	handler Handler // immutable

	// State fields.
	cfg tg.Config
	// cdnNeedsInit mirrors TDesktop connectionInited state for CDN transport.
	// true means requests must go via invokeWithLayer(initConnection).
	cdnNeedsInit atomic.Bool
	// pending buffers OnSession events until initConnection config is available.
	pending []mtproto.Session
	ongoing int
	latest  time.Time
	mux     sync.Mutex

	sessionInit *tdsync.Ready // immutable
	gotConfig   *tdsync.Ready // immutable
	dead        *tdsync.Ready // immutable

	connBackoff func(ctx context.Context) backoff.BackOff // immutable
}

// OnSession implements mtproto.Handler.
func (c *Conn) OnSession(session mtproto.Session) error {
	c.log.Info("SessionInit")
	c.sessionInit.Signal()

	// Quote (PFS): "Once auth.bindTempAuthKey has been executed successfully,
	// the client can continue generating API calls as usual."
	// Link: https://core.telegram.org/api/pfs
	//
	// In PFS mode bind is performed before initConnection, so OnSession can happen
	// before config is ready. We must not block read-loop handler here.
	c.mux.Lock()
	c.pending = append(c.pending, session)
	c.mux.Unlock()

	if !c.configReady() {
		return nil
	}
	return c.flushPendingSession()
}

func (c *Conn) configReady() bool {
	select {
	case <-c.gotConfig.Ready():
		return true
	default:
		return false
	}
}

func (c *Conn) flushPendingSession() error {
	c.mux.Lock()
	pending := append([]mtproto.Session(nil), c.pending...)
	cfg := c.cfg
	c.pending = c.pending[:0]
	c.mux.Unlock()
	if len(pending) == 0 {
		return nil
	}
	for _, s := range pending {
		// Preserve event ordering in case multiple session events arrive before
		// config is ready.
		if err := c.handler.OnSession(cfg, s); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) trackInvoke() func() {
	start := c.clock.Now()

	c.mux.Lock()
	defer c.mux.Unlock()

	c.ongoing++
	c.latest = start

	return func() {
		c.mux.Lock()
		defer c.mux.Unlock()

		c.ongoing--
		end := c.clock.Now()
		c.latest = end

		c.log.Debug("Invoke",
			zap.Duration("duration", end.Sub(start)),
			zap.Int("ongoing", c.ongoing),
		)
	}
}

// Run initialize connection.
func (c *Conn) Run(ctx context.Context) (err error) {
	defer c.dead.Signal()
	defer func() {
		if err != nil && ctx.Err() == nil {
			c.log.Debug("Connection dead", zap.Error(err))
			if c.onDead != nil {
				c.onDead(err)
			}
		}
	}()
	// Carry the live session (including the current content seqno) out of the
	// dying connection so the next reconnect to this DC continues the sequence
	// instead of resuming from the seqno snapshotted once at new_session_created
	// time. Without this the resumed seqno is stale-low and the server rejects
	// it (bad_msg code 32), degrading every reconnect to a fresh session.
	defer c.carryLiveSession()
	return c.proto.Run(ctx, func(ctx context.Context) error {
		// Signal death on init error to unblock waiters in waitSession/OnSession.
		err := c.init(ctx)
		if err != nil {
			c.dead.Signal()
		}
		return err
	})
}

// carryLiveSession snapshots the live MTProto session of the just-terminated
// connection and replays it through the session handler so the stored per-DC
// session refreshes its seqno (and salt) from the live value. It only runs once
// config is known (otherwise there is no DC to attribute the session to and the
// snapshot is the initial empty one).
func (c *Conn) carryLiveSession() {
	if !c.configReady() {
		return
	}
	s := c.proto.Session()
	if s.ID == 0 {
		// No established session to carry.
		return
	}
	c.mux.Lock()
	cfg := c.cfg
	c.mux.Unlock()
	if err := c.handler.OnSession(cfg, s); err != nil {
		c.log.Debug("Failed to carry live session on shutdown", zap.Error(err))
	}
}

func (c *Conn) waitSession(ctx context.Context) error {
	select {
	// Connection is considered ready only after mode-specific init succeeded.
	case <-c.gotConfig.Ready():
		return nil
	case <-c.dead.Ready():
		return pool.ErrConnDead
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ready returns channel to determine connection readiness.
// Useful for pooling.
func (c *Conn) Ready() <-chan struct{} {
	// Pool should expose readiness only when Invoke can send API calls.
	return c.gotConfig.Ready()
}

// Invoke implements Invoker.
func (c *Conn) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	// Tracking ongoing invokes.
	defer c.trackInvoke()()
	if err := c.waitSession(ctx); err != nil {
		return errors.Wrap(err, "waitSession")
	}

	if c.mode == ConnModeCDN {
		// CDN mode has dedicated request wrapping rules (see invokeCDN).
		err := c.invokeCDN(ctx, input, output)
		return err
	}
	// Match the official Telegram Android client: invokeWithLayer is the carrier
	// of initConnection and the two are sent as one nested unit only while init
	// is needed for this DC's auth-key. init() already negotiated the layer for
	// this connection's (persisted, per-auth-key) init state, so once inited the
	// request goes on the wire as the BARE TL method — no invokeWithLayer, no
	// initConnection. wrapRequest still applies the orthogonal invokeWithoutUpdates
	// wrapper in ConnModeData; that is independent of the layer wrapper.
	req := c.wrapRequest(noopDecoder{input})
	err := c.proto.Invoke(ctx, req, output)
	return err
}
func (c *Conn) invokeCDN(
	ctx context.Context,
	input bin.Encoder,
	output bin.Decoder,
) error {
	// TDesktop model:
	// - while connection is "not inited": wrap every query in invokeWithLayer(initConnection);
	// - after first successful reply: use raw CDN methods;
	// - if server returns CONNECTION_NOT_INITED/LAYER_INVALID on raw call:
	//   mark "not inited" and retry wrapped once.
	if c.cdnNeedsInit.Load() {
		err := c.invokeCDNWrapped(ctx, input, output)
		if err == nil {
			c.cdnNeedsInit.Store(false)
			return nil
		}
		return err
	}

	err := c.invokeCDNRaw(ctx, input, output)
	if err == nil {
		return nil
	}
	if c.shouldCDNRetryWrapped(err) {
		c.cdnNeedsInit.Store(true)
		retryErr := c.invokeCDNWrapped(ctx, input, output)
		if retryErr == nil {
			c.cdnNeedsInit.Store(false)
			return nil
		}
		return retryErr
	}
	return err
}
func (c *Conn) invokeCDNWrapped(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req := &tg.InvokeWithLayerRequest{
		Layer: tg.Layer,
		Query: c.cdnInitRequest(noopDecoder{input}),
	}
	return c.proto.Invoke(ctx, req, output)
}
func (c *Conn) invokeCDNRaw(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return c.proto.Invoke(ctx, input, output)
}
func (c *Conn) shouldCDNRetryWrapped(err error) bool {
	if err == nil {
		return false
	}
	if rpcErr, ok := tgerr.As(err); ok {
		// Retry wrapped only for not-inited/layer-invalid transport state.
		v := rpcErr.IsOneOf(
			"CONNECTION_NOT_INITED",
			"CONNECTION_LAYER_INVALID",
		)
		return v
	}
	return false
}

// OnMessage implements mtproto.Handler.
func (c *Conn) OnMessage(b *bin.Buffer) error {
	return c.handler.OnMessage(b)
}

type noopDecoder struct {
	bin.Encoder
}

func (n noopDecoder) Decode(b *bin.Buffer) error {
	return errors.New("not implemented")
}

func (c *Conn) wrapRequest(req bin.Object) bin.Object {
	if c.mode == ConnModeData {
		return &tg.InvokeWithoutUpdatesRequest{
			Query: req,
		}
	}

	return req
}

func (c *Conn) cdnInitRequest(query bin.Object) bin.Object {
	// Match TDesktop CDN init wrapper:
	// only device/system are anonymized, the rest of initConnection
	// parameters stay aligned with regular connection settings.
	return &tg.InitConnectionRequest{
		APIID:          c.appID,
		DeviceModel:    "n/a",
		SystemVersion:  "n/a",
		AppVersion:     c.device.AppVersion,
		SystemLangCode: c.device.SystemLangCode,
		LangPack:       c.device.LangPack,
		LangCode:       c.device.LangCode,
		Proxy:          c.device.Proxy,
		Params:         c.device.Params,
		Query:          query,
	}
}
func (c *Conn) init(ctx context.Context) error {
	c.log.Debug("Initializing")

	if c.mode == ConnModeCDN {
		// CDN connections skip help.getConfig init flow and become ready
		// immediately after MTProto auth-key exchange.
		c.cdnNeedsInit.Store(true)
		c.mux.Lock()
		c.latest = c.clock.Now()
		c.cfg = tg.Config{ThisDC: c.dc}
		c.mux.Unlock()
		c.gotConfig.Signal()
		err := c.flushPendingSession()
		return err
	}
	version := c.initVersion()
	// Skip the full initConnection if this DC already accepted the same init
	// version (Android-style cached init version). The connection still refreshes
	// config via a bare help.getConfig so it becomes ready (gotConfig must signal
	// regardless of the skip).
	skipInit := c.initCache.Done(c.dc, version)

	if skipInit {
		c.log.Debug("Skipping initConnection (cached init version)",
			zap.Int("dc_id", c.dc), zap.Int64("init_version", version),
		)
	}

	var cfg tg.Config
	if err := backoff.RetryNotify(func() error {
		if err := c.proto.Invoke(ctx, c.initRequest(skipInit), &cfg); err != nil {
			if tgerr.Is(err, tgerr.ErrFloodWait) {
				// Server sometimes returns FLOOD_WAIT(0) if you create
				// multiple connections in short period of time.
				//
				// See https://github.com/gotd/td/issues/388.
				return errors.Wrap(err, "flood wait")
			}
			// On the skip path the server may reject the bare
			// help.getConfig when its init state (bound to the
			// auth_key) diverged from our cache — e.g. a restored session whose
			// key was rotated server-side. Bust the stale cache entry and retry
			// once with the full initConnection in the same init() call, mirroring
			// the CDN shouldCDNRetryWrapped recovery. Without this the connection
			// would loop forever: reconnect -> skip -> reject -> permanent error.
			if skipInit && c.shouldRetryFullInit(err) {
				c.log.Debug("Cached init rejected by server; recovering with full initConnection",
					zap.Int("dc_id", c.dc), zap.Int64("init_version", version),
				)
				c.initCache.Delete(c.dc)
				skipInit = false
				return errors.Wrap(err, "cached init rejected")
			}
			// Not retrying other errors.
			return backoff.Permanent(errors.Wrap(err, "invoke"))
		}

		return nil
	}, c.connBackoff(ctx), func(err error, duration time.Duration) {
		c.log.Debug("Retrying connection initialization",
			zap.Error(err), zap.Duration("duration", duration),
		)
	}); err != nil {
		return errors.Wrap(err, "initConnection")
	}

	if c.setup != nil {
		if err := c.setup(ctx, c); err != nil {
			return errors.Wrap(err, "setup")
		}
	}

	c.mux.Lock()
	c.latest = c.clock.Now()
	c.cfg = cfg
	c.mux.Unlock()

	// Record the negotiated init version only after a successful full init
	// (as Android records it after a non-error reply), so a failed init is
	// retried, not cached.
	if !skipInit {
		c.initCache.Set(c.dc, version)
	}

	c.gotConfig.Signal()
	err := c.flushPendingSession()
	return err
}

// initRequest builds the init() invoke. When skip is true it sends a bare
// help.getConfig (relying on the server-side cached, per-auth-key init state),
// matching how the official Telegram Android client issues bare requests once a
// DC's auth-key is inited; otherwise it sends the full
// invokeWithLayer(initConnection(getConfig)).
func (c *Conn) initRequest(skip bool) bin.Object {
	if skip {
		return c.wrapRequest(&tg.HelpGetConfigRequest{})
	}
	q := c.wrapRequest(&tg.InitConnectionRequest{
		APIID:          c.appID,
		DeviceModel:    c.device.DeviceModel,
		SystemVersion:  c.device.SystemVersion,
		AppVersion:     c.device.AppVersion,
		SystemLangCode: c.device.SystemLangCode,
		LangPack:       c.device.LangPack,
		LangCode:       c.device.LangCode,
		Proxy:          c.device.Proxy,
		Params:         c.device.Params,
		Query:          c.wrapRequest(&tg.HelpGetConfigRequest{}),
	})
	return c.wrapRequest(&tg.InvokeWithLayerRequest{
		Layer: tg.Layer,
		Query: q,
	})
}

// shouldRetryFullInit reports whether a skip-path init error signals that the
// server's init state (which is bound to the auth_key) diverged from our cache
// and a full initConnection must be re-sent. Mirrors shouldCDNRetryWrapped.
func (c *Conn) shouldRetryFullInit(err error) bool {
	rpcErr, ok := tgerr.As(err)
	if !ok {
		return false
	}
	return rpcErr.IsOneOf(
		"CONNECTION_NOT_INITED",
		"CONNECTION_LAYER_INVALID",
	)
}

// initVersion returns a stable hash of the initConnection payload identity
// (app id, device, system and language parameters) plus the connection's
// auth_key_id. A change in any of these forces a fresh initConnection,
// mirroring how the Telegram Android client re-initializes on app build /
// language change. Including the auth_key_id self-busts the per-DC cache when a
// DC's cached init version outlives its auth_key (restored session whose key
// was rotated server-side, or migrate-away-and-back with a fresh key), because
// the server's init state is bound to the auth_key.
func (c *Conn) initVersion() int64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d",
		c.appID,
		c.device.DeviceModel,
		c.device.SystemVersion,
		c.device.AppVersion,
		c.device.SystemLangCode,
		c.device.LangPack,
		c.device.LangCode,
		c.authKeyID(),
	)
	return int64(h.Sum64())
}

// authKeyID returns the fingerprint of the connection's stable auth key, or 0
// if no key is established yet. It is read from the live MTProto session, which
// is cheap and lock-guarded by the proto connection.
//
// In PFS mode the runtime Key is the per-connection temporary key, which is
// regenerated on every reconnect; keying the init version off it would bust the
// cache on every connection and defeat the purpose. We therefore use the stable
// permanent key (PermKey) in PFS — init state is bound to the permanent key and
// survives temporary-key rotation, matching the official client. Outside PFS,
// PermKey is zero and Key is the long-lived key.
func (c *Conn) authKeyID() int64 {
	s := c.proto.Session()
	if !s.PermKey.Zero() {
		return s.PermKey.IntID()
	}
	return s.Key.IntID()
}

// Ping calls ping for underlying protocol connection.
func (c *Conn) Ping(ctx context.Context) error {
	return c.proto.Ping(ctx)
}
