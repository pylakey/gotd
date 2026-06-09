package manager

import (
	"context"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// testBackoff is a fast, non-blocking backoff for unit tests.
func testBackoff(ctx context.Context) backoff.BackOff {
	return backoff.WithContext(backoff.NewConstantBackOff(0), ctx)
}

// keyedProto reports a configurable auth key id via Session(), so tests can
// exercise the auth_key_id contribution to the init-version identity.
type keyedProto struct {
	keyID int64
}

func (*keyedProto) Invoke(context.Context, bin.Encoder, bin.Decoder) error { return nil }

func (*keyedProto) Run(ctx context.Context, f func(ctx context.Context) error) error {
	return f(ctx)
}

func (*keyedProto) Ping(context.Context) error { return nil }

func (p *keyedProto) Session() mtproto.Session {
	var key crypto.AuthKey
	key.SetIntID(p.keyID)
	return mtproto.Session{Key: key}
}

// TestInitVersionChangesWithAuthKeyID verifies R6 path (a): the init-version
// identity must include the connection's auth_key_id, so that when a DC's
// cached init version outlives its auth_key (restored session whose key was
// rotated server-side), the version changes and the cache self-busts instead of
// skipping init forever.
func TestInitVersionChangesWithAuthKeyID(t *testing.T) {
	a := require.New(t)

	mk := func(keyID int64) int64 {
		c := newTestConn(ConnModeUpdates, &keyedProto{keyID: keyID})
		c.appID = 42
		c.device = DeviceConfig{
			DeviceModel:    "Pixel",
			SystemVersion:  "Android 14",
			AppVersion:     "10.0.0",
			SystemLangCode: "en",
			LangPack:       "android",
			LangCode:       "en",
		}
		return c.initVersion()
	}

	base := mk(0x1111)
	a.Equal(base, mk(0x1111), "identical auth_key_id must hash identically")
	a.NotEqual(base, mk(0x2222), "changing auth_key_id must change the init version")
}

// notInitedSkipProto answers the bare invokeWithLayer(getConfig) skip-path
// request with CONNECTION_NOT_INITED (auth-key/init-state divergence), then
// succeeds for the full initConnection retry. It records every request so the
// test can assert the recovery sequence.
type notInitedSkipProto struct {
	calls         []bin.Encoder
	notInitedLeft int
}

func (p *notInitedSkipProto) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	p.calls = append(p.calls, input)

	// A bare invokeWithLayer(getConfig) (skip path) carries no initConnection.
	if !requestHasInitConnection(input) && p.notInitedLeft > 0 {
		p.notInitedLeft--
		return tgerr.New(400, "CONNECTION_NOT_INITED")
	}

	if cfg, ok := output.(*tg.Config); ok {
		cfg.ThisDC = 203
	}
	return nil
}

func (*notInitedSkipProto) Run(ctx context.Context, f func(ctx context.Context) error) error {
	return f(ctx)
}

func (*notInitedSkipProto) Ping(context.Context) error { return nil }

func (*notInitedSkipProto) Session() mtproto.Session { return mtproto.Session{} }

// requestHasInitConnection unwraps invokeWithLayer/invokeWithoutUpdates layers
// and reports whether the innermost wrapper is an initConnection request.
func requestHasInitConnection(input bin.Encoder) bool {
	var cur any = input
	for {
		switch v := cur.(type) {
		case *tg.InvokeWithLayerRequest:
			cur = v.Query
		case *tg.InvokeWithoutUpdatesRequest:
			cur = v.Query
		case *tg.InitConnectionRequest:
			return true
		default:
			return false
		}
	}
}

// TestConnInitSkipPathRecoversOnNotInited verifies R6 path (b): when the init
// cache reports Done for the DC but the server rejects the bare
// invokeWithLayer(getConfig) with CONNECTION_NOT_INITED, the connection must
// clear the cache entry, retry with the FULL initConnection, and become ready
// instead of erroring permanently (which would loop: reconnect -> skip -> fail).
func TestConnInitSkipPathRecoversOnNotInited(t *testing.T) {
	a := require.New(t)

	cache := NewInitVersionCache()
	p := &notInitedSkipProto{notInitedLeft: 1}

	c := &Conn{
		mode:        ConnModeUpdates,
		dc:          203,
		appID:       42,
		device:      DeviceConfig{AppVersion: "test-app"},
		proto:       p,
		clock:       clock.System,
		log:         zap.NewNop(),
		handler:     NoopHandler{},
		initCache:   cache,
		sessionInit: tdsync.NewReady(),
		gotConfig:   tdsync.NewReady(),
		dead:        tdsync.NewReady(),
		connBackoff: testBackoff,
	}

	// Seed the cache so the connection takes the skip path on init.
	version := c.initVersion()
	cache.Set(c.dc, version)
	a.True(cache.Done(c.dc, version))

	a.NoError(c.init(context.Background()), "init must recover, not fail permanently")

	select {
	case <-c.gotConfig.Ready():
	case <-time.After(time.Second):
		a.Fail("gotConfig must be signaled after recovery")
	}

	// First request was the bare skip-path getConfig (no initConnection),
	// the last was the full recovery initConnection.
	a.GreaterOrEqual(len(p.calls), 2, "must send bare then full init")
	a.False(requestHasInitConnection(p.calls[0]), "first request is the bare skip-path getConfig")
	a.True(requestHasInitConnection(p.calls[len(p.calls)-1]), "recovery must resend full initConnection")

	c.mux.Lock()
	cfg := c.cfg
	c.mux.Unlock()
	a.Equal(203, cfg.ThisDC)
}
