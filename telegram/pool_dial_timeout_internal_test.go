package telegram

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/telegram/internal/manager"
)

// TestNewPoolConn_DialTimeoutAndInitCache verifies R4#1: connections created via
// the dc()/pool path (data, media-only, CDN) get the Android per-mode dial
// timeout AND a non-nil InitCache, just like the primary createConn path. Before
// the fix these pools bypassed both, falling back to mtproto's 35s default and
// never participating in the per-DC init cache.
func TestNewPoolConn_DialTimeoutAndInitCache(t *testing.T) {
	tests := []struct {
		name string
		mode manager.ConnMode
		want time.Duration
	}{
		{"data", manager.ConnModeData, dialTimeoutData},
		{"cdn", manager.ConnModeCDN, dialTimeoutData},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedTimeout time.Duration
			var capturedCache *manager.InitVersionCache

			c := &Client{log: zap.NewNop()}
			c.init()
			c.session = pool.NewSyncSession(pool.Session{DC: 2})
			c.create = func(_ mtproto.Dialer, _ manager.ConnMode, _ int, opts mtproto.Options, connOpts manager.ConnOptions) pool.Conn {
				capturedTimeout = opts.DialTimeout
				capturedCache = connOpts.InitCache
				return fingerprintNotFoundConn{}
			}

			var suppress atomic.Bool
			c.newPoolConn(5, tt.mode, c.primaryDC(5), c.opts, &suppress)

			require.Equal(t, tt.want, capturedTimeout, "per-mode dial timeout must be applied")
			require.NotNil(t, capturedCache, "pool conns must share the init-version cache")
			require.Same(t, c.initVersions, capturedCache, "pool conns must use the client's init cache")
		})
	}
}

// TestNewPoolConn_ExplicitDialTimeoutWins verifies an explicit client
// DialTimeout still takes precedence over the per-mode Android default on the
// pool path.
func TestNewPoolConn_ExplicitDialTimeoutWins(t *testing.T) {
	var capturedTimeout time.Duration

	c := &Client{log: zap.NewNop()}
	c.init()
	c.opts.DialTimeout = 7 * time.Second
	c.session = pool.NewSyncSession(pool.Session{DC: 2})
	c.create = func(_ mtproto.Dialer, _ manager.ConnMode, _ int, opts mtproto.Options, _ manager.ConnOptions) pool.Conn {
		capturedTimeout = opts.DialTimeout
		return fingerprintNotFoundConn{}
	}

	var suppress atomic.Bool
	c.newPoolConn(5, manager.ConnModeData, c.primaryDC(5), c.opts, &suppress)

	require.Equal(t, 7*time.Second, capturedTimeout)
}
