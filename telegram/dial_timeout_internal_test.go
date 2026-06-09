package telegram

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/telegram/internal/manager"
)

// TestCreateConn_DialTimeoutPerMode verifies R4#1: each connection mode gets the
// Android per-type connect timeout when the caller leaves DialTimeout unset.
func TestCreateConn_DialTimeoutPerMode(t *testing.T) {
	tests := []struct {
		mode manager.ConnMode
		want time.Duration
	}{
		{manager.ConnModeUpdates, dialTimeoutGeneric},
		{manager.ConnModeData, dialTimeoutData},
		{manager.ConnModeCDN, dialTimeoutData},
	}
	for _, tt := range tests {
		var captured time.Duration
		c := &Client{log: zap.NewNop()}
		c.init()
		c.session = pool.NewSyncSession(pool.Session{DC: 2})
		c.create = func(_ mtproto.Dialer, _ manager.ConnMode, _ int, opts mtproto.Options, _ manager.ConnOptions) pool.Conn {
			captured = opts.DialTimeout
			return fingerprintNotFoundConn{}
		}

		c.createConn(0, tt.mode, nil, nil)
		require.Equalf(t, tt.want, captured, "mode %v", tt.mode)
	}
}

// TestCreateConn_ExplicitDialTimeoutWins verifies an explicit client DialTimeout
// takes precedence over the per-mode Android default.
func TestCreateConn_ExplicitDialTimeoutWins(t *testing.T) {
	var captured time.Duration
	c := &Client{log: zap.NewNop()}
	c.init()
	c.opts.DialTimeout = 7 * time.Second
	c.session = pool.NewSyncSession(pool.Session{DC: 2})
	c.create = func(_ mtproto.Dialer, _ manager.ConnMode, _ int, opts mtproto.Options, _ manager.ConnOptions) pool.Conn {
		captured = opts.DialTimeout
		return fingerprintNotFoundConn{}
	}

	c.createConn(0, manager.ConnModeUpdates, nil, nil)
	require.Equal(t, 7*time.Second, captured)
}
