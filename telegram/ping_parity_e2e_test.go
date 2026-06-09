package telegram_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// TestPingParity_DecoupledDisconnectDelay verifies R1 on the wire: the
// disconnect_delay announced in ping_delay_disconnect equals the configured
// PingDisconnectDelay (35s), decoupled from the deliberately tiny 50ms ping
// cadence. This proves both that the cadence is configurable (pings arrive
// rapidly) and that disconnect_delay is no longer derived from
// PingInterval+PingTimeout (which would be ~50ms, not 35s).
func TestPingParity_DecoupledDisconnectDelay(t *testing.T) {
	var (
		mu     sync.Mutex
		delays []int
	)
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(delays)
	}

	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		srv, _ := s.Cluster.DC(2, "server")
		srv.SetOnPing(func(_ *tgtest.Request, disconnectDelay int) {
			mu.Lock()
			delays = append(delays, disconnectDelay)
			mu.Unlock()
		})
	}, func(ctx context.Context, c clientSetup) error {
		opts := c.Options
		opts.NoUpdates = true
		opts.PingInterval = 50 * time.Millisecond
		opts.PingTimeout = 35 * time.Second
		opts.PingDisconnectDelay = 35 * time.Second
		client := telegram.NewClient(1, "hash", opts)

		return client.Run(ctx, func(ctx context.Context) error {
			require.Eventually(c.TB, func() bool { return count() >= 3 },
				30*time.Second, 10*time.Millisecond,
				"expected several pings at the configured 50ms cadence")

			mu.Lock()
			defer mu.Unlock()
			for i, d := range delays {
				require.Equalf(c.TB, 35, d,
					"ping %d: disconnect_delay must be the decoupled 35s, not interval-derived", i)
			}
			c.Complete()
			return nil
		})
	})(t)
}
