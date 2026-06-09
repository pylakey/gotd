package telegram_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/gotd/td/session"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/tgtest/cluster"
	"github.com/gotd/td/transport"
)

// TestSessionContinuity_AcrossReconnect verifies R2: a plain reconnect to the
// same DC (forced by dropping the server-side connection but keeping the auth
// key) preserves the MTProto session_id and continues the seqno, and performs
// NO new key exchange. Today (before R2) each reconnect mints a fresh
// session_id and resets seqno.
func TestSessionContinuity_AcrossReconnect(t *testing.T) {
	t.Parallel()

	// Observer captures client-side "Generating new auth key" log lines so we
	// can assert the handshake happens exactly once across the reconnect.
	core, logs := observer.New(zap.InfoLevel)
	log := zap.New(zapcore.NewTee(core, zaptest.NewLogger(t).Core()))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	g := tdsync.NewCancellableGroup(ctx)

	c := cluster.NewCluster(cluster.Options{
		Logger:   log.Named("cluster"),
		Protocol: transport.Intermediate,
	})
	c.Common().Vector(tg.UsersGetUsersRequestTypeID, user)

	var (
		mu         sync.Mutex
		sessionIDs []int64
		firstSess  tgtest.Session
		haveSess   bool
	)
	// srv is captured in setup and used to force-disconnect from the run side.
	var srv *tgtest.Server
	srv, _ = c.DC(2, "server")
	c.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
		func(server *tgtest.Server, req *tgtest.Request) error {
			mu.Lock()
			sessionIDs = append(sessionIDs, req.Session.ID)
			if !haveSess {
				firstSess = req.Session
				haveSess = true
			}
			mu.Unlock()
			return server.SendVector(req, user)
		},
	)

	g.Go(c.Up)
	g.Go(func(ctx context.Context) error {
		select {
		case <-c.Ready():
		case <-ctx.Done():
			return ctx.Err()
		}

		opts := telegram.Options{
			PublicKeys:     c.Keys(),
			Resolver:       c.Resolver(),
			Logger:         log.Named("client"),
			SessionStorage: &session.StorageMemory{},
			DCList:         c.List(),
			AckBatchSize:   1,
			AckInterval:    50 * time.Millisecond,
			RetryInterval:  50 * time.Millisecond,
			UpdateHandler: telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
				return nil
			}),
		}
		client := telegram.NewClient(1, "hash", opts)

		return client.Run(ctx, func(ctx context.Context) error {
			raw := tg.NewClient(client)

			// First RPC establishes the session.
			if _, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
				return errors.Wrap(err, "first rpc")
			}

			mu.Lock()
			sess := firstSess
			mu.Unlock()

			// Force a reconnect WITHOUT losing the auth key.
			srv.ForceDisconnect(sess)

			// Second RPC after reconnect. Retry until the client has
			// re-established the connection.
			require.Eventually(t, func() bool {
				_, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
				return err == nil
			}, 60*time.Second, 100*time.Millisecond, "second rpc must succeed after reconnect")

			mu.Lock()
			ids := append([]int64(nil), sessionIDs...)
			mu.Unlock()

			require.GreaterOrEqual(t, len(ids), 2, "expected at least two recorded requests")
			first := ids[0]
			require.NotZero(t, first, "session_id must be non-zero")
			for i, id := range ids {
				require.Equalf(t, first, id,
					"session_id must be preserved across reconnect (request %d)", i)
			}

			// Exactly one key exchange (the initial handshake): a plain
			// reconnect must reuse the auth key, not re-run DH.
			handshakes := 0
			for _, e := range logs.All() {
				if e.Message == "Generating new auth key" {
					handshakes++
				}
			}
			require.Equal(t, 1, handshakes,
				"a plain reconnect must not trigger a new key exchange")

			cancel()
			return nil
		})
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err)
	}
}
