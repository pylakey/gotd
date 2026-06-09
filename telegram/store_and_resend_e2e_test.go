package telegram_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/gotd/td/session"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/tgtest/cluster"
	"github.com/gotd/td/transport"
)

// TestStoreAndResend_AcrossReconnect verifies R8: a single in-flight read-only
// RPC whose connection is dropped mid-flight is transparently replayed on the
// freshly reconnected conn (with a FRESH msg_id) and returns its result to the
// caller — instead of failing with "engine was closed". Mirrors the official
// client keeping requests in runningRequests above the swappable socket.
//
// The server drops the conn on first receipt and answers only the resend, so the
// caller's single Invoke can only succeed via the store-and-resend path.
func TestStoreAndResend_AcrossReconnect(t *testing.T) {
	t.Parallel()

	log := zaptest.NewLogger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	g := tdsync.NewCancellableGroup(ctx)

	c := cluster.NewCluster(cluster.Options{
		Logger:   log.Named("cluster"),
		Protocol: transport.Intermediate,
	})

	var (
		mu      sync.Mutex
		msgIDs  []int64
		dropped bool
	)
	var srv *tgtest.Server
	srv, _ = c.DC(2, "server")
	c.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
		func(server *tgtest.Server, req *tgtest.Request) error {
			mu.Lock()
			msgIDs = append(msgIDs, req.MsgID)
			first := !dropped
			if first {
				dropped = true
			}
			mu.Unlock()

			if first {
				// Drop the conn (keep the auth key) and do NOT answer: the only way
				// the caller's single Invoke can complete is the resend on the new
				// conn.
				srv.ForceDisconnect(req.Session)
				return nil
			}
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
			// High retry interval so the per-conn rpc engine does NOT resend the
			// same msg_id on the dying conn before ForceClose unwinds it — keeping
			// the "fresh msg_id on the new conn" assertion deterministic.
			RetryInterval: 10 * time.Second,
			// NoUpdates so the client does NOT make its internal users.getUsers
			// (Self) subscribe call, which would otherwise also hit this handler and
			// make the drop-on-first logic non-deterministic. The single test RPC
			// below is then the only users.getUsers the server sees.
			NoUpdates: true,
		}
		client := telegram.NewClient(1, "hash", opts)

		return client.Run(ctx, func(ctx context.Context) error {
			raw := tg.NewClient(client)

			// A SINGLE call. It is dropped mid-flight and must succeed via the
			// store-and-resend replay on the new conn — no caller-visible error.
			res, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
			require.NoError(t, err, "single Invoke must succeed despite the mid-RPC drop")
			require.Len(t, res, 1, "result delivered exactly once")

			mu.Lock()
			ids := append([]int64(nil), msgIDs...)
			mu.Unlock()

			require.GreaterOrEqual(t, len(ids), 2, "server must observe the request at least twice (drop + resend)")
			require.NotEqual(t, ids[0], ids[len(ids)-1], "the resend must carry a FRESH msg_id (new conn)")

			cancel()
			return nil
		})
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err)
	}
}
