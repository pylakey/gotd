package telegram_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/transport"
)

// countsInitConnection reports whether the raw request buffer is (or wraps) an
// initConnection request. It mirrors tgtest.UnpackInvoke's unwrapping of
// invokeWithLayer / invokeWithoutUpdates, but counts initConnection instead of
// transparently unwrapping it. It operates on a copy so the real request buffer
// is left intact for the server's handler.
func countsInitConnection(raw []byte) bool {
	buf := &bin.Buffer{Buf: raw}
	for {
		id, err := buf.PeekID()
		if err != nil {
			return false
		}
		switch id {
		case tg.InitConnectionRequestTypeID:
			return true
		case tg.InvokeWithLayerRequestTypeID:
			r := &tg.InvokeWithLayerRequest{Query: &peekQuery{}}
			if err := r.Decode(buf); err != nil {
				return false
			}
		case tg.InvokeWithoutUpdatesRequestTypeID:
			r := &tg.InvokeWithoutUpdatesRequest{Query: &peekQuery{}}
			if err := r.Decode(buf); err != nil {
				return false
			}
		default:
			return false
		}
	}
}

// peekQuery decodes only the type id of a nested query, leaving the rest of the
// buffer positioned at the start of that nested query so the unwrap loop can
// continue inspecting it.
type peekQuery struct{}

func (*peekQuery) Decode(b *bin.Buffer) error {
	_, err := b.PeekID()
	return err
}

func (*peekQuery) Encode(*bin.Buffer) error { return nil }

// TestInitConnectionCache_NoSecondInitOnReconnect proves R6 on the wire: after
// the first initConnection to a DC, a forced reconnect to the SAME DC (auth key
// preserved) does NOT send a second initConnection. The reconnected client must
// still become ready, proving the cached path refreshes config via a bare
// invokeWithLayer(help.getConfig).
func TestInitConnectionCache_NoSecondInitOnReconnect(t *testing.T) {
	var initConnections atomic.Int64

	var (
		mu      sync.Mutex
		srv     *tgtest.Server
		session tgtest.Session
		haveSID bool
	)
	captureSession := func(s tgtest.Session) {
		mu.Lock()
		defer mu.Unlock()
		if !haveSID {
			session = s
			haveSID = true
		}
	}
	currentSession := func() (tgtest.Session, bool) {
		mu.Lock()
		defer mu.Unlock()
		return session, haveSID
	}

	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		server, d := s.Cluster.DC(2, "server")
		mu.Lock()
		srv = server
		mu.Unlock()
		server.SetOnRequest(func(req *tgtest.Request) {
			captureSession(req.Session)
			if countsInitConnection(req.Buf.Copy()) {
				initConnections.Add(1)
			}
		})
		// A trivial RPC the test can issue after (re)connect to confirm the
		// connection is usable. help.getConfig is already served by the cluster
		// common dispatcher, so we add an explicit users.getUsers handler.
		d.Vector(tg.UsersGetUsersRequestTypeID, user)
	}, func(ctx context.Context, c clientSetup) error {
		opts := c.Options
		opts.NoUpdates = true
		client := telegram.NewClient(1, "hash", opts)

		// The callback returns errors instead of asserting in-place: assertions are
		// surfaced by the harness via require.NoError on g.Wait() in the main test
		// goroutine, and c.Complete() always runs so the cluster shuts down promptly
		// on any outcome (instead of hanging until the context deadline).
		return client.Run(ctx, func(ctx context.Context) error {
			defer c.Complete()
			api := client.API()

			// First RPC over the initial connection. By the time this returns the
			// connection is ready, which means the (single) initConnection for DC
			// 2 has already been sent.
			if _, err := api.UsersGetUsers(ctx, []tg.InputUserClass{
				&tg.InputUserSelf{},
			}); err != nil {
				return errors.Wrap(err, "first RPC")
			}
			if got := initConnections.Load(); got != 1 {
				return errors.Errorf("exactly one initConnection expected after first connect, got %d", got)
			}

			// Force a reconnect to the SAME DC. ForceDisconnect drops the MTProto
			// session (session_id) but keeps the auth key, so the client reconnects
			// without re-running the key exchange.
			sess, ok := currentSession()
			if !ok {
				return errors.New("server must have observed a session")
			}
			mu.Lock()
			server := srv
			mu.Unlock()
			server.ForceDisconnect(sess)

			// Issue RPCs until the reconnected connection becomes ready again. The
			// reconnect must reuse the cached init version (bare
			// invokeWithLayer(help.getConfig)) and therefore must not send a second
			// initConnection.
			ready := false
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				rctx, cancel := context.WithTimeout(ctx, time.Second)
				_, err := api.UsersGetUsers(rctx, []tg.InputUserClass{
					&tg.InputUserSelf{},
				})
				cancel()
				if err == nil {
					ready = true
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(50 * time.Millisecond):
				}
			}
			if !ready {
				return errors.New("reconnected connection must become ready and serve RPCs")
			}

			if got := initConnections.Load(); got != 1 {
				return errors.Errorf("reconnect to the same DC must not send a second initConnection, got %d", got)
			}
			return nil
		})
	})(t)
}
