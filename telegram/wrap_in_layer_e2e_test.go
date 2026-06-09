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

// topLevelTypeID returns the type id of the outermost request in a raw request
// buffer, operating on a copy so the real buffer is left intact. It does NOT
// unwrap anything: it reports what the server observes on the wire as the
// top-level object (e.g. tg.InvokeWithLayerRequestTypeID for a wrapped request,
// or tg.UsersGetUsersRequestTypeID for a bare one).
func topLevelTypeID(raw []byte) (uint32, bool) {
	buf := &bin.Buffer{Buf: raw}
	id, err := buf.PeekID()
	if err != nil {
		return 0, false
	}
	return id, true
}

// requestWrapsInLayer reports whether the raw request buffer is (or wraps) an
// invokeWithLayer request, mirroring countsInitConnection but looking for the
// invokeWithLayer carrier instead of initConnection. Operates on a copy.
func requestWrapsInLayer(raw []byte) bool {
	buf := &bin.Buffer{Buf: raw}
	for {
		id, err := buf.PeekID()
		if err != nil {
			return false
		}
		switch id {
		case tg.InvokeWithLayerRequestTypeID:
			return true
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

// innerMethodTypeID unwraps ONLY the orthogonal invokeWithoutUpdates wrapper
// (which ConnModeData keeps even when inited) and returns the type id of the
// inner method. It deliberately does NOT unwrap invokeWithLayer/initConnection,
// so a wrapped request reports the wrapper id. Operates on a copy.
func innerMethodTypeID(raw []byte) (uint32, bool) {
	buf := &bin.Buffer{Buf: raw}
	for {
		id, err := buf.PeekID()
		if err != nil {
			return 0, false
		}
		if id != tg.InvokeWithoutUpdatesRequestTypeID {
			return id, true
		}
		r := &tg.InvokeWithoutUpdatesRequest{Query: &peekQuery{}}
		if err := r.Decode(buf); err != nil {
			return 0, false
		}
	}
}

// TestWrapInLayer_BareAfterInit proves the Android wrapInLayer parity on the
// wire: the request that establishes init for the DC is
// invokeWithLayer(initConnection(...)), but once inited the user RPC goes on the
// wire as the BARE TL method (tg.UsersGetUsersRequestTypeID) — no
// invokeWithLayer, no initConnection. Against current HEAD (every request is
// invokeWithLayer) the bare assertion FAILS.
func TestWrapInLayer_BareAfterInit(t *testing.T) {
	var (
		sawInitConnection atomic.Bool
		sawWithLayer      atomic.Bool
		sawBareGetUsers   atomic.Bool
	)

	testCluster(transport.Intermediate, false, func(s clusterSetup) {
		server, d := s.Cluster.DC(2, "server")
		server.SetOnRequest(func(req *tgtest.Request) {
			raw := req.Buf.Copy()
			if countsInitConnection(raw) {
				sawInitConnection.Store(true)
			}
			if requestWrapsInLayer(raw) {
				sawWithLayer.Store(true)
			}
			if id, ok := topLevelTypeID(raw); ok && id == tg.UsersGetUsersRequestTypeID {
				sawBareGetUsers.Store(true)
			}
		})
		d.Vector(tg.UsersGetUsersRequestTypeID, user)
	}, func(ctx context.Context, c clientSetup) error {
		// Default options: updates enabled => ConnModeUpdates => a bare inited
		// request is the TRULY bare TL method (no invokeWithoutUpdates either).
		client := telegram.NewClient(1, "hash", c.Options)
		return client.Run(ctx, func(ctx context.Context) error {
			defer c.Complete()
			api := client.API()

			// First RPC drives the connection ready. By the time it returns, init
			// for DC 2 has been negotiated via invokeWithLayer(initConnection(...)).
			if _, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
				return errors.Wrap(err, "first RPC")
			}
			// A second, definitely post-init RPC.
			if _, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
				return errors.Wrap(err, "second RPC")
			}

			if !sawInitConnection.Load() {
				return errors.New("init must establish via invokeWithLayer(initConnection(...))")
			}
			if !sawWithLayer.Load() {
				return errors.New("init must use invokeWithLayer as the carrier of initConnection")
			}
			if !sawBareGetUsers.Load() {
				return errors.New("post-init RPC must go on the wire as a bare users.getUsers (no invokeWithLayer)")
			}
			return nil
		})
	})(t)
}

// TestWrapInLayer_SkipReconnectBare extends the R6 ForceDisconnect e2e: after a
// same-DC reconnect where init is skipped (cached perm-key init version), NO
// invokeWithLayer and NO initConnection are sent for the whole reconnected
// connection — getConfig and user RPCs are bare — and the client still becomes
// ready and serves RPCs.
func TestWrapInLayer_SkipReconnectBare(t *testing.T) {
	var (
		// reconnected gates wire inspection to the post-reconnect connection only.
		reconnected           atomic.Bool
		withLayerAfterReconn  atomic.Bool
		initConnAfterReconn   atomic.Bool
		bareGetUsersAfterReco atomic.Bool
	)

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
			if !reconnected.Load() {
				return
			}
			raw := req.Buf.Copy()
			if requestWrapsInLayer(raw) {
				withLayerAfterReconn.Store(true)
			}
			if countsInitConnection(raw) {
				initConnAfterReconn.Store(true)
			}
			// ConnModeData keeps the orthogonal invokeWithoutUpdates wrapper, so a
			// bare RPC is invokeWithoutUpdates(users.getUsers): unwrap only that
			// wrapper, then assert the inner method is the raw users.getUsers.
			if id, ok := innerMethodTypeID(raw); ok && id == tg.UsersGetUsersRequestTypeID {
				bareGetUsersAfterReco.Store(true)
			}
		})
		d.Vector(tg.UsersGetUsersRequestTypeID, user)
	}, func(ctx context.Context, c clientSetup) error {
		opts := c.Options
		opts.NoUpdates = true
		client := telegram.NewClient(1, "hash", opts)
		return client.Run(ctx, func(ctx context.Context) error {
			defer c.Complete()
			api := client.API()

			// Drive the first connection ready (full init happens here, before
			// the reconnect gate is open, so it is not inspected).
			if _, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
				return errors.Wrap(err, "first RPC")
			}

			sess, ok := currentSession()
			if !ok {
				return errors.New("server must have observed a session")
			}
			// Open the inspection gate for everything the reconnected connection
			// sends, then drop the session to force a same-DC reconnect (auth key
			// preserved => init skipped via cached perm-key version).
			reconnected.Store(true)
			mu.Lock()
			server := srv
			mu.Unlock()
			server.ForceDisconnect(sess)

			ready := false
			deadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(deadline) {
				rctx, cancel := context.WithTimeout(ctx, time.Second)
				_, err := api.UsersGetUsers(rctx, []tg.InputUserClass{&tg.InputUserSelf{}})
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

			if withLayerAfterReconn.Load() {
				return errors.New("skip-path reconnect must not send invokeWithLayer")
			}
			if initConnAfterReconn.Load() {
				return errors.New("skip-path reconnect must not send initConnection")
			}
			if !bareGetUsersAfterReco.Load() {
				return errors.New("skip-path reconnect must serve a bare users.getUsers RPC")
			}
			return nil
		})
	})(t)
}
