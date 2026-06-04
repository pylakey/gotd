package telegram

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/telegram/internal/manager"
	"github.com/gotd/td/tg"
)

func newResyncTestClient(t *testing.T) (*Client, chan tg.UpdatesClass) {
	t.Helper()
	got := make(chan tg.UpdatesClass, 4)

	client := &Client{
		log:     zaptest.NewLogger(t),
		rand:    rand.New(rand.NewSource(1)),
		appID:   TestAppID,
		appHash: TestAppHash,
		clock:   clock.System,
		session: pool.NewSyncSession(pool.Session{
			DC: 2,
		}),
		newConnBackoff: defaultBackoff(clock.System),
		ctx:            context.Background(),
		cancel:         func() {},
		updateHandler: UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			got <- u
			return nil
		}),
		create: func(
			create mtproto.Dialer,
			mode manager.ConnMode,
			appID int,
			opts mtproto.Options,
			connOpts manager.ConnOptions,
		) pool.Conn {
			ready := tdsync.NewReady()
			return &testConn{ready: ready}
		},
	}
	client.init()
	return client, got
}

func expectSyntheticTooLong(t *testing.T, got chan tg.UpdatesClass) {
	t.Helper()
	select {
	case u := <-got:
		require.IsType(t, &tg.UpdatesTooLong{}, u,
			"resync must push a synthetic UpdatesTooLong (mapped to getDifference by telegram/updates)")
	case <-time.After(5 * time.Second):
		t.Fatal("expected a synthetic UpdatesTooLong push")
	}
}

// TestOnSessionTriggersUpdatesResync: every session edge (new_session_created
// after a reconnect, in-conn recreateSession) must force the updates manager
// to refetch the difference — Android (tgnet) forces getDifference on every
// reconnect. Without it, pushes sent into the dead-transport window are
// invisible until the next pts gap or the 15-minute idle timer.
func TestOnSessionTriggersUpdatesResync(t *testing.T) {
	client, got := newResyncTestClient(t)

	require.NoError(t, client.onSession(tg.Config{ThisDC: 2}, mtproto.Session{ID: 10, Salt: 10}))
	expectSyntheticTooLong(t, got)
}

// TestRestartPrimaryConnTriggersUpdatesResync: a continued-session reconnect
// can produce NO new_session_created (hence no onSession edge on the new
// conn), so the conn-swap path must schedule the resync itself.
func TestRestartPrimaryConnTriggersUpdatesResync(t *testing.T) {
	client, got := newResyncTestClient(t)
	client.conn = client.createConn(0, manager.ConnModeUpdates, nil, nil)

	client.restartPrimaryConn(errors.New("conn dead"))
	expectSyntheticTooLong(t, got)
}

// TestResyncUpdatesSkippedInNoUpdatesMode: in NoUpdates mode the server does
// not push updates to this client at all — a synthetic resync is pointless.
func TestResyncUpdatesSkippedInNoUpdatesMode(t *testing.T) {
	client, got := newResyncTestClient(t)
	client.noUpdatesMode = true

	client.resyncUpdates()

	select {
	case u := <-got:
		t.Fatalf("no update expected in noUpdatesMode, got %T", u)
	case <-time.After(100 * time.Millisecond):
	}
}
