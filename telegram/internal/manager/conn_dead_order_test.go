package manager

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/tg"
)

type stubProto struct {
	session mtproto.Session
	runErr  error
}

func (p *stubProto) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	return nil
}

func (p *stubProto) Run(ctx context.Context, f func(ctx context.Context) error) error {
	return p.runErr
}

func (p *stubProto) Ping(ctx context.Context) error { return nil }

func (p *stubProto) Session() mtproto.Session { return p.session }

type deadOrderHandler struct {
	dead            *tdsync.Ready
	calls           int
	deadAtOnSession bool
}

func (h *deadOrderHandler) OnSession(cfg tg.Config, s mtproto.Session) error {
	h.calls++
	select {
	case <-h.dead.Ready():
		h.deadAtOnSession = true
	default:
	}
	return nil
}

func (h *deadOrderHandler) OnMessage(b *bin.Buffer) error { return nil }

// TestConnRunSignalsDeadBeforeShutdownCarry: when a conn dies, the dead signal
// must fire BEFORE carryLiveSession drives the shutdown OnSession edge. The
// store-and-resend replay (Client.replayLiveRequests) spawned from that edge
// discriminates "never reached the server" via pool.ErrConnDead from
// waitSession; if dead is not yet signaled, the replay hits the force-closed
// engine instead and burns one of its maxReplay attempts on every reconnect.
func TestConnRunSignalsDeadBeforeShutdownCarry(t *testing.T) {
	a := require.New(t)

	dead := tdsync.NewReady()
	h := &deadOrderHandler{dead: dead}
	c := &Conn{
		log:         zap.NewNop(),
		handler:     h,
		proto:       &stubProto{session: mtproto.Session{ID: 42}, runErr: errors.New("transport died")},
		sessionInit: tdsync.NewReady(),
		gotConfig:   tdsync.NewReady(),
		dead:        dead,
	}
	// Simulate a previously-ready conn so carryLiveSession proceeds.
	c.mux.Lock()
	c.cfg = tg.Config{ThisDC: 2}
	c.mux.Unlock()
	c.gotConfig.Signal()

	a.Error(c.Run(context.Background()))
	a.Equal(1, h.calls, "shutdown carry must drive OnSession exactly once")
	a.True(h.deadAtOnSession, "dead must be signaled before the shutdown-carry OnSession edge")
}

// TestConnWaitSessionPrefersDeadOverReady: on a conn that was ready and then
// died, BOTH gotConfig and dead are signaled; select picks among ready cases
// at random, so without an explicit priority ~50% of Invokes on a dying conn
// would proceed into the closed engine instead of returning pool.ErrConnDead.
func TestConnWaitSessionPrefersDeadOverReady(t *testing.T) {
	c := &Conn{
		sessionInit: tdsync.NewReady(),
		gotConfig:   tdsync.NewReady(),
		dead:        tdsync.NewReady(),
	}
	c.gotConfig.Signal()
	c.dead.Signal()

	for range 100 {
		require.ErrorIs(t, c.waitSession(context.Background()), pool.ErrConnDead)
	}
}
