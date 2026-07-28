package mtproto

import (
	"context"
	"crypto/rand"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gotd/log"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/transport"
)

// hangingConn never returns from Recv until closed. It models a raw socket
// read that ignores ctx entirely and can only be interrupted by Close(),
// which is exactly how transport.connection behaves: it derives a deadline
// from ctx.Deadline() and never watches ctx.Done().
type hangingConn struct {
	closed    chan struct{}
	started   chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func newHangingConn() *hangingConn {
	return &hangingConn{
		closed:  make(chan struct{}),
		started: make(chan struct{}),
	}
}

func (c *hangingConn) Send(ctx context.Context, b *bin.Buffer) error { return nil }

func (c *hangingConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.once.Do(func() { close(c.started) })
	<-c.closed
	return context.Canceled
}

func (c *hangingConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

var _ transport.Conn = (*hangingConn)(nil)

// TestConnectIsInterruptibleByContext is the regression test for the window
// this change closes: while connect() runs the key exchange, Conn.Run has not
// started its goroutine group yet, so handleClose does not exist and a
// cancelled ctx cannot close the socket. Callers that cancel and then wait --
// pool.DC.Close() is cancel() followed by Supervisor.Wait() -- hang forever.
func TestConnectIsInterruptibleByContext(t *testing.T) {
	hanging := newHangingConn()

	conn := New(func(ctx context.Context) (transport.Conn, error) {
		return hanging, nil
	}, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- conn.Run(ctx, func(ctx context.Context) error { return nil })
	}()

	// Wait until connect() is actually parked in the exchange read rather
	// than guessing with a sleep.
	select {
	case <-hanging.started:
	case <-time.After(2 * time.Second):
		t.Fatal("connect() never reached the exchange read")
	}

	cancel()

	select {
	case err := <-done:
		// The forced Close() breaks the parked read with a raw transport
		// error; connect() must report it alongside ctx.Err() so callers
		// classifying with errors.Is see cancellation.
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("connect did not return after context cancellation")
	}
}

// TestConnectWatcherDoesNotLeakOnSuccess proves the watcher terminates when
// connect() returns successfully. A goroutine leaked per connection attempt
// would be worse than the hang this fixes.
func TestConnectWatcherDoesNotLeakOnSuccess(t *testing.T) {
	a := require.New(t)

	closeMe := &closeConn{}
	c := Conn{
		dialer: func(ctx context.Context) (transport.Conn, error) {
			return closeMe, nil
		},
		clock: clock.System,
		authKey: crypto.AuthKey{
			ID: [8]byte{1}, // Non-zero: skips the key exchange entirely.
		},
		rand: rand.Reader, // newSessionID succeeds, so connect() returns nil.
		log:  log.For(log.Nop),
		// Real Conns always carry a positive DialTimeout (Options defaults it
		// to 35s). Leaving this zero would make connectCtx expire instantly on
		// every iteration and the watcher would force-close on success.
		dialTimeout: time.Minute,
	}

	before := runtime.NumGoroutine()

	const iterations = 50
	for range iterations {
		a.NoError(c.connect(context.Background()))
	}

	// Poll here rather than via require.Eventually: that helper evaluates its
	// condition in a freshly spawned goroutine, which runtime.NumGoroutine()
	// would itself count, making the check unsatisfiable either way.
	deadline := time.Now().Add(time.Second)
	var after int
	for {
		after = runtime.NumGoroutine()
		if after <= before || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.LessOrEqual(after, before, "watcher goroutines leaked after successful connect()")
}

// TestConnectDialTimeoutDuringExchangeNormalizesToDeadlineExceeded covers the
// reason the watcher observes connectCtx rather than ctx. In non-PFS mode
// connectCtx additionally carries DialTimeout, and a transport wrapper that
// ignores context deadlines would otherwise stay parked past it with nothing
// left to interrupt the read.
func TestConnectDialTimeoutDuringExchangeNormalizesToDeadlineExceeded(t *testing.T) {
	a := require.New(t)

	hanging := newHangingConn()

	c := New(func(ctx context.Context) (transport.Conn, error) {
		return hanging, nil
	}, Options{DialTimeout: 200 * time.Millisecond})

	// The caller's ctx is never cancelled; only the internal dial timeout
	// fires while the exchange read is parked in hanging.Recv.
	err := c.connect(context.Background())
	a.Error(err)
	a.ErrorIs(err, context.DeadlineExceeded)
}

// TestConnectGenuineExchangeErrorIsNotRewritten pins the condition guarding
// normalization: it applies only when the watcher actually closed the socket,
// never merely because a context happens to be done by the time connect()
// returns. Both contexts stay alive for this whole synchronous call, so this
// failure must surface unchanged and must not gain a context error.
func TestConnectGenuineExchangeErrorIsNotRewritten(t *testing.T) {
	a := require.New(t)

	failing := &closeConn{}
	c := New(func(ctx context.Context) (transport.Conn, error) {
		return failing, nil
	}, Options{DialTimeout: time.Minute})

	err := c.connect(context.Background())
	a.Error(err)
	a.ErrorIs(err, io.EOF, "genuine exchange error must survive unchanged")
	a.NotErrorIs(err, context.Canceled)
	a.NotErrorIs(err, context.DeadlineExceeded)
}
