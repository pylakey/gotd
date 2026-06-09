package mtproto

import (
	"context"
	"crypto/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/gotd/neo"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/rpc"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/transport"
)

// blackholeConn models a half-open / blackholed socket: writes "succeed" (are
// dropped), reads block forever until SetReadDeadline is set to a past time, at
// which point the in-flight Recv returns an i/o timeout — exactly like a real
// net.Conn whose read deadline has elapsed.
type blackholeConn struct {
	mux         sync.Mutex
	deadlineSet bool
	wake        chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once

	recvCount int // number of Recv calls (under mux).
}

func newBlackholeConn() *blackholeConn {
	return &blackholeConn{
		wake:   make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
}

func (c *blackholeConn) Send(ctx context.Context, b *bin.Buffer) error { return nil }

func (c *blackholeConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.mux.Lock()
	c.recvCount++
	c.mux.Unlock()
	for {
		c.mux.Lock()
		dead := c.deadlineSet
		c.mux.Unlock()
		if dead {
			return newReadTimeoutErr()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return newReadTimeoutErr()
		case <-c.wake:
		}
	}
}

func (c *blackholeConn) SetReadDeadline(t time.Time) error {
	c.mux.Lock()
	c.deadlineSet = !t.IsZero()
	c.mux.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

func (c *blackholeConn) SetWriteDeadline(t time.Time) error { return nil }

func (c *blackholeConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *blackholeConn) recvCalls() int {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.recvCount
}

var _ transport.Conn = (*blackholeConn)(nil)

// newReadTimeoutErr returns the same error shape a real net.Conn produces when a
// read deadline elapses: a *net.OpError whose Err reports Timeout() == true.
// readLoop's noUpdates path recognizes this and continues (it does not surface
// it as a read failure), so the watchdog's ErrReadTimeout is the error that
// unwinds the run group — matching production behavior.
func newReadTimeoutErr() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: timeoutErr{}}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// newWatchdogConn builds a started-ready Conn over the given transport with a
// short read timeout/tick for deterministic testing. The session is pre-seeded
// (non-zero key + session id) so connect() skips key exchange and the run group
// starts immediately.
func newWatchdogConn(tb testing.TB, tr transport.Conn, c clock.Clock, ready bool) *Conn {
	var engine *rpc.Engine
	engine = rpc.New(func(ctx context.Context, msgID int64, seqNo int32, in bin.Encoder) error {
		return nil // black-hole the write; no server response ever comes.
	}, rpc.Options{Clock: c})

	opt := Options{
		Clock:     c,
		Random:    rand.Reader,
		Logger:    zaptest.NewLogger(tb),
		Key:       crypto.Key{}.WithID(),
		SessionID: 1, // non-zero -> connect skips key exchange.
		MessageID: proto.NewMessageIDGen(c.Now),
		// Push ping/ack/salt loops far out so they never fire during the bounded
		// mock-clock travel; the watchdog is the only loop under test.
		PingInterval:      time.Hour,
		PingTimeout:       time.Hour,
		AckInterval:       time.Hour,
		SaltFetchInterval: time.Hour,
		ReadTimeout:       200 * time.Millisecond,
		WatchdogTick:      5 * time.Millisecond,
		engine:            engine,
	}
	conn := New(func(ctx context.Context) (transport.Conn, error) {
		return tr, nil
	}, opt)
	conn.messageIDBuf = noopBuf{}
	conn.gotSession = tdsync.NewReady()
	if ready {
		conn.gotSession.Signal()
	}
	// Signaled by the watchdog after it registers its ticker; the mock clock is
	// not safe for concurrent ticker creation + time travel, so tests must wait
	// for this before advancing the clock.
	conn.watchdogReady = make(chan struct{})
	return conn
}

// advancePast drives the mock clock a single step past the read timeout. neo
// jumps Now() to the target first, then fires every due moment exactly once with
// the advanced time, so the watchdog's next tick carries a timestamp that
// reflects full elapsed silence. Deterministic, no real sleeps. The caller must
// have waited on conn.watchdogReady so the watchdog ticker is registered before
// the clock is advanced.
func advancePast(c *neo.Time, readTimeout, tick time.Duration) {
	c.Travel(readTimeout + 2*tick)
}

// TestReadWatchdog_SilentWithPendingWork asserts that on a silent transport with
// an in-flight RPC, conn.Run returns with ErrReadTimeout once the clock advances
// past the read timeout. Without the watchdog (pre-fix) Run wedges forever.
func TestReadWatchdog_SilentWithPendingWork(t *testing.T) {
	a := require.New(t)

	c := neo.NewTime(time.Now())
	bh := newBlackholeConn()
	conn := newWatchdogConn(t, bh, c, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- conn.Run(ctx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	// Start an in-flight RPC so hasPendingWork() is true. The send is
	// black-holed, so the request stays registered in the engine.
	invokeDone := make(chan struct{})
	go func() {
		defer close(invokeDone)
		_ = conn.Invoke(ctx, testPayload{Data: []byte{1}}, testPayload{})
	}()

	// Wait until the run group is up (Recv called), the RPC is registered, and the
	// watchdog ticker exists (so the mock clock can be advanced race-free).
	waitFor(t, func() bool { return bh.recvCalls() > 0 && conn.rpc.Pending() > 0 })
	<-conn.watchdogReady

	advancePast(c, 200*time.Millisecond, 5*time.Millisecond)

	select {
	case err := <-runErr:
		a.Error(err)
		a.ErrorIsf(err, ErrReadTimeout, "unexpected Run error: %v", err)
	case <-time.After(5 * time.Second):
		a.Fail("conn.Run wedged: watchdog did not fire on silent socket with pending work")
	}

	// Drain the in-flight RPC goroutine so it cannot log after the test returns.
	cancel()
	select {
	case <-invokeDone:
	case <-time.After(5 * time.Second):
		a.Fail("in-flight Invoke did not return after Run exit")
	}
}

// TestReadWatchdog_IdleNotChurned asserts that a ready connection with NO pending
// work is NOT force-reconnected on silence: the watchdog slides the deadline and
// keeps the connection (no idle churn).
func TestReadWatchdog_IdleNotChurned(t *testing.T) {
	a := require.New(t)

	c := neo.NewTime(time.Now())
	bh := newBlackholeConn()
	conn := newWatchdogConn(t, bh, c, true) // ready, no pending RPC.

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- conn.Run(ctx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	waitFor(t, func() bool { return bh.recvCalls() > 0 })
	<-conn.watchdogReady
	a.Zero(conn.rpc.Pending(), "no pending work expected")

	// Advance well past several read timeouts.
	for range 5 {
		advancePast(c, 200*time.Millisecond, 5*time.Millisecond)
	}

	// The watchdog must NOT have reconnected the idle conn.
	select {
	case err := <-runErr:
		a.Failf("idle conn churned", "conn.Run returned unexpectedly: %v", err)
	case <-time.After(300 * time.Millisecond):
		// Good: still running.
	}

	// Cleanup: cancel Run and wait for the group to fully shut down so no
	// goroutine logs after the test returns.
	cancel()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		a.Fail("conn.Run did not shut down after ctx cancel")
	}
}

// halfOpenConn is a production-faithful stub of transport.connection on a
// half-open / blackholed socket. It mirrors the real Recv contract that the
// watchdog must defeat:
//
//   - Recv RESETS its read deadline to zero at the TOP of every call (exactly
//     like transport/connection.go, which does SetReadDeadline(time.Time{})
//     before reading), so a past deadline set by the watchdog between calls only
//     trips the Recv that is already blocked — a fresh Recv would clear it and
//     re-block forever.
//   - When a past deadline is observed by an in-flight Recv, it returns a real
//     *net.OpError whose Err reports Timeout() == true, ONCE.
//   - Recv does NOT observe ctx (a real codec.Read on net.Conn ignores ctx; only
//     the deadline can unblock it).
//   - Close does NOT abort an in-progress Recv (half-open socket: the FIN never
//     arrives, so the blocked read is not woken by Close).
//
// This faithfully reproduces RC#1: the single-shot watchdog sets a past deadline
// and returns ErrReadTimeout; the blocked Recv unblocks once with a timeout
// error; if readLoop then takes the noUpdates->continue path it issues a fresh
// Recv that clears the deadline and re-wedges permanently. Only a deterministic
// abort (return the error while the abort flag is set) breaks the wedge.
type halfOpenConn struct {
	mux       sync.Mutex
	deadline  time.Time // current read deadline (zero == none).
	wake      chan struct{}
	recvCount int
}

func newHalfOpenConn() *halfOpenConn {
	return &halfOpenConn{wake: make(chan struct{}, 1)}
}

func (c *halfOpenConn) Send(ctx context.Context, b *bin.Buffer) error { return nil }

func (c *halfOpenConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.mux.Lock()
	c.recvCount++
	// Faithful to transport/connection.go: reset the read deadline to zero at
	// the top of every Recv. Any past deadline the watchdog set is wiped here.
	c.deadline = time.Time{}
	c.mux.Unlock()

	for {
		c.mux.Lock()
		d := c.deadline
		c.mux.Unlock()
		if !d.IsZero() && !d.After(time.Now()) {
			// A past deadline was set on this already-blocked Recv: return the
			// exact error shape a real net.Conn produces, once.
			return &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
		}
		// No deadline: block. Does NOT observe ctx and is NOT woken by Close —
		// only a (future) SetReadDeadline wake can advance us.
		<-c.wake
	}
}

func (c *halfOpenConn) SetReadDeadline(t time.Time) error {
	c.mux.Lock()
	c.deadline = t
	c.mux.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

func (c *halfOpenConn) SetWriteDeadline(t time.Time) error { return nil }

// Close does not abort an in-progress Recv: a half-open socket never delivers
// the FIN, so the blocked read stays blocked.
func (c *halfOpenConn) Close() error { return nil }

func (c *halfOpenConn) recvCalls() int {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.recvCount
}

var _ transport.Conn = (*halfOpenConn)(nil)

// TestReadWatchdog_HalfOpenDeterministicAbort is the production-faithful
// regression for RC#1. With the faithful halfOpenConn (Recv resets its deadline
// at the top of each call, Close does not abort a blocked read), the single-shot
// watchdog's SetReadDeadline(past) only unblocks the Recv that is already in
// flight. If readLoop reacts to that timeout with noUpdates->continue, the next
// Recv clears the deadline and re-wedges forever — and Close cannot break it.
//
// Against the dcfb8ac58 watchdog this reproduces the residual wedge: conn.Run
// does not return within the bounded window (RED). After the deterministic-abort
// fix (aborting flag + return-on-error in readLoop) Run returns ErrReadTimeout
// every time (GREEN).
func TestReadWatchdog_HalfOpenDeterministicAbort(t *testing.T) {
	a := require.New(t)

	c := neo.NewTime(time.Now())
	ho := newHalfOpenConn()
	conn := newWatchdogConn(t, ho, c, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- conn.Run(ctx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	// In-flight RPC so hasPendingWork() is true. The send is black-holed, so the
	// request stays registered in the engine.
	invokeDone := make(chan struct{})
	go func() {
		defer close(invokeDone)
		_ = conn.Invoke(ctx, testPayload{Data: []byte{1}}, testPayload{})
	}()

	waitFor(t, func() bool { return ho.recvCalls() > 0 && conn.rpc.Pending() > 0 })
	<-conn.watchdogReady

	advancePast(c, 200*time.Millisecond, 5*time.Millisecond)

	select {
	case err := <-runErr:
		a.Error(err)
		a.ErrorIsf(err, ErrReadTimeout, "unexpected Run error: %v", err)
	case <-time.After(5 * time.Second):
		a.Fail("conn.Run wedged on half-open socket: watchdog did not deterministically abort")
	}

	cancel()
	select {
	case <-invokeDone:
	case <-time.After(5 * time.Second):
		a.Fail("in-flight Invoke did not return after Run exit")
	}
}

// TestConnRun_TeardownUnblocksBlockedRead reproduces the production wedge that
// the watchdog alone does NOT cover. On a half-open socket the ping keepalive
// detects death first (pong-miss at ~pingTimeout, BEFORE the watchdog's longer
// read timeout) and that pong-miss error cancels the run group. Teardown then
// funnels through handleClose, which must unblock readLoop's blocked Recv.
//
// Close alone does NOT wake a half-open read (halfOpenConn.Close is a no-op,
// matching a real socket whose FIN never arrives), so unless handleClose pushes
// a past read deadline, readLoop stays blocked in Recv forever, g.Wait() never
// returns, conn.Run never returns, and reconnectUntilClosed never redials — the
// exact 13-minute wedge seen in production. Here cancel() stands in for the
// group-cancelling pong-miss; the watchdog is disabled (readTimeout=0) so it
// cannot be what unblocks readLoop, isolating the teardown path.
//
// RED before the handleClose deadline-push fix (conn.Run wedges); GREEN after.
func TestConnRun_TeardownUnblocksBlockedRead(t *testing.T) {
	a := require.New(t)

	c := neo.NewTime(time.Now())
	ho := newHalfOpenConn()
	conn := newWatchdogConn(t, ho, c, true)
	// Disable the watchdog: this test must prove the teardown path (handleClose)
	// unblocks the read, not the watchdog.
	conn.readTimeout = 0

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- conn.Run(ctx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	// Wait until readLoop is blocked in Recv.
	waitFor(t, func() bool { return ho.recvCalls() > 0 })

	// Cancel the run group, exactly as a pong-miss (or any task error) would.
	// handleClose must unblock the blocked Recv so readLoop — and thus conn.Run —
	// returns.
	cancel()

	select {
	case <-runErr:
		// GREEN: conn.Run unwound after the group was cancelled.
	case <-time.After(5 * time.Second):
		a.Fail("conn.Run wedged: handleClose did not unblock the half-open read on group cancel")
	}
}

// waitFor polls cond up to ~2s, failing the test if it never holds. Used to
// synchronize on goroutine startup (not as a substitute for clock advancement).
func waitFor(tb testing.TB, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatal("condition not met within timeout")
}
