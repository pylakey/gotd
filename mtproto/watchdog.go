package mtproto

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

// ErrReadTimeout is returned by the read watchdog when the connection has gone
// silent (no bytes received within ReadTimeout) while it still has pending
// work. It unwinds conn.Run so the connection is recreated.
var ErrReadTimeout = errors.New("read timeout: connection silent with pending work")

// touchLastRecv refreshes the read-silence deadline to the current clock time.
// Called on connect and on every successful Recv.
func (c *Conn) touchLastRecv() {
	c.lastRecvUnixNano.Store(c.clock.Now().UnixNano())
}

// hasPendingWork reports whether the connection has outstanding work that a
// silent socket would stall. This mirrors the official Telegram client gate
// (reconnect only when not yet connected or there are pending requests): a
// truly idle, ready connection is left alone.
func (c *Conn) hasPendingWork() bool {
	// Not yet ready: session has not been signaled, so the connection is still
	// completing setup and must not be left to stall silently.
	select {
	case <-c.gotSession.Ready():
	default:
		return true
	}
	// In-flight RPC requests awaiting a response.
	return c.rpc.Pending() > 0
}

// readWatchdog detects a silent / half-open / blackholed socket. On a ticker it
// checks how long it has been since the last byte from the server. If silence
// exceeds readTimeout AND there is pending work, it aborts the blocked read so
// readLoop errors out and conn.Run returns (the reconnect loop then redials in
// place). If there is no pending work it slides the deadline forward and keeps
// the connection, so a healthy idle connection is never churned.
//
// This is an additional backstop to the ping keepalive (pong-miss), not a
// replacement: it covers the case where the ping write succeeds into a dead TCP
// buffer but no bytes ever return.
func (c *Conn) readWatchdog(ctx context.Context) error {
	if c.readTimeout <= 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	tick := c.watchdogTick
	if tick <= 0 {
		tick = time.Second
	}

	ticker := c.clock.Ticker(tick)
	defer ticker.Stop()
	if c.watchdogReady != nil {
		close(c.watchdogReady)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C():
			// Use the tick's own timestamp as "now"; it is the clock time the
			// tick fired at and avoids re-reading the clock here.
			last := time.Unix(0, c.lastRecvUnixNano.Load())
			if now.Sub(last) <= c.readTimeout {
				// Not silent: a recent Recv (or connect) refreshed the deadline.
				continue
			}
			if !c.hasPendingWork() {
				// Silent but idle: slide the deadline and keep the connection.
				c.lastRecvUnixNano.Store(now.UnixNano())
				continue
			}
			// Silent with pending work: the socket is dead. Abort
			// deterministically.
			//
			// 1. Set the aborting flag FIRST. readLoop (and any write path)
			//    checks it on a transport error and unwinds instead of retrying,
			//    so the abort cannot race a fresh Recv that would reset the
			//    deadline (transport.connection.Recv clears it at the top of every
			//    call) and re-wedge a half-open socket forever.
			// 2. Push past read AND write deadlines: a past read deadline trips a
			//    blocked Recv where Close may not on a half-open socket; a past
			//    write deadline trips a Send blocked on a full kernel buffer.
			// 3. Return ErrReadTimeout to unwind the run group.
			c.log.Warn("read watchdog: silence with pending work, forcing reconnect",
				zap.Duration("read_timeout", c.readTimeout),
			)
			c.watchdogAborting.Store(true)
			past := now.Add(-time.Second)
			if err := c.conn.SetReadDeadline(past); err != nil {
				c.log.Debug("read watchdog: set read deadline failed", zap.Error(err))
			}
			if err := c.conn.SetWriteDeadline(past); err != nil {
				c.log.Debug("read watchdog: set write deadline failed", zap.Error(err))
			}
			return errors.Wrap(ErrReadTimeout, "read watchdog")
		}
	}
}
