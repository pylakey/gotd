package telegram

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

// Reconnect backoff profile matching the official Telegram Android client.
//
// It uses a two-mode reconnect delay keyed on the disconnect reason:
//
//   - Socket-level network failure (errno ECONNRESET / EHOSTUNREACH and
//     related): start at 50ms and double up to a 400ms cap, giving the
//     sequence 50→100→200→400→400ms. The fast retry assumes a transient blip.
//   - Any other drop (clean server close, handshake failure, current/moving DC):
//     a fixed 1000ms delay.
//
// The doubling interval is reset to 50ms once a connection becomes useful;
// we map that to Reset, which the client invokes from onReady.
const (
	reconnectInitialInterval = 50 * time.Millisecond
	reconnectMaxInterval     = 400 * time.Millisecond
	reconnectCleanInterval   = 1000 * time.Millisecond
)

// reconnectBackoff implements backoff.BackOff with the Telegram Android
// two-mode reconnect profile.
//
// It is error-aware: reconnectUntilClosed records the error that caused the
// upcoming reconnect via setLastError before each NextBackOff call (RetryNotify
// invokes the operation, then NextBackOff, on the same goroutine, so the
// ordering is deterministic). NextBackOff classifies that error to pick the
// network-failure (doubling) or clean-drop (fixed 1s) profile.
type reconnectBackoff struct {
	mu      sync.Mutex
	cur     time.Duration // current network-failure interval; 0 means "reset"
	lastErr error
}

func newReconnectBackoff() *reconnectBackoff {
	return &reconnectBackoff{}
}

// setLastError records the error that triggered the upcoming reconnect so that
// the next NextBackOff can classify socket failure vs clean drop.
func (b *reconnectBackoff) setLastError(err error) {
	b.mu.Lock()
	b.lastErr = err
	b.mu.Unlock()
}

// NextBackOff returns the delay before the next reconnect attempt. It never
// returns backoff.Stop; the surrounding backoff.WithContext stops the loop on
// context cancellation.
func (b *reconnectBackoff) NextBackOff() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !isNetworkFailure(b.lastErr) {
		// Clean drop / handshake / DC move: fixed 1s.
		return reconnectCleanInterval
	}

	// Socket-level network failure: 50→100→200→400→400ms doubling.
	if b.cur <= 0 {
		b.cur = reconnectInitialInterval
	}
	d := b.cur
	b.cur = min(d*2, reconnectMaxInterval)
	return d
}

// Reset returns the backoff to its initial state, mirroring Android resetting
// lastReconnectTimeout to 50ms once useful data arrives.
func (b *reconnectBackoff) Reset() {
	b.mu.Lock()
	b.cur = 0
	b.lastErr = nil
	b.mu.Unlock()
}

// isNetworkFailure reports whether err is a socket-level network failure that
// Android handles with the fast doubling backoff (ECONNRESET / EHOSTUNREACH and
// related unreachable/refused/timeout errnos, plus net timeouts). Everything
// else (clean EOF, handshake or RPC errors) is treated as a clean drop.
func isNetworkFailure(err error) bool {
	if err == nil {
		return false
	}
	for _, errno := range [...]syscall.Errno{
		syscall.ECONNRESET,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		syscall.ENETDOWN,
		syscall.ECONNREFUSED,
		syscall.ETIMEDOUT,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}
