package mtproto

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
)

// https://core.telegram.org/mtproto/description#message-identifier-msg-id
// A message is rejected over 300 seconds after it is created or 30 seconds
// before it is created (this is needed to protect from replay attacks).
const (
	maxPast   = time.Second * 300
	maxFuture = time.Second * 30
)

// errRejected is returned on invalid message that should not be processed.
var errRejected = errors.New("message rejected")

// errStaleMessageID marks the subset of rejections that signal our inbound
// msg_id window has diverged from the server's: an already-processed
// (duplicate / too-low) msg_id or a msg_id created too far in the past. These
// are recreate-session triggers — continuing on a stale window can make a
// desync permanent. A "too far in future" id is excluded (local clock skew, not
// a session desync) as is a foreign session_id (potential replay of another
// session), so neither rotates.
//
// It also satisfies errors.Is(err, errRejected) so the message itself is still
// ignored rather than treated as a fatal read error.
var errStaleMessageID = staleMessageIDError{}

type staleMessageIDError struct{}

func (staleMessageIDError) Error() string { return "stale message id" }
func (staleMessageIDError) Is(target error) bool {
	return target == errRejected || target == errStaleMessageID
}

func checkMessageID(now time.Time, rawID int64) error {
	id := proto.MessageID(rawID)

	// Check that message is from server.
	switch id.Type() {
	case proto.MessageFromServer, proto.MessageServerResponse:
		// Valid.
	default:
		return errors.Wrapf(errRejected, "unexpected type %s", id.Type())
	}

	created := id.Time()
	if created.Before(now) && now.Sub(created) > maxPast {
		return errors.Wrap(errStaleMessageID, "created too far in past")
	}
	if created.Sub(now) > maxFuture {
		return errors.Wrap(errRejected, "created too far in future")
	}

	return nil
}

func (c *Conn) decryptMessage(b *bin.Buffer) (*crypto.EncryptedMessageData, error) {
	session := c.session()
	msg, err := c.cipher.DecryptFromBuffer(session.Key, b)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	// Validating message. This protects from replay attacks.
	if msg.SessionID != session.ID {
		return nil, errors.Wrapf(errRejected, "invalid session (got %d, expected %d)", msg.SessionID, session.ID)
	}
	if err := checkMessageID(c.serverNow(), msg.MessageID); err != nil {
		return nil, errors.Wrapf(err, "bad message id %d", msg.MessageID)
	}
	if !c.consumeMessageID(msg.MessageID) {
		return nil, errors.Wrapf(errStaleMessageID, "duplicate or too low message id %d", msg.MessageID)
	}

	return msg, nil
}

// consumeMessageID checks the inbound msg_id against the replay-protection
// buffer. The buffer is read under sessionMux because recreateSession may swap
// it atomically with the session_id / seqno rotation.
func (c *Conn) consumeMessageID(msgID int64) bool {
	c.sessionMux.RLock()
	buf := c.messageIDBuf
	c.sessionMux.RUnlock()
	return buf.Consume(msgID)
}

func (c *Conn) consumeMessage(ctx context.Context, buf *bin.Buffer) error {
	msg, err := c.decryptMessage(buf)
	if errors.Is(err, errStaleMessageID) {
		// Our inbound msg_id window diverged from the server's. Rotate the
		// session (new session_id, seqno reset, replay buffer reset) so the
		// next reconnect cannot inherit the stale window, then ignore this
		// message.
		c.log.Warn("Recreating session on stale inbound message id", zap.Error(err))
		if rErr := c.recreateSession(); rErr != nil {
			return errors.Wrap(rErr, "recreate session")
		}
		return nil
	}
	if errors.Is(err, errRejected) {
		c.log.Warn("Ignoring rejected message", zap.Error(err))
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "consume message")
	}

	if err := c.handleMessage(msg.MessageID, &bin.Buffer{Buf: msg.Data()}); err != nil {
		// Probably we can return here, but this will shutdown whole
		// connection which can be unexpected.
		//
		// A context.Canceled here is just teardown: the conn/handler ctx was
		// canceled while already-decoded messages were still draining. That is
		// not a failure, and on a busy account a single graceful stop can drain
		// hundreds of in-flight updates — logging each at warn floods the logs.
		if errors.Is(err, context.Canceled) {
			c.log.Debug("Stopped handling message: context canceled", zap.Error(err))
		} else {
			c.log.Warn("Error while handling message", zap.Error(err))
		}
		// Sending acknowledge even on error. Client should restore
		// from missing updates via explicit pts check and getDiff call.
	}

	needAck := (msg.SeqNo & 0x01) != 0
	if needAck {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.ackSendChan <- msg.MessageID:
		}
	}

	return nil
}

func (c *Conn) noUpdates(err error) bool {
	// Checking for read timeout.
	var syscall *net.OpError
	if errors.As(err, &syscall) && syscall.Timeout() {
		// We call SetReadDeadline so such error is expected.
		c.log.Debug("No updates")
		return true
	}
	return false
}

func (c *Conn) handleAuthKeyNotFound(ctx context.Context) error {
	if c.session().ID == 0 {
		// The 404 error can also be caused by zero session id.
		// See https://github.com/gotd/td/issues/107
		//
		// We should recover from this in createAuthKey, but in general
		// this code branch should be unreachable.
		c.log.Warn("BUG: zero session id found")
	}
	if c.pfs {
		// In PFS mode 404 most likely means lost temporary key, so caller should
		// recreate transport and re-bind, not regenerate permanent key in-place.
		return errors.Wrap(ErrPFSReconnectRequired, "temporary auth key not found in pfs mode")
	}
	c.log.Warn("Re-generating keys (server not found key that we provided)")
	if err := c.createAuthKey(ctx); err != nil {
		return errors.Wrap(err, "unable to create auth key")
	}
	c.log.Info("Re-created auth keys")
	// Request will be retried by ack loop.
	// Probably we can speed-up this.
	return nil
}

func (c *Conn) readLoop(ctx context.Context) (err error) {
	log := c.log.Named("read")
	log.Debug("Read loop started")
	defer func() {
		l := log
		if err != nil {
			l = log.With(zap.NamedError("reason", err))
		}
		l.Debug("Read loop done")
	}()

	var (
		// Last error encountered by consumeMessage.
		lastErr atomic.Value
		// To wait all spawned goroutines
		handlers sync.WaitGroup
	)
	// On a CLEAN exit wait for in-flight message handlers, but NEVER block the
	// connection's teardown on them once ctx is canceled. A handler may be parked
	// handing an already-received update to the CLIENT-LEVEL updates manager
	// (which outlives this connection); that manager's own consumer can in turn
	// be waiting on getDifference against THIS dying connection. Blocking the
	// connection's teardown on such a handler deadlocks the reconnect:
	// handlers.Wait -> conn.Run never returns -> reconnectUntilClosed never
	// redials -> getDifference never recovers on a new conn -> the handler never
	// drains. So on ctx cancel we stop waiting and let the handlers finish on
	// their own: the update is still processed once the manager drains on the new
	// connection (it is NOT dropped), and any per-conn rpc-result handler simply
	// hits the already force-closed engine (a no-op). The decode already happened
	// before the handler was spawned, so no dying-conn state is touched here.
	defer func() {
		done := make(chan struct{})
		go func() {
			handlers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
	}()

	for {
		// We've tried multiple ways to reduce allocations via reusing buffer,
		// but naive implementation induces high idle memory waste.
		//
		// Proper optimization will probably require total rework of bin.Buffer
		// with sharded (by payload size?) pool that can be used after message
		// size read (after readLen).
		//
		// Such optimization can introduce additional complexity overhead and
		// is probably not worth it.
		buf := &bin.Buffer{}

		// Halting if consumeMessage encountered error.
		// Should be something critical with crypto.
		if err, ok := lastErr.Load().(error); ok && err != nil {
			return errors.Wrap(err, "halting")
		}

		if err := c.conn.Recv(ctx, buf); err != nil {
			// The read watchdog OR a failed write (via abortDeadSocket) has decided
			// to tear down a dead/half-open socket and pushed a past read deadline;
			// this is the error from that. Propagate
			// it to unwind the run group deterministically. Without this, the
			// noUpdates->continue path below would issue a fresh Recv that resets
			// the read deadline (transport.connection.Recv clears it at the top of
			// every call) and re-wedges before the watchdog's returned error can
			// cancel ctx — only a single-shot abort would not break a half-open
			// socket where Close does not unblock the read.
			if c.watchdogAborting.Load() {
				// Surface ErrReadTimeout (wrapping the transport error for
				// context) so conn.Run's caller sees the same sentinel whether the
				// group reports the watchdog's return or this readLoop unwind
				// first — the two race and either may win.
				return errors.Wrap(ErrReadTimeout, "transport abort: "+err.Error())
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if c.noUpdates(err) {
					continue
				}
			}

			var protoErr *codec.ProtocolErr
			if errors.As(err, &protoErr) && protoErr.Code == codec.CodeAuthKeyNotFound {
				if err := c.handleAuthKeyNotFound(ctx); err != nil {
					return errors.Wrap(err, "auth key not found")
				}

				continue
			}

			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "read loop")
			default:
				return errors.Wrap(err, "read")
			}
		}

		// Refresh the read-silence deadline: any byte from the server (pong, ack,
		// message) proves the socket is alive and resets the watchdog.
		c.touchLastRecv()

		handlers.Add(1)
		go func() {
			defer handlers.Done()

			// Spawning goroutine per incoming message to utilize as much
			// resources as possible while keeping idle utilization low.
			//
			// The "worker" model was replaced by this due to idle utilization
			// overhead, especially on multi-CPU systems with multiple running
			// clients.
			if err := c.consumeMessage(ctx, buf); err != nil {
				// context.Canceled is teardown (the ack-send was aborted because
				// the conn ctx was canceled), not a processing failure — keep it
				// at debug so a graceful stop does not flood error logs.
				if errors.Is(err, context.Canceled) {
					log.Debug("Stopped processing message: context canceled", zap.Error(err))
				} else {
					log.Error("Failed to process message", zap.Error(err))
				}
				lastErr.Store(errors.Wrap(err, "consume"))
			}
		}()
	}
}
