package mtproto

import (
	"context"
	"sync"

	"github.com/go-faster/errors"
	"github.com/gotd/log"
	"go.uber.org/multierr"

	"github.com/gotd/td/exchange"
)

// connect establishes connection using configured transport, creating
// new auth key if needed.
func (c *Conn) connect(ctx context.Context) (rErr error) {
	connectCtx := ctx
	if !c.pfs {
		// Backward-compatible non-PFS behavior: dial timeout limits the whole
		// connect phase (dial + key exchange).
		var cancel context.CancelFunc
		connectCtx, cancel = context.WithTimeout(ctx, c.dialTimeout)
		defer cancel()
	}

	dialCtx := connectCtx
	if c.pfs {
		// Quote (PFS): "The generation of temporary and permanent auth keys can be done in parallel."
		// Link: https://core.telegram.org/api/pfs
		//
		// In PFS mode key generation can take longer, so we only apply dial timeout
		// to socket establishment.
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, c.dialTimeout)
		defer cancel()
	}

	conn, err := c.dialer(dialCtx)
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	c.conn = conn

	// Set by the watcher below when it force-closes conn, and read by the
	// normalization defer. Declaration order here is load-bearing: defers run
	// LIFO, so this one is declared FIRST to run LAST, after both the wait for
	// the watcher and the close-on-error defer have had their say on rErr.
	// Reordering it does not silently misbehave -- the read would then race
	// the watcher's write and the race detector fails the build.
	var closedByWatcher bool
	defer func() {
		// Report a watcher-forced close as the context error it actually was.
		// Closing the socket surfaces in the parked read as a raw transport
		// failure ("use of closed network connection"), which would break
		// callers using errors.Is to tell cancellation from a genuine
		// transport fault. multierr.Append keeps the transport cause alongside
		// the context error instead of discarding it.
		//
		// Keyed on the watcher having fired, not on a context being done: a
		// genuine exchange failure that merely coincides with cancellation
		// must not be relabelled.
		if rErr != nil && closedByWatcher {
			rErr = errors.Wrap(multierr.Append(connectCtx.Err(), rErr), "connect")
		}
	}()

	// Both the close-on-error defer below and the watcher can decide to close
	// conn, and transport.Conn does not promise Close is safe to call twice.
	// The guard is scoped to this call rather than to the Conn: connect may
	// run more than once on the same Conn, and a Conn-scoped guard would turn
	// every close after the first into a no-op.
	var closeOnce sync.Once
	closeTransport := func() error {
		var err error
		closeOnce.Do(func() { err = conn.Close() })
		return err
	}

	defer func() {
		if rErr != nil {
			multierr.AppendInto(&rErr, closeTransport())
		}
	}()

	// The key exchange runs before Conn.Run starts its goroutine group, so
	// handleClose -- which is what normally closes the socket on cancellation
	// -- does not exist yet. transport.connection.Recv takes its deadline
	// solely from ctx.Deadline() and never watches ctx.Done(), so a parked
	// read can only be broken by closing the socket. Without this watcher,
	// cancelling during the exchange does nothing at all, and callers that
	// cancel and then wait (pool.DC.Close -> Supervisor.Wait) hang with it.
	//
	// Watches connectCtx rather than ctx because connectCtx is what drives
	// the exchange. In PFS mode the two are the same; in non-PFS mode
	// connectCtx additionally expires on dialTimeout, and a transport wrapper
	// that ignores context deadlines would otherwise stay parked past it.
	connectDone := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-connectCtx.Done():
			closedByWatcher = true
			if err := closeTransport(); err != nil {
				c.log.Debug(ctx, "Failed to close connection on cancel", log.Error(err))
			}
		case <-connectDone:
		}
	}()

	// Declared last so it runs first: the watcher is joined before anything
	// else inspects conn or rErr, which is what makes the two closers
	// sequential rather than concurrent and what publishes closedByWatcher to
	// the defers that follow. Waiting for watcherDone, rather than merely
	// signalling connectDone, is also what keeps the watcher from outliving
	// connect() as a stray goroutine.
	//
	// In non-PFS mode connectCtx has its own deferred cancel(), which fires on
	// every return path including success, at what another goroutine sees as
	// the same instant as connectDone. Two ready select cases are chosen at
	// random, so this wait additionally guarantees the watcher decides while
	// connectCtx.Done() can only be ready for a real reason.
	defer func() {
		close(connectDone)
		<-watcherDone
	}()

	if c.pfs {
		return c.connectPFS(ctx)
	}

	session := c.session()
	if session.Key.Zero() {
		c.log.Info(ctx, "Generating new auth key")
		start := c.clock.Now()
		if err := c.createAuthKey(connectCtx); err != nil {
			return errors.Wrap(err, "create auth key")
		}

		c.log.Info(ctx, "Auth key generated",
			log.Duration("duration", c.clock.Now().Sub(start)),
		)
		return nil
	}

	c.log.Info(ctx, "Key already exists")
	if session.ID == 0 {
		// NB: Telegram can return 404 error if session id is zero.
		//
		// See https://github.com/gotd/td/issues/107.
		c.log.Debug(ctx, "Generating new session id")
		if err := c.newSessionID(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Conn) connectPFS(ctx context.Context) error {
	if c.permKey.Zero() {
		c.log.Info(ctx, "Generating new permanent auth key")
		start := c.clock.Now()
		if err := c.createPermAuthKey(ctx); err != nil {
			return errors.Wrap(err, "create permanent auth key")
		}
		c.log.Info(ctx, "Permanent auth key generated",
			log.Duration("duration", c.clock.Now().Sub(start)),
		)
	} else {
		// Reuse persisted permanent key to keep existing authorization.
		c.log.Info(ctx, "Permanent key already exists")
	}

	c.log.Info(ctx, "Generating new temporary auth key")
	start := c.clock.Now()
	if err := c.createTempAuthKey(ctx); err != nil {
		return errors.Wrap(err, "create temporary auth key")
	}
	c.log.Info(ctx, "Temporary auth key generated",
		log.Duration("duration", c.clock.Now().Sub(start)),
	)

	return nil
}

func (c *Conn) runExchange(
	ctx context.Context,
	mode exchange.ExchangeMode,
	expiresIn int,
) (exchange.ClientExchangeResult, error) {
	ex := exchange.NewExchanger(c.conn, c.dcID).
		WithClock(c.clock).
		WithLogger(c.log.Named("exchange").Logger()).
		WithTimeout(c.exchangeTimeout).
		WithRand(c.rand)
	if mode == exchange.ExchangeModeTemporary {
		// Temporary mode maps to p_q_inner_data_temp_dc in exchange package.
		ex = ex.WithTempMode(expiresIn)
	}
	return ex.Client(c.rsaPublicKeys).Run(ctx)
}

func (c *Conn) logExchangeInit(ctx context.Context) {
	if !c.log.Enabled(ctx, log.LevelDebug) {
		return
	}
	// Useful for debugging i/o timeout errors on tcp reads or writes.
	attrs := []log.Attr{
		log.Duration("timeout", c.exchangeTimeout),
	}
	if deadline, ok := ctx.Deadline(); ok {
		attrs = append(attrs, log.Time("context_deadline", deadline))
	}
	c.log.Debug(ctx, "Initializing new key exchange", attrs...)
}

// createAuthKey generates new authorization key.
func (c *Conn) createAuthKey(ctx context.Context) error {
	// Grab exclusive lock for writing.
	// It prevents message sending during key regeneration if server forgot current auth key.
	c.exchangeLock.Lock()
	defer c.exchangeLock.Unlock()

	c.logExchangeInit(ctx)
	r, err := c.runExchange(ctx, exchange.ExchangeModePermanent, 0)
	if err != nil {
		return err
	}

	c.sessionMux.Lock()
	c.authKey = r.AuthKey
	c.sessionID = r.SessionID
	c.salt = r.ServerSalt
	c.sessionMux.Unlock()

	return nil
}

func (c *Conn) createPermAuthKey(ctx context.Context) error {
	c.exchangeLock.Lock()
	defer c.exchangeLock.Unlock()

	c.logExchangeInit(ctx)
	r, err := c.runExchange(ctx, exchange.ExchangeModePermanent, 0)
	if err != nil {
		return err
	}

	c.sessionMux.Lock()
	c.permKey = r.AuthKey
	// Creation timestamp is used by ENCRYPTED_MESSAGE_INVALID recovery policy.
	c.permKeyCreatedAt = c.clock.Now().Unix()
	c.sessionMux.Unlock()

	return nil
}

func (c *Conn) createTempAuthKey(ctx context.Context) error {
	c.exchangeLock.Lock()
	defer c.exchangeLock.Unlock()

	c.logExchangeInit(ctx)
	r, err := c.runExchange(ctx, exchange.ExchangeModeTemporary, c.tempKeyTTL)
	if err != nil {
		return err
	}

	expiresAt := r.ExpiresAt
	if expiresAt == 0 {
		// Defensive fallback if exchange result does not expose expiry.
		expiresAt = c.clock.Now().Unix() + int64(c.tempKeyTTL)
	}

	c.sessionMux.Lock()
	c.authKey = r.AuthKey
	c.sessionID = r.SessionID
	c.salt = r.ServerSalt
	c.tempKeyExpiry = expiresAt
	c.sessionMux.Unlock()

	return nil
}
