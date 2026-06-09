package mtproto

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
)

// Ping sends ping request to server and waits until pong is received or
// context is canceled.
func (c *Conn) Ping(ctx context.Context) error {
	// Generating random id.
	// Probably we should check for collisions here.
	pingID, err := crypto.RandInt64(c.rand)
	if err != nil {
		return err
	}

	pong := c.pong(pingID)
	defer c.removePong(pingID)

	if err := c.writeServiceMessage(ctx, &mt.PingRequest{PingID: pingID}); err != nil {
		return errors.Wrap(err, "write")
	}

	select {
	case <-pong:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) handlePong(b *bin.Buffer) error {
	var pong mt.Pong
	if err := pong.Decode(b); err != nil {
		return errors.Errorf("decode: %x", err)
	}
	c.log.Debug("Pong")

	c.pingMux.Lock()
	ch, ok := c.ping[pong.PingID]
	if ok {
		close(ch)
		delete(c.ping, pong.PingID)
	}
	c.pingMux.Unlock()

	return nil
}

func (c *Conn) pingDelayDisconnect(ctx context.Context, delay int) error {
	// Generating random id.
	// Probably we should check for collisions here.
	pingID, err := crypto.RandInt64(c.rand)
	if err != nil {
		return err
	}

	pong := c.pong(pingID)
	defer c.removePong(pingID)

	if err := c.writeServiceMessage(ctx, &mt.PingDelayDisconnectRequest{
		PingID:          pingID,
		DisconnectDelay: delay,
	}); err != nil {
		return errors.Wrap(err, "write")
	}

	select {
	case <-pong:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) pong(pingID int64) chan struct{} {
	ch := make(chan struct{})
	c.pingMux.Lock()
	c.ping[pingID] = ch
	c.pingMux.Unlock()
	return ch
}

func (c *Conn) removePong(pingID int64) {
	c.pingMux.Lock()
	delete(c.ping, pingID)
	c.pingMux.Unlock()
}

func (c *Conn) pingLoop(ctx context.Context) error {
	// disconnect_delay announced to the server. The official Telegram Android
	// client decouples this from the ping cadence (generic: ping every 19s,
	// disconnect_delay 35s). When pingDisconnect is unset we fall back to the
	// legacy coupling: e.g. ping every 60s -> disconnect_delay 75s.
	delay := c.pingDisconnect
	if delay <= 0 {
		delay = c.pingInterval + c.pingTimeout
	}

	ticker := c.clock.Ticker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "ping loop")
		case <-ticker.C():
			start := c.clock.Now()
			if err := func() error {
				ctx, cancel := context.WithTimeout(ctx, c.pingTimeout)
				defer cancel()

				return c.pingDelayDisconnect(ctx, int(delay.Seconds()))
			}(); err != nil {
				return errors.Wrap(err, "disconnect (pong missed)")
			}
			// One debug line per keepalive (cadence ~pingInterval) to observe
			// ping_delay_disconnect timing and round-trip during debugging.
			c.log.Debug("ping_delay_disconnect",
				zap.Duration("interval", c.pingInterval),
				zap.Int("disconnect_delay_s", int(delay.Seconds())),
				zap.Duration("rtt", c.clock.Now().Sub(start)),
			)
		}
	}
}
