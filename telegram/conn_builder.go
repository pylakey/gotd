package telegram

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/internal/manager"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"
)

// Per-connection-type connect timeouts matching the official Telegram Android
// client. gotd has only three ConnModes, so we map the generic/updates connection to the
// Android "generic" timeout (12s) and data/CDN (download/upload) connections to
// the Android "upload" timeout (25s). Push/proxy Android types have no gotd analog.
const (
	dialTimeoutGeneric = 12 * time.Second
	dialTimeoutData    = 25 * time.Second
)

// dialTimeoutForMode returns the Android-parity connect timeout for a connection
// mode. It is only used when the caller did not set an explicit DialTimeout.
func dialTimeoutForMode(mode manager.ConnMode) time.Duration {
	switch mode {
	case manager.ConnModeData, manager.ConnModeCDN:
		return dialTimeoutData
	default: // ConnModeUpdates — generic/primary connection.
		return dialTimeoutGeneric
	}
}

// applyConnDefaults wires the Android per-connection-type dial timeout into opts
// unless the caller set an explicit DialTimeout on the client options (which
// takes precedence). Shared by every connection-creation path (primary
// createConn and the data/CDN/media pools via newPoolConn) so the wiring cannot
// drift between them.
func (c *Client) applyConnDefaults(opts *mtproto.Options, mode manager.ConnMode) {
	if c.opts.DialTimeout == 0 {
		opts.DialTimeout = dialTimeoutForMode(mode)
	}
}

type clientHandler struct {
	client *Client
}

func (c clientHandler) OnSession(cfg tg.Config, s mtproto.Session) error {
	return c.client.onSession(cfg, s)
}

func (c clientHandler) OnMessage(b *bin.Buffer) error {
	return c.client.handleUpdates(b)
}

func (c *Client) asHandler() manager.Handler {
	return clientHandler{
		client: c,
	}
}

type cdnClientHandler struct {
	client *Client
}

func (c cdnClientHandler) OnSession(cfg tg.Config, s mtproto.Session) error {
	// CDN sessions are stored separately from regular DC sessions.
	return c.client.onCDNSession(cfg, s)
}

func (cdnClientHandler) OnMessage(*bin.Buffer) error {
	// CDN connections never deliver updates.
	return nil
}

func (c *Client) asCDNHandler() manager.Handler {
	return cdnClientHandler{
		client: c,
	}
}

type connConstructor func(
	create mtproto.Dialer,
	mode manager.ConnMode,
	appID int,
	opts mtproto.Options,
	connOpts manager.ConnOptions,
) pool.Conn

func defaultConstructor() connConstructor {
	return func(
		create mtproto.Dialer,
		mode manager.ConnMode,
		appID int,
		opts mtproto.Options,
		connOpts manager.ConnOptions,
	) pool.Conn {
		return manager.CreateConn(create, mode, appID, opts, connOpts)
	}
}

func (c *Client) dcList() dcs.List {
	cfg := c.cfg.Load()
	return dcs.List{
		Options: cfg.DCOptions,
		Domains: c.domains,
		Test:    c.testDC,
	}
}

func (c *Client) primaryDC(dc int) mtproto.Dialer {
	return func(ctx context.Context) (transport.Conn, error) {
		return c.resolver.Primary(ctx, dc, c.dcList())
	}
}

func (c *Client) createPrimaryConn(setup manager.SetupCallback) pool.Conn {
	return c.createConn(0, c.defaultMode, setup, c.onDead)
}

func (c *Client) createConn(
	id int64,
	mode manager.ConnMode,
	setup manager.SetupCallback,
	onDead func(error),
) pool.Conn {
	opts, s := c.session.Options(c.opts)
	opts.Logger = c.log.Named("conn").With(
		zap.Int64("conn_id", id),
		zap.Int("dc_id", s.DC),
	)
	c.applyConnDefaults(&opts, mode)

	return c.create(
		c.primaryDC(s.DC), mode, c.appID,
		opts, manager.ConnOptions{
			DC:        s.DC,
			Test:      c.testDC,
			Device:    c.device,
			Handler:   c.asHandler(),
			Setup:     setup,
			OnDead:    onDead,
			InitCache: c.initVersions,
		},
	)
}
