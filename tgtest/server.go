package tgtest

import (
	"context"
	"crypto/rsa"
	"io"
	"net"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"nhooyr.io/websocket"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/tmap"
	"github.com/gotd/td/transport"
)

// Server is a MTProto server structure.
type Server struct {
	// DC ID of this server.
	dcID int
	// Key pair of this server.
	key exchange.PrivateKey // immutable

	// Codec constructor. May be nil.
	codec func() transport.Codec // immutable,nilable
	// Server-side message cipher.
	cipher crypto.Cipher // immutable
	// Clock to use in key exchange and message ID generation.
	clock clock.Clock // immutable
	// MessageID generator
	msgID mtproto.MessageIDSource // immutable

	readTimeout  time.Duration
	writeTimeout time.Duration

	// RPC handler.
	handler Handler // immutable

	// users stores session info.
	users *users

	// type map for logging.
	types *tmap.Map   // immutable
	log   *zap.Logger // immutable

	// onPing, if set, is called for every ping_delay_disconnect with the
	// announced disconnect_delay. Test-only observability hook; nil by default.
	onPing func(req *Request, disconnectDelay int) // nilable

	// onRequest, if set, is called for every request right before it reaches the
	// RPC handler, with req.Buf still pointing at the raw (not yet unwrapped)
	// request. Test-only observability hook; nil by default.
	onRequest func(req *Request) // nilable

	// onSeqNo, if set, is called for every decrypted client message with its
	// session_id, msg_id and content seqno (as carried in the encrypted message
	// envelope, before container unpacking). Test-only observability hook; nil by
	// default, so existing callers see zero behavior change. Intended for tests
	// that assert seqno monotonicity / continuity across reconnects.
	onSeqNo func(sessionID, msgID int64, seqNo int32) // nilable
}

// SetOnPing installs an observer invoked for every incoming
// ping_delay_disconnect with the disconnect_delay announced by the client.
// It must be set before the server starts serving. Intended for tests that
// assert ping cadence / disconnect_delay parity.
func (s *Server) SetOnPing(fn func(req *Request, disconnectDelay int)) {
	s.onPing = fn
}

// SetOnRequest installs an observer invoked for every request just before it is
// passed to the RPC handler. req.Buf still holds the raw request bytes (e.g.
// invokeWithLayer(initConnection(...)) before UnpackInvoke unwraps them), so the
// observer can peek a copy to count wrapper requests. It must be set before the
// server starts serving. Intended for tests that assert initConnection parity.
func (s *Server) SetOnRequest(fn func(req *Request)) {
	s.onRequest = fn
}

// SetOnSeqNo installs an observer invoked for every decrypted client message
// with its session_id, msg_id and content seqno (from the encrypted message
// envelope, before any container unpacking). It must be set before the server
// starts serving. Intended for tests that assert seqno continuity across
// reconnects; nil by default, so it is a no-op for existing callers and the
// server itself performs no seqno validation.
func (s *Server) SetOnSeqNo(fn func(sessionID, msgID int64, seqNo int32)) {
	s.onSeqNo = fn
}

// NewPrivateKey creates new private key from RSA private key.
func NewPrivateKey(k *rsa.PrivateKey) exchange.PrivateKey {
	return exchange.PrivateKey{
		RSA: k,
	}
}

// NewServer creates new Server.
func NewServer(key exchange.PrivateKey, handler Handler, opts ServerOptions) *Server {
	opts.setDefaults()

	s := &Server{
		dcID:         opts.DC,
		key:          key,
		codec:        opts.Codec,
		cipher:       crypto.NewServerCipher(opts.Random),
		clock:        opts.Clock,
		msgID:        opts.MessageID,
		readTimeout:  opts.ReadTimeout,
		writeTimeout: opts.WriteTimeout,
		handler:      handler,
		users:        newUsers(),
		types:        opts.Types,
		log:          opts.Logger,
	}
	return s
}

// Key returns public key of this server.
func (s *Server) Key() exchange.PublicKey {
	return s.key.Public()
}

// Serve runs server loop using given listener.
func (s *Server) Serve(ctx context.Context, l transport.Listener) error {
	return s.serve(ctx, l)
}

func (s *Server) serve(ctx context.Context, l transport.Listener) error {
	s.log.Info("Serving")
	defer func() {
		s.log.Info("Stopping")
	}()

	grp := tdsync.NewCancellableGroup(ctx)
	grp.Go(func(context.Context) error {
		for {
			conn, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				return errors.Wrap(err, "accept")
			}

			grp.Go(func(ctx context.Context) error {
				if err := s.serveConn(ctx, conn); err != nil {
					// Client disconnected.
					var syscallErr *net.OpError
					switch {
					case errors.Is(err, io.EOF):
						return nil
					case errors.As(err, &syscallErr) &&
						(syscallErr.Op == "write" || syscallErr.Op == "read"):
						return nil
					}
					// TODO(tdakkota): emulate errors too?
					if code := websocket.CloseStatus(err); code >= 0 {
						return nil
					}

					s.log.Info("Serving handler error", zap.Error(err))
				}
				return nil
			})
		}
	})
	grp.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return l.Close()
	})
	return grp.Wait()
}
