package mtproto

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/gotd/neo"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/proto"
)

// fixedErrHandler returns a fixed error from OnMessage, modelling an update
// whose handoff to the client-level updates manager fails (e.g. with
// context.Canceled during teardown).
type fixedErrHandler struct{ err error }

func (h fixedErrHandler) OnMessage(*bin.Buffer) error { return h.err }
func (fixedErrHandler) OnSession(Session) error       { return nil }

var errBoom = errors.New("boom")

// TestRead_TeardownCancelIsQuiet is the regression for the teardown log flood:
// stopping a busy account gracefully drains hundreds of already-decoded updates
// whose handoff then fails with context.Canceled. Each used to log at
// warn/error ("Error while handling message" / "Failed to process message"),
// flooding the logs (~1.8k lines per stop). Teardown cancellation must be
// logged at debug; genuine errors must still be loud.
func TestRead_TeardownCancelIsQuiet(t *testing.T) {
	// read.go:126 — a handler returning context.Canceled is teardown, not a
	// failure: debug, not warn. The error is still swallowed (returns nil).
	t.Run("handler cancel logs debug not warn", func(t *testing.T) {
		a := require.New(t)
		conn := newWatchdogConn(t, newBlackholeConn(), neo.NewTime(time.Now()), true)
		core, logs := observer.New(zapcore.DebugLevel)
		conn.log = zap.New(core)
		conn.handler = fixedErrHandler{err: context.Canceled}

		var msg bin.Buffer
		// Even seqno -> no ack required -> consumeMessage swallows the handler error.
		a.NoError(conn.newEncryptedMessage(conn.messageID.New(proto.MessageServerResponse), 0, idPayload{}, &msg))
		a.NoError(conn.consumeMessage(context.Background(), &msg))

		a.Zero(logs.FilterLevelExact(zapcore.WarnLevel).Len(), "teardown cancel must not warn")
		a.Zero(logs.FilterLevelExact(zapcore.ErrorLevel).Len())
		a.Equal(1, logs.FilterMessageSnippet("Stopped handling message").Len(), "teardown cancel should be debug")
	})

	// A genuine (non-cancel) handler error must stay loud — only teardown is quieted.
	t.Run("real handler error still warns", func(t *testing.T) {
		a := require.New(t)
		conn := newWatchdogConn(t, newBlackholeConn(), neo.NewTime(time.Now()), true)
		core, logs := observer.New(zapcore.DebugLevel)
		conn.log = zap.New(core)
		conn.handler = fixedErrHandler{err: errBoom}

		var msg bin.Buffer
		a.NoError(conn.newEncryptedMessage(conn.messageID.New(proto.MessageServerResponse), 0, idPayload{}, &msg))
		a.NoError(conn.consumeMessage(context.Background(), &msg))

		a.Equal(1, logs.FilterMessageSnippet("Error while handling message").Len(), "real handler error must still warn")
		a.Equal(1, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	})

	// read.go:297 — consumeMessage returns context.Canceled when the ack-send is
	// aborted by ctx cancellation; readLoop must log that at debug, not error.
	t.Run("readloop ack cancel logs debug not error", func(t *testing.T) {
		a := require.New(t)
		osc := &oneShotConn{blackholeConn: newBlackholeConn(), once: make(chan []byte, 1)}
		conn := newWatchdogConn(t, osc, neo.NewTime(time.Now()), true)
		conn.readTimeout = 0 // disable the watchdog; isolate the teardown path
		core, logs := observer.New(zapcore.DebugLevel)
		conn.log = zap.New(core)
		conn.handler = nopHandler{} // OnMessage succeeds; the ack-send is what aborts
		conn.conn = osc             // readLoop reads c.conn directly (Run normally dials it)

		var msg bin.Buffer
		// Odd seqno -> ack required; a canceled ctx aborts the ack-send so
		// consumeMessage returns context.Canceled.
		a.NoError(conn.newEncryptedMessage(conn.messageID.New(proto.MessageServerResponse), 1, idPayload{}, &msg))
		osc.once <- append([]byte(nil), msg.Buf...)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // pre-cancel: the buffered frame is still delivered, then the ack-send aborts
		runErr := make(chan error, 1)
		go func() { runErr <- conn.readLoop(ctx) }()
		a.ErrorIs(<-runErr, context.Canceled)

		// The per-message goroutine logs after readLoop returns; wait for it.
		require.Eventually(t, func() bool {
			return logs.FilterMessageSnippet("Stopped processing").Len() == 1
		}, time.Second, 5*time.Millisecond)
		a.Zero(logs.FilterLevelExact(zapcore.ErrorLevel).Len(), "teardown cancel must not log at error")
	})
}
