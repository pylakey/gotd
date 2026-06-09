package telegram

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/rpc"
	"github.com/gotd/td/tg"
)

// regNopDecoder is a throwaway bin.Decoder for registry tests (the fake invoker
// never writes to it).
type regNopDecoder struct{}

func (regNopDecoder) Decode(*bin.Buffer) error { return nil }

// waitRegistered polls until exactly one live request is registered, returning
// it. Used to synchronize on the middleware having parked.
func waitRegistered(t *testing.T, r *requestRegistry) *liveRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := r.snapshot(); len(s) == 1 {
			return s[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("request not registered within timeout")
	return nil
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name  string
		input bin.Encoder
		want  resendPolicy
	}{
		{"updates.getDifference", &tg.UpdatesGetDifferenceRequest{}, resendYes},
		{"updates.getState", &tg.UpdatesGetStateRequest{}, resendYes},
		{"updates.getChannelDifference", &tg.UpdatesGetChannelDifferenceRequest{}, resendYes},
		{"users.getUsers", &tg.UsersGetUsersRequest{}, resendYes},
		{"help.getConfig", &tg.HelpGetConfigRequest{}, resendYes},
		{"sendMessage with random_id", &tg.MessagesSendMessageRequest{RandomID: 123}, resendYes},
		{"sendMessage zero random_id", &tg.MessagesSendMessageRequest{RandomID: 0}, resendNo},
		{"deleteMessages (no random_id, not allowlisted)", &tg.MessagesDeleteMessagesRequest{}, resendNo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classify(tt.input))
		})
	}
}

func TestIsTransportDrop(t *testing.T) {
	require.False(t, isTransportDrop(nil))
	require.True(t, isTransportDrop(rpc.ErrEngineClosed))
	require.True(t, isTransportDrop(pool.ErrConnDead))
	require.True(t, isTransportDrop(mtproto.ErrConnDead))
	require.True(t, isTransportDrop(errors.Wrap(rpc.ErrEngineClosed, "rpcDoRequest")))
	require.True(t, isTransportDrop(errors.Wrap(pool.ErrConnDead, "waitSession")))
	// Phase-1 write-death shape: wrapped through the engine + invoke layers.
	require.True(t, isTransportDrop(errors.Wrap(errors.Wrap(mtproto.ErrConnDead, "send"), "retryUntilAck")))
	// "engine forcibly closed" wraps context.Canceled; matched by message.
	require.True(t, isTransportDrop(errors.New("retryUntilAck: send: engine forcibly closed: context canceled")))
	// A genuine caller cancellation must NOT be a transport drop.
	require.False(t, isTransportDrop(context.Canceled))
	require.False(t, isTransportDrop(errors.New("FLOOD_WAIT (5)")))
}

// TestRequestRegistry_ParkThenReplay: a replayable request whose first invoke
// hits a transport drop parks, then returns the result once a (simulated) replay
// finishes it, and the entry is removed.
func TestRequestRegistry_ParkThenReplay(t *testing.T) {
	r := newRequestRegistry()
	c := &Client{ctx: context.Background()}
	c.requests = r

	next := InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		return rpc.ErrEngineClosed // simulate the conn dying under this request.
	})
	mw := r.middleware(c).Handle(next)

	res := make(chan error, 1)
	go func() {
		res <- mw.Invoke(context.Background(), &tg.UsersGetUsersRequest{}, regNopDecoder{})
	}()

	lr := waitRegistered(t, r)
	// Must be parked (not returned) — caller ctx is alive and the error is a drop.
	select {
	case <-res:
		t.Fatal("Invoke returned without parking on a transport drop")
	case <-time.After(50 * time.Millisecond):
	}

	lr.finishResult(nil) // simulate replayLiveRequests delivering success on the new conn.

	select {
	case err := <-res:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Invoke did not return after replay finish")
	}
	require.Empty(t, r.snapshot(), "entry must be removed after the owner returns")
}

// TestRequestRegistry_CallerCancelSurfaces: a caller cancellation while the
// request is in flight surfaces context.Canceled (not treated as a transport
// drop) and the entry is removed.
func TestRequestRegistry_CallerCancelSurfaces(t *testing.T) {
	r := newRequestRegistry()
	c := &Client{ctx: context.Background()}
	c.requests = r

	next := InvokeFunc(func(ctx context.Context, _ bin.Encoder, _ bin.Decoder) error {
		<-ctx.Done() // block until the caller cancels.
		return ctx.Err()
	})
	mw := r.middleware(c).Handle(next)

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		res <- mw.Invoke(ctx, &tg.UsersGetUsersRequest{}, regNopDecoder{})
	}()

	waitRegistered(t, r)
	cancel()

	select {
	case err := <-res:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Invoke did not return after caller cancel")
	}
	require.Empty(t, r.snapshot())
}

// TestRequestRegistry_BypassesNonReplayable: a non-replayable request (no
// random_id, not allowlisted) bypasses the registry entirely — its error is
// returned verbatim and nothing is registered.
func TestRequestRegistry_BypassesNonReplayable(t *testing.T) {
	r := newRequestRegistry()
	c := &Client{ctx: context.Background()}
	c.requests = r

	sentinel := errors.New("real rpc error")
	next := InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		return sentinel
	})
	mw := r.middleware(c).Handle(next)

	err := mw.Invoke(context.Background(), &tg.MessagesDeleteMessagesRequest{}, regNopDecoder{})
	require.ErrorIs(t, err, sentinel)
	require.Empty(t, r.snapshot(), "non-replayable request must not be registered")
}

// TestRequestRegistry_RealErrorSurfaces: a replayable request that gets a real
// (non-transport) error surfaces it immediately rather than parking.
func TestRequestRegistry_RealErrorSurfaces(t *testing.T) {
	r := newRequestRegistry()
	c := &Client{ctx: context.Background()}
	c.requests = r

	sentinel := errors.New("FLOOD_WAIT (30)")
	next := InvokeFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		return sentinel
	})
	mw := r.middleware(c).Handle(next)

	err := mw.Invoke(context.Background(), &tg.UsersGetUsersRequest{}, regNopDecoder{})
	require.ErrorIs(t, err, sentinel)
	require.Empty(t, r.snapshot())
}

// TestLiveRequest_FinishOnce: concurrent finishErr calls deliver exactly one
// result and close done exactly once (sync.Once contract).
func TestLiveRequest_FinishOnce(t *testing.T) {
	lr := &liveRequest{done: make(chan struct{})}

	var wg sync.WaitGroup
	for i := range 8 {
		err := errors.Errorf("err-%d", i)
		wg.Go(func() {
			lr.finishErr(err)
		})
	}
	wg.Wait()

	select {
	case <-lr.done:
	default:
		t.Fatal("done was not closed")
	}
	require.Error(t, lr.resultErr, "exactly one finish must have set a non-nil result")
}
