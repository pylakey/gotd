package telegram

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/pool"
	"github.com/gotd/td/rpc"
	"github.com/gotd/td/tg"
)

// ErrReplayExhausted is delivered to the caller when a store-and-resend request
// has been replayed maxReplay times across reconnects without completing.
var ErrReplayExhausted = errors.New("store-and-resend: replay attempts exhausted")

// maxReplay caps cross-reconnect replays of a single request, independent of the
// per-conn rpc retry (rpc.Options.MaxRetries, which retries the SAME msg_id
// within ONE conn). A request that keeps hitting fresh transport drops fails
// after this many reconnect-driven replays instead of parking forever — it is
// also bounded by the caller ctx deadline.
const maxReplay = 5

// resendPolicy classifies whether a request may be auto-replayed across a
// reconnect without risking duplicate side effects.
type resendPolicy uint8

const (
	// resendNo: never replay. The request bypasses the registry entirely and
	// behaves byte-for-byte like the pre-store-and-resend code (default-safe).
	resendNo resendPolicy = iota
	// resendYes: safe to replay verbatim — a read-only method (re-reading is
	// harmless) or a write carrying a random_id the server deduplicates.
	resendYes
)

// readOnlyAllowlist holds TypeIDs of methods whose double execution is harmless.
// HARDCODED: the generated tg schema carries no read-only metadata. Keep this
// tight; extend deliberately with vetted read-only (*.get*) methods only.
var readOnlyAllowlist = map[uint32]struct{}{
	tg.UpdatesGetDifferenceRequestTypeID:        {}, // the headline updates-pump case.
	tg.UpdatesGetStateRequestTypeID:             {},
	tg.UpdatesGetChannelDifferenceRequestTypeID: {},
	tg.UsersGetUsersRequestTypeID:               {},
	tg.HelpGetConfigRequestTypeID:               {},
}

// classify decides the replay policy for a request.
//
// The official Telegram client replays in-flight requests across a reconnect: a
// read re-executes harmlessly and a write is collapsed by the server on its
// random_id. We mirror that — allowlisted reads and non-zero-random_id writes are
// replayable; everything else is left to fail exactly as before.
func classify(input bin.Encoder) resendPolicy {
	if t, ok := input.(interface{ TypeID() uint32 }); ok {
		if _, ro := readOnlyAllowlist[t.TypeID()]; ro {
			return resendYes
		}
	}
	// Writes carrying a random_id are deduplicated by the server on resend (the
	// replay re-encodes the SAME input, hence the same random_id). A zero
	// random_id has nothing to dedup on, so it is not replayed.
	if rid, ok := input.(interface{ GetRandomID() int64 }); ok && rid.GetRandomID() != 0 {
		return resendYes
	}
	return resendNo
}

// isTransportDrop reports whether err is a transient connection death the
// registry should wait-and-replay through, as opposed to a real RPC error,
// caller cancellation, or a permanent (auth) failure that must surface.
//
// It matches exactly the shapes a conn teardown produces:
//   - rpc.ErrEngineClosed: a new Do after the engine was force-closed.
//   - pool.ErrConnDead: a request that arrived after the conn already died.
//   - mtproto.ErrConnDead: the request whose own write hit the dead socket
//     (Phase 1 write-death) — surfaced synchronously, before the force-close.
//   - "engine forcibly closed": an in-flight Do whose reqCtx was canceled by
//     ForceClose. That wraps context.Canceled, so it is matched by MESSAGE — not
//     errors.Is(context.Canceled), which would also swallow a genuine caller
//     cancellation.
func isTransportDrop(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, rpc.ErrEngineClosed) || errors.Is(err, pool.ErrConnDead) || errors.Is(err, mtproto.ErrConnDead) {
		return true
	}
	return strings.Contains(err.Error(), "engine forcibly closed")
}

// captureDecoder copies the raw result bytes instead of decoding them, so each
// invoke attempt (the original on the dying conn + every replay on a new conn)
// gets its OWN result buffer and none of them ever touch the caller's output
// decoder. This is essential because a result can still arrive on the dying conn
// (its message handler may outlive teardown) and would otherwise race the replay
// — and the caller's read — on a shared output. The captured bytes of the
// SUCCESSFUL attempt are decoded into the caller's output exactly once, by the
// registry, under finishOne.
type captureDecoder struct {
	buf []byte
}

func (d *captureDecoder) Decode(b *bin.Buffer) error {
	d.buf = append(d.buf[:0], b.Buf...)
	return nil
}

// liveRequest is one caller Invoke tracked across reconnects.
type liveRequest struct {
	id        uint64
	input     bin.Encoder     // re-encoded fresh on each (re)send; carries the same random_id.
	output    bin.Decoder     // caller's decoder; written exactly once, on success, by finishResult.
	callerCtx context.Context // caller ctx; cancellation here aborts for real.

	done      chan struct{} // closed once when terminal; resultErr is then valid.
	finishOne sync.Once     // guards done/resultErr/output against owner vs replay race.
	resultErr error

	attempts  int         // cross-reconnect replay counter (guarded by registry mux).
	parked    atomic.Bool // true once the owner is parked on a transport drop (replay-eligible).
	replaying atomic.Bool // true while a replay goroutine is in flight (no overlap).
}

// finishResult decodes the captured raw result of the first successful attempt
// into the caller's output and wakes the parked caller, exactly once.
func (lr *liveRequest) finishResult(raw []byte) {
	lr.finishOne.Do(func() {
		lr.resultErr = lr.output.Decode(&bin.Buffer{Buf: raw})
		close(lr.done)
	})
}

// finishErr delivers a terminal error to the parked caller, exactly once.
func (lr *liveRequest) finishErr(err error) {
	lr.finishOne.Do(func() {
		lr.resultErr = err
		close(lr.done)
	})
}

// requestRegistry is the durable, above-conn store of in-flight replayable
// requests (the Go analog of the official client's runningRequests). Owned by
// telegram.Client so entries survive the per-reconnect conn swap. Guarded by its
// own mutex, NOT connMux.
type requestRegistry struct {
	mux  sync.Mutex
	seq  uint64
	live map[uint64]*liveRequest
}

func newRequestRegistry() *requestRegistry {
	return &requestRegistry{live: map[uint64]*liveRequest{}}
}

func (r *requestRegistry) add(ctx context.Context, input bin.Encoder, output bin.Decoder) *liveRequest {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.seq++
	lr := &liveRequest{
		id:        r.seq,
		input:     input,
		output:    output,
		callerCtx: ctx,
		done:      make(chan struct{}),
	}
	r.live[lr.id] = lr
	return lr
}

func (r *requestRegistry) remove(id uint64) {
	r.mux.Lock()
	delete(r.live, id)
	r.mux.Unlock()
}

func (r *requestRegistry) snapshot() []*liveRequest {
	r.mux.Lock()
	defer r.mux.Unlock()
	out := make([]*liveRequest, 0, len(r.live))
	for _, lr := range r.live {
		out = append(out, lr)
	}
	return out
}

// bumpAttempts increments and returns the replay attempt count under the mux.
func (r *requestRegistry) bumpAttempts(lr *liveRequest) int {
	r.mux.Lock()
	defer r.mux.Unlock()
	lr.attempts++
	return lr.attempts
}

// middleware wraps the invoker so a replayable request that hits a transient
// transport drop is parked until the replay path completes it on the new conn,
// instead of failing to the caller. It is installed INNERMOST (it wraps
// invokeDirect), so user middlewares above it never see the transient drop —
// only the eventual success or real failure.
func (r *requestRegistry) middleware(c *Client) Middleware {
	return MiddlewareFunc(func(next tg.Invoker) InvokeFunc {
		return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
			if classify(input) == resendNo {
				return next.Invoke(ctx, input, output) // not replayable: current behavior.
			}

			lr := r.add(ctx, input, output)
			defer r.remove(lr.id)

			// Invoke into a per-attempt capture buffer, never the caller's output:
			// a late result on the dying conn must not race the replay or the
			// caller. On success the captured bytes are decoded into the caller's
			// output once (finishResult).
			capt := &captureDecoder{}
			err := next.Invoke(ctx, input, capt)
			switch {
			case err == nil:
				lr.finishResult(capt.buf)
				return lr.resultErr
			case ctx.Err() != nil:
				// The caller's own ctx was canceled/expired — a real abort, not a
				// transport death. Checked BEFORE isTransportDrop because a
				// ForceClose drop wraps context.Canceled too.
				return ctx.Err()
			case isTransportDrop(err):
				// The conn died with this request in flight. Mark it parked so the
				// replay path will re-issue it (and only it — an in-flight request,
				// e.g. one mid-DC-migration, must NOT be speculatively replayed).
				// Then park until the replay path (replayLiveRequests, driven by the
				// new conn's onSession edge) delivers a result on the fresh conn, the
				// caller gives up, or the client shuts down. The replay goroutine is
				// the sole re-issuer — we never re-invoke here against the dead conn.
				lr.parked.Store(true)
				select {
				case <-lr.done:
					return lr.resultErr
				case <-ctx.Done():
					// Caller gave up. Claim finishOne BEFORE returning so an
					// in-flight replay goroutine's finishResult becomes a no-op and
					// can never decode into the caller's output after this Invoke has
					// returned and the entry was removed (write-after-return race).
					lr.finishErr(ctx.Err())
					return lr.resultErr
				case <-c.ctx.Done():
					lr.finishErr(c.ctx.Err())
					return lr.resultErr
				}
			default:
				return err // real RPC error / permanent failure: surface.
			}
		}
	})
}

// replayLiveRequests re-issues every parked replayable request on the freshly
// reconnected conn. Called from onSession (with the new c.conn installed). It
// only acts on requests that are (a) still wanted by their caller, (b) actually
// parked on a transport drop (not merely in flight, e.g. mid-migration), and
// (c) not already finished. At most one replay goroutine per request runs at a
// time (the replaying guard), so a flapping reconnect cannot double-issue a
// request or race the caller's output decoder. If a concurrent onSession edge is
// skipped because a replay is already in flight, it is benign: the in-flight
// replay either completes the request or, on a fresh drop, leaves it parked for
// the next onSession edge (and the caller ctx bounds the wait).
func (c *Client) replayLiveRequests() {
	for _, lr := range c.requests.snapshot() {
		if lr.callerCtx.Err() != nil {
			continue // caller already gone; the owning middleware will remove it.
		}
		if !lr.parked.Load() {
			continue // still in flight on its original path (e.g. mid-migration); not dropped.
		}
		select {
		case <-lr.done:
			continue // already finished; awaiting removal by its owner.
		default:
		}
		if !lr.replaying.CompareAndSwap(false, true) {
			continue // a replay for this request is already running.
		}

		go func(lr *liveRequest) {
			defer lr.replaying.Store(false)
			// invokeDirect re-snapshots c.conn (via invokeConn), so this targets the
			// NEW conn and keeps datacenter-migration handling. A fresh msg_id is
			// minted inside mtproto.Conn.Invoke; the same input (hence the same
			// random_id) is re-encoded by the conn write path. The registry
			// middleware is NOT in this path, so there is no re-registration. The
			// per-attempt capture buffer isolates this replay's result from any late
			// result on the previous (dying) conn and from the caller's output.
			capt := &captureDecoder{}
			err := c.invokeDirect(lr.callerCtx, lr.input, capt)
			switch {
			case err == nil:
				lr.finishResult(capt.buf)
			case errors.Is(err, pool.ErrConnDead):
				// The conn was already dead before we sent anything (e.g. this replay
				// was driven by the dying conn's shutdown-carry onSession, before
				// reconnectUntilClosed swapped c.conn to the fresh conn). It never
				// reached the server, so it does NOT consume a replay attempt — the
				// next ready-conn onSession edge replays it for real.
				return
			case isTransportDrop(err):
				// Sent, then the conn dropped under us. Count this real replay round;
				// give up (fail the caller) once maxReplay is exhausted, otherwise
				// leave it parked for the next onSession edge.
				if c.requests.bumpAttempts(lr) > maxReplay {
					lr.finishErr(errors.Wrap(ErrReplayExhausted, "store-and-resend"))
				}
				return
			default:
				lr.finishErr(err) // real RPC error / permanent: deliver to the caller.
			}
		}(lr)
	}
}
