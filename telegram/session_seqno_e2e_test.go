package telegram_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/gotd/td/session"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgtest"
	"github.com/gotd/td/tgtest/cluster"
	"github.com/gotd/td/transport"
)

// TestSessionSeqNoContinuity_AcrossReconnect verifies R2 seqno continuity for
// real: a plain reconnect to the same DC (forced by dropping the server-side
// connection but keeping the auth key) must continue the content seqno, never
// regress it. The stored pool.Session.SeqNo must track the LIVE seqno of the
// connection, not a stale snapshot taken once at new_session_created time.
//
// Before the fix, the carried SeqNo is frozen at new_session_created and the
// reconnect resumes content seqno BELOW what the server already saw on that
// session_id, which a real Telegram server rejects with bad_msg code 32. tgtest
// does not validate seqno by default, so we install the opt-in SetOnSeqNo
// observer to record the server's last-seen content seqno per session_id and
// assert no regression.
func TestSessionSeqNoContinuity_AcrossReconnect(t *testing.T) {
	t.Parallel()

	log := zaptest.NewLogger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	g := tdsync.NewCancellableGroup(ctx)

	c := cluster.NewCluster(cluster.Options{
		Logger:   log.Named("cluster"),
		Protocol: transport.Intermediate,
	})
	c.Common().Vector(tg.UsersGetUsersRequestTypeID, user)

	type seen struct {
		maxMsgID    int64
		maxMsgSeqNo int32 // content seqno carried by maxMsgID
		maxSeqNo    int32 // highest content seqno seen
	}
	var (
		mu sync.Mutex
		// perSession tracks, per session_id, the highest content msg_id seen and
		// its seqno, plus the highest seqno seen.
		perSession = map[int64]*seen{}
		// regressions records genuine seqno regressions: a strictly-later content
		// message (higher msg_id) whose seqno did not advance past the previous
		// highest msg_id's seqno. Retransmits (msg_id not newer) are tolerated, as
		// real Telegram does, so the test is not fooled by redelivered in-flight
		// messages around the reconnect.
		regressions []string
		firstSess   tgtest.Session
		haveSess    bool
	)

	var srv *tgtest.Server
	srv, _ = c.DC(2, "server")
	srv.SetOnSeqNo(func(sessionID, msgID int64, seqNo int32) {
		// Only content messages carry an odd seqno; service messages are even.
		if seqNo%2 == 0 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		s, ok := perSession[sessionID]
		if !ok {
			perSession[sessionID] = &seen{maxMsgID: msgID, maxMsgSeqNo: seqNo, maxSeqNo: seqNo}
			return
		}
		if msgID > s.maxMsgID {
			// A strictly-later content message. Its seqno must advance past the
			// previous latest message's seqno; if not, the session's seqno window
			// regressed (the desync bad_msg code 32 guards against).
			if seqNo <= s.maxMsgSeqNo {
				regressions = append(regressions,
					formatRegression(sessionID, s.maxMsgSeqNo, seqNo))
			}
			s.maxMsgID = msgID
			s.maxMsgSeqNo = seqNo
		}
		if seqNo > s.maxSeqNo {
			s.maxSeqNo = seqNo
		}
	})

	c.Dispatch(2, "server").HandleFunc(tg.UsersGetUsersRequestTypeID,
		func(server *tgtest.Server, req *tgtest.Request) error {
			mu.Lock()
			if !haveSess {
				firstSess = req.Session
				haveSess = true
			}
			mu.Unlock()
			return server.SendVector(req, user)
		},
	)

	g.Go(c.Up)
	g.Go(func(ctx context.Context) error {
		select {
		case <-c.Ready():
		case <-ctx.Done():
			return ctx.Err()
		}

		opts := telegram.Options{
			PublicKeys:     c.Keys(),
			Resolver:       c.Resolver(),
			Logger:         log.Named("client"),
			SessionStorage: &session.StorageMemory{},
			DCList:         c.List(),
			AckBatchSize:   1,
			AckInterval:    50 * time.Millisecond,
			RetryInterval:  50 * time.Millisecond,
			UpdateHandler: telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
				return nil
			}),
		}
		client := telegram.NewClient(1, "hash", opts)

		return client.Run(ctx, func(ctx context.Context) error {
			raw := tg.NewClient(client)

			// Several content RPCs so the live content seqno climbs well above 1.
			for range 5 {
				if _, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
					return errors.Wrap(err, "pre-reconnect rpc")
				}
			}

			mu.Lock()
			sess := firstSess
			var preMax int32
			if s := perSession[sess.ID]; s != nil {
				preMax = s.maxSeqNo
			}
			mu.Unlock()
			require.Greater(t, preMax, int32(1), "live content seqno must climb before reconnect")

			// Force a plain reconnect WITHOUT losing the auth key.
			srv.ForceDisconnect(sess)

			// One post-reconnect content RPC. Retry until the client reconnected.
			// The observer records the seqno the server sees for the resumed
			// session_id; the bug surfaces as a regression at or below preMax.
			require.Eventually(t, func() bool {
				_, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
				return err == nil
			}, 60*time.Second, 100*time.Millisecond, "post-reconnect rpc must succeed")

			mu.Lock()
			regs := append([]string(nil), regressions...)
			mu.Unlock()

			require.Emptyf(t, regs,
				"content seqno must not regress across a plain reconnect (same session_id): %v", regs)

			cancel()
			return nil
		})
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err)
	}
}

func formatRegression(sessionID int64, prev, got int32) string {
	return errors.Errorf("session %#x: seqno %d <= previous max %d", sessionID, got, prev).Error()
}
