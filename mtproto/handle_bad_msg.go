package mtproto

import (
	"fmt"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
)

type badMessageError struct {
	Code     int
	NewSalt  int64
	BadMsgID int64
}

const (
	codeMessageIDTooLow     = 16
	codeMessageIDTooHigh    = 17
	codeIncorrectServerSalt = 48
)

// isTimeOffsetCode reports whether a bad_msg_notification code signals that our
// msg_id was outside the server's accept window because our clock estimate is
// off (too low / too high). These are corrected by re-learning the server-time
// offset from the rejected msg_id's time and retrying — not by rotating the
// session. This mirrors the official client adjusting timeDifference on these
// codes rather than starting a new session.
func isTimeOffsetCode(code int) bool {
	return code == codeMessageIDTooLow || code == codeMessageIDTooHigh
}

// recreateSessionCodes are the bad_msg_notification error codes that indicate
// the server's view of our session (seqno or container framing) has
// irrecoverably diverged from ours. The only safe recovery is to drop the
// current session_id and start a fresh one, resetting seqno and the replay
// buffer in lockstep. Codes 16/17 are deliberately excluded: they are clock
// (msg_id time) errors corrected by offset re-learning + retry. Code 48
// (bad_server_salt) is also excluded: it is a salt-only correction.
var recreateSessionCodes = map[int]struct{}{
	19: {}, // container msg_id equals a previously received msg_id
	32: {}, // msg_seqno too low
	33: {}, // msg_seqno too high
	64: {}, // invalid container
}

func isRecreateSessionCode(code int) bool {
	_, ok := recreateSessionCodes[code]
	return ok
}

func (c badMessageError) Error() string {
	description := map[int]string{
		codeMessageIDTooLow:     "msg_id too low",
		codeMessageIDTooHigh:    "msg_id too high",
		codeIncorrectServerSalt: "incorrect server salt",

		18: "incorrect two lower order msg_id bits",
		19: "container msg_id is the same as msg_id of a previously received message",
		20: "message too old",
		32: "msg_seqno too low",
		33: "msg_seqno too high",
		34: "even msg_seqno expected, but odd received",
		35: "odd msg_seqno expected, but even received",
	}[c.Code]
	if description == "" {
		return fmt.Sprintf("bad msg error code %d", c.Code)
	}
	return description
}

// handleBadMsg processes bad_msg_notification / bad_server_salt. serverMsgID is
// the envelope msg_id of the carrying message: it is server-stamped, so its
// time is the authoritative server clock used to re-learn the server-time
// offset on the clock-error codes (16/17). The bad_msg_id field, by contrast,
// is our own rejected msg_id and carries no server time.
func (c *Conn) handleBadMsg(serverMsgID int64, b *bin.Buffer) error {
	id, err := b.PeekID()
	if err != nil {
		return err
	}
	switch id {
	case mt.BadMsgNotificationTypeID:
		var bad mt.BadMsgNotification
		if err := bad.Decode(b); err != nil {
			return err
		}

		if isTimeOffsetCode(bad.ErrorCode) {
			// msg_id was outside the server's accept window because our clock
			// estimate drifted. Re-learn the server-time offset from the carrying
			// message's (server-stamped) time so the retried request — and every
			// subsequent msg_id — lands inside the window.
			if err := c.correctTimeOffset(bad.ErrorCode, serverMsgID); err != nil {
				return errors.Wrap(err, "correct time offset")
			}
		} else if isRecreateSessionCode(bad.ErrorCode) {
			// Our session_id/seqno has diverged from the server. Rotate the
			// session (new session_id, seqno reset, replay buffer reset) before
			// surfacing the error so any retry uses the fresh session.
			c.log.Warn("Recreating session on bad_msg_notification",
				zap.Int("error_code", bad.ErrorCode),
				zap.Int64("bad_msg_id", bad.BadMsgID),
			)
			if err := c.recreateSession(); err != nil {
				return errors.Wrap(err, "recreate session")
			}
		}

		c.rpc.NotifyError(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode, BadMsgID: bad.BadMsgID})
		return nil
	case mt.BadServerSaltTypeID:
		var bad mt.BadServerSalt
		if err := bad.Decode(b); err != nil {
			return err
		}

		c.rpc.NotifyError(bad.BadMsgID, &badMessageError{Code: bad.ErrorCode, NewSalt: bad.NewServerSalt, BadMsgID: bad.BadMsgID})
		return nil
	default:
		return errors.Errorf("unknown type id 0x%d", id)
	}
}
