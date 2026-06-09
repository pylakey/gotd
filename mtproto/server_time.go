package mtproto

import (
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/proto"
)

// maxTimeOffsetSeconds bounds how far a single learned/corrected server-time
// offset may move us from the local wall clock. The official Telegram client
// keeps a server time difference of the same order; anything beyond a few hours
// is far more likely to be a corrupt msg_id or a hostile value than a genuine
// clock skew, so we clamp it instead of letting it drive msg_id generation off
// a cliff.
const maxTimeOffsetSeconds = 24 * 60 * 60

// ServerTimeOffset holds the difference, in seconds, between the Telegram
// server clock and the local wall clock (server - local), mirroring the
// official client's persisted timeDifference. It is owned above the
// per-reconnect Conn so a learned/corrected offset survives reconnects and is
// applied to every generated msg_id and inbound id-bounds check.
//
// The zero value is ready to use and means "no known offset".
type ServerTimeOffset struct {
	seconds atomic.Int64
}

// NewServerTimeOffset returns an offset store seeded with the given value
// (clamped to the sanity bound). A zero seed means "no known offset".
func NewServerTimeOffset(seconds int) *ServerTimeOffset {
	o := &ServerTimeOffset{}
	o.Store(seconds)
	return o
}

// Load returns the current offset in seconds (server - local).
func (o *ServerTimeOffset) Load() int {
	return int(o.seconds.Load())
}

// Store sets the offset to the given value, clamped to the sanity bound.
func (o *ServerTimeOffset) Store(seconds int) {
	o.seconds.Store(int64(clampOffset(seconds)))
}

func clampOffset(seconds int) int {
	if seconds > maxTimeOffsetSeconds {
		return maxTimeOffsetSeconds
	}
	if seconds < -maxTimeOffsetSeconds {
		return -maxTimeOffsetSeconds
	}
	return seconds
}

// serverClock wraps a clock.Clock, shifting Now() by the shared server-time
// offset. Timer/Ticker are delegated unchanged: they measure durations, which
// the offset does not affect.
type serverClock struct {
	base   clock.Clock
	offset *ServerTimeOffset
}

func newServerClock(base clock.Clock, offset *ServerTimeOffset) serverClock {
	return serverClock{base: base, offset: offset}
}

// Now returns the local wall clock shifted by the current server-time offset,
// i.e. the client's best estimate of the server clock.
func (c serverClock) Now() time.Time {
	return c.base.Now().Add(time.Duration(c.offset.Load()) * time.Second)
}

func (c serverClock) Timer(d time.Duration) clock.Timer   { return c.base.Timer(d) }
func (c serverClock) Ticker(d time.Duration) clock.Ticker { return c.base.Ticker(d) }

// serverNow returns the client's current estimate of the server clock (local
// wall clock plus the persistent server-time offset). Inbound msg_id bounds and
// outbound msg_id generation are both anchored to this so they stay aligned
// with the server's accept window even when the local clock is skewed.
func (c *Conn) serverNow() time.Time {
	now := c.clock.Now()
	if c.timeOffset == nil {
		return now
	}
	return now.Add(time.Duration(c.timeOffset.Load()) * time.Second)
}

// learnTimeOffset sets the server-time offset from an observed server time,
// computed as serverTime - local. The delta is clamped to the sanity bound by
// the store. Returns the resulting offset in seconds. A no-op (returns 0) when
// the offset store is absent (bare test constructions).
func (c *Conn) learnTimeOffset(serverTime time.Time) int {
	if c.timeOffset == nil {
		return 0
	}
	offset := int(serverTime.Unix() - c.clock.Now().Unix())
	c.timeOffset.Store(offset)
	return c.timeOffset.Load()
}

// seedTimeOffsetFromExchange seeds the persistent server-time offset from the
// server_time reported during key exchange. Called after a fresh handshake so
// the very first content msg_id is already aligned with the server clock. A
// zero serverTime means the value was unavailable and is ignored.
func (c *Conn) seedTimeOffsetFromExchange(serverTime int64) {
	if serverTime == 0 {
		return
	}
	offset := c.learnTimeOffset(time.Unix(serverTime, 0))
	if offset != 0 {
		c.log.Debug("Seeded server-time offset from handshake",
			zap.Int("server_time_offset_seconds", offset),
		)
	}
}

// correctTimeOffset re-learns the server-time offset after a bad_msg_notification
// reporting that our msg_id was too low (code 16) or too high (code 17).
// serverMsgID is the envelope msg_id of the carrying message: it is
// server-stamped, so its time is the server clock at the moment the server
// produced the notification and is the authoritative reference.
//
// A downward correction (offset decreased) means our next msg_id would be lower
// than ids already sent in this session. Emitting a lower msg_id mid-session is
// itself a code-16 trigger, so instead — as the official client does — we rotate
// the session via recreateSession. The msg_id high-water mark lives in the
// MessageIDGen, not in the session_id, so rotation alone is not enough:
// recreateSession also re-anchors the generator to the corrected (lower) server
// clock so the next msg_id actually drops into the server's accept window. An
// upward correction is safe to apply in-place, since msg_ids only move forward.
func (c *Conn) correctTimeOffset(code int, serverMsgID int64) error {
	if c.timeOffset == nil {
		return nil
	}
	prev := c.timeOffset.Load()
	serverTime := proto.MessageID(serverMsgID).Time()
	next := c.learnTimeOffset(serverTime)

	log := c.log.With(
		zap.Int("error_code", code),
		zap.Int64("server_msg_id", serverMsgID),
		zap.Int("offset_before", prev),
		zap.Int("offset_after", next),
	)

	if next < prev {
		// Downward correction: rotate so the lower baseline is valid.
		log.Warn("Correcting server-time offset (downward) and recreating session")
		return c.recreateSession()
	}

	log.Warn("Correcting server-time offset")
	return nil
}
