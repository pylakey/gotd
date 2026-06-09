package manager

import (
	"maps"
	"sync"
)

// InitVersionCache tracks, per DC, the initConnection version already
// negotiated with the server, so reconnects to a DC can skip re-sending the
// full initConnection.
//
// It mirrors the official Telegram Android client's persisted per-DC init
// version: once a DC has accepted an initConnection for a given
// app/device/lang identity, plain reconnects send bare requests, and the
// identity is persisted so a process restart does not re-init either.
//
// A nil *InitVersionCache disables caching entirely (every connection performs
// a full init), keeping the zero value safe for callers that do not opt in.
type InitVersionCache struct {
	mu sync.Mutex
	m  map[int]int64
}

// NewInitVersionCache creates an empty cache.
func NewInitVersionCache() *InitVersionCache {
	return &InitVersionCache{m: map[int]int64{}}
}

// Done reports whether the given init version was already negotiated for dc.
func (c *InitVersionCache) Done(dc int, version int64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[dc]
	return ok && v == version
}

// Set records that version was negotiated for dc. It must be called only after
// a successful (non-error) initConnection reply, mirroring how Android records
// the init version only after the reply.
func (c *InitVersionCache) Set(dc int, version int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.m[dc] = version
	c.mu.Unlock()
}

// Delete drops the cached init version for dc, forcing the next connection to
// that DC to re-send the full initConnection. Used to recover when the server
// rejects a bare (skip-path) request because its init state — bound to the
// auth_key — diverged from the cache.
func (c *InitVersionCache) Delete(dc int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.m, dc)
	c.mu.Unlock()
}

// Snapshot returns a copy of the per-DC init versions for persistence.
func (c *InitVersionCache) Snapshot() map[int]int64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) == 0 {
		return nil
	}
	return maps.Clone(c.m)
}

// Restore seeds the cache from persisted per-DC init versions.
func (c *InitVersionCache) Restore(m map[int]int64) {
	if c == nil || len(m) == 0 {
		return
	}
	c.mu.Lock()
	maps.Copy(c.m, m)
	c.mu.Unlock()
}
