package manager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproto"
)

// sessionProto is a protoConn stub returning a configurable session, used to
// exercise initVersion()'s auth-key keying.
type sessionProto struct {
	captureProto
	session mtproto.Session
}

func (p *sessionProto) Session() mtproto.Session { return p.session }

// TestInitVersion_PFSStableAcrossTempRotation pins R6 behavior in PFS mode:
// the init version must key off the stable permanent key, so a temporary-key
// rotation (every reconnect) does NOT bust the per-DC init cache, while a real
// permanent-key change does.
func TestInitVersion_PFSStableAcrossTempRotation(t *testing.T) {
	a := require.New(t)
	dev := DeviceConfig{
		DeviceModel:    "Pixel",
		SystemVersion:  "Android 14",
		AppVersion:     "10.0.0",
		SystemLangCode: "en",
		LangPack:       "android",
		LangCode:       "en",
	}
	mk := func(sess mtproto.Session) int64 {
		c := newTestConn(ConnModeUpdates, &sessionProto{session: sess})
		c.device = dev
		return c.initVersion()
	}
	perm := crypto.Key{1}.WithID()

	// PFS: PermKey set, runtime Key (temp) rotates between reconnects.
	v1 := mk(mtproto.Session{Key: crypto.Key{2}.WithID(), PermKey: perm})
	v2 := mk(mtproto.Session{Key: crypto.Key{3}.WithID(), PermKey: perm})
	a.Equal(v1, v2, "PFS: temp-key rotation must NOT change the init version")

	// A permanent-key change (re-auth / key rotation) must bust the cache.
	v3 := mk(mtproto.Session{Key: crypto.Key{2}.WithID(), PermKey: crypto.Key{9}.WithID()})
	a.NotEqual(v1, v3, "PFS: permanent-key change must change the init version")

	// Non-PFS: PermKey zero, so the single long-lived Key drives the version.
	n1 := mk(mtproto.Session{Key: crypto.Key{2}.WithID()})
	n2 := mk(mtproto.Session{Key: crypto.Key{7}.WithID()})
	a.NotEqual(n1, n2, "non-PFS: auth-key change must change the init version")
}

func TestInitVersionCacheDoneSet(t *testing.T) {
	a := require.New(t)
	c := NewInitVersionCache()

	a.False(c.Done(2, 100), "empty cache must report not done")

	c.Set(2, 100)
	a.True(c.Done(2, 100), "recorded version must report done")
	a.False(c.Done(2, 101), "different version must report not done")
	a.False(c.Done(4, 100), "different DC must report not done")

	// Overwriting with a new version invalidates the old one.
	c.Set(2, 200)
	a.False(c.Done(2, 100), "stale version must report not done after overwrite")
	a.True(c.Done(2, 200), "new version must report done")
}

func TestInitVersionCacheSnapshotRestore(t *testing.T) {
	a := require.New(t)
	c := NewInitVersionCache()
	c.Set(2, 100)
	c.Set(4, 400)

	snap := c.Snapshot()
	a.Equal(map[int]int64{2: 100, 4: 400}, snap)

	// Snapshot must be a copy: mutating it must not affect the cache.
	snap[2] = 999
	a.True(c.Done(2, 100), "snapshot must be a copy independent of the cache")

	// Restore into a fresh cache reproduces the state.
	restored := NewInitVersionCache()
	restored.Restore(map[int]int64{2: 100, 4: 400})
	a.True(restored.Done(2, 100))
	a.True(restored.Done(4, 400))
}

func TestInitVersionCacheSnapshotEmpty(t *testing.T) {
	a := require.New(t)
	a.Nil(NewInitVersionCache().Snapshot(), "empty cache snapshot must be nil")
}

func TestInitVersionCacheDelete(t *testing.T) {
	a := require.New(t)
	c := NewInitVersionCache()
	c.Set(2, 100)
	c.Set(4, 400)

	c.Delete(2)
	a.False(c.Done(2, 100), "deleted DC must report not done (forces full re-init)")
	a.True(c.Done(4, 400), "unrelated DC must be unaffected")

	// Deleting a missing DC is a no-op and must not panic.
	a.NotPanics(func() { c.Delete(99) })

	// Delete must be safe on a nil cache.
	var nilCache *InitVersionCache
	a.NotPanics(func() { nilCache.Delete(2) })
}

func TestInitVersionCacheNilSafe(t *testing.T) {
	a := require.New(t)
	var c *InitVersionCache

	// All methods must be safe on a nil receiver (caching disabled).
	a.NotPanics(func() {
		a.False(c.Done(2, 100), "nil cache must always report not done")
		c.Set(2, 100)
		a.False(c.Done(2, 100), "nil cache must not record anything")
		a.Nil(c.Snapshot(), "nil cache snapshot must be nil")
		c.Restore(map[int]int64{2: 100})
	})
}

func TestInitVersionChangesWithDeviceField(t *testing.T) {
	a := require.New(t)

	base := newTestConn(ConnModeUpdates, &captureProto{})
	base.device = DeviceConfig{
		DeviceModel:    "Pixel",
		SystemVersion:  "Android 14",
		AppVersion:     "10.0.0",
		SystemLangCode: "en",
		LangPack:       "android",
		LangCode:       "en",
	}
	baseVersion := base.initVersion()

	// Identical identity must yield an identical version (stable hash).
	same := newTestConn(ConnModeUpdates, &captureProto{})
	same.appID = base.appID
	same.device = base.device
	a.Equal(baseVersion, same.initVersion(), "identical identity must hash identically")

	mutate := func(fn func(c *Conn)) int64 {
		c := newTestConn(ConnModeUpdates, &captureProto{})
		c.appID = base.appID
		c.device = base.device
		fn(c)
		return c.initVersion()
	}

	cases := []struct {
		name string
		fn   func(c *Conn)
	}{
		{"appID", func(c *Conn) { c.appID = base.appID + 1 }},
		{"DeviceModel", func(c *Conn) { c.device.DeviceModel = "Galaxy" }},
		{"SystemVersion", func(c *Conn) { c.device.SystemVersion = "Android 15" }},
		{"AppVersion", func(c *Conn) { c.device.AppVersion = "10.0.1" }},
		{"SystemLangCode", func(c *Conn) { c.device.SystemLangCode = "ru" }},
		{"LangPack", func(c *Conn) { c.device.LangPack = "ios" }},
		{"LangCode", func(c *Conn) { c.device.LangCode = "ru" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require.NotEqual(t, baseVersion, mutate(tt.fn),
				"changing %s must change the init version", tt.name)
		})
	}
}
