package session

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestData_TimeOffsetRoundTrip asserts that the persistent server-time offset
// (Android timeDifference) survives a Save/Load round-trip, and that older
// session files without the field load as offset 0 (JSON-additive, like
// InitVersions).
func TestData_TimeOffsetRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("RoundTrip", func(t *testing.T) {
		a := require.New(t)
		loader := Loader{Storage: &StorageMemory{}}

		data := &Data{
			DC:         2,
			AuthKey:    make([]byte, 256),
			AuthKeyID:  []byte("gotd1337"),
			Salt:       10,
			TimeOffset: 137,
		}
		a.NoError(loader.Save(ctx, data))

		got, err := loader.Load(ctx)
		a.NoError(err)
		a.Equal(137, got.TimeOffset, "TimeOffset must survive Save/Load")
	})

	t.Run("NegativeRoundTrip", func(t *testing.T) {
		a := require.New(t)
		loader := Loader{Storage: &StorageMemory{}}

		data := &Data{DC: 2, TimeOffset: -42}
		a.NoError(loader.Save(ctx, data))

		got, err := loader.Load(ctx)
		a.NoError(err)
		a.Equal(-42, got.TimeOffset)
	})

	t.Run("OlderFileLoadsAsZero", func(t *testing.T) {
		a := require.New(t)

		// An older session file: valid version, no TimeOffset key.
		raw := []byte(`{"Version":1,"Data":{"DC":2,"Salt":7}}`)
		storage := &StorageMemory{}
		a.NoError(storage.StoreSession(ctx, raw))

		// Sanity: the raw payload genuinely lacks the field.
		var probe struct {
			Data map[string]json.RawMessage
		}
		a.NoError(json.Unmarshal(raw, &probe))
		_, present := probe.Data["TimeOffset"]
		a.False(present, "fixture must not contain TimeOffset")

		loader := Loader{Storage: storage}
		got, err := loader.Load(ctx)
		a.NoError(err)
		a.Equal(0, got.TimeOffset, "absent field must load as offset 0")
	})
}
