package telegram

import (
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/gotd/td/clock"
)

func TestReconnectBackoff_NetworkFailureProfile(t *testing.T) {
	b := newReconnectBackoff()
	b.setLastError(syscall.ECONNRESET)

	// Android socket-failure profile: 50→100→200→400→400ms.
	want := []time.Duration{50, 100, 200, 400, 400, 400}
	for i, w := range want {
		got := b.NextBackOff()
		require.Equalf(t, w*time.Millisecond, got, "step %d", i)
		require.NotEqualf(t, backoff.Stop, got, "step %d must not stop", i)
	}
}

func TestReconnectBackoff_CleanDropFixed1s(t *testing.T) {
	b := newReconnectBackoff()
	// No error and non-network errors are clean drops: fixed 1s.
	for _, err := range []error{nil, io.EOF} {
		b.setLastError(err)
		for range 3 {
			require.Equal(t, time.Second, b.NextBackOff())
		}
	}
}

func TestReconnectBackoff_ResetReturnsToInitial(t *testing.T) {
	b := newReconnectBackoff()
	b.setLastError(syscall.ECONNRESET)
	require.Equal(t, 50*time.Millisecond, b.NextBackOff())
	require.Equal(t, 100*time.Millisecond, b.NextBackOff())

	// Reset mirrors Android resetting the interval once data is useful.
	b.Reset()
	require.Equal(t, time.Second, b.NextBackOff(), "reset clears the recorded error -> clean drop")

	b.setLastError(syscall.EHOSTUNREACH)
	require.Equal(t, 50*time.Millisecond, b.NextBackOff(), "doubling restarts from 50ms after reset")
}

func TestReconnectBackoff_Classification(t *testing.T) {
	netFail := []error{
		syscall.ECONNRESET,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		syscall.ECONNREFUSED,
		syscall.ETIMEDOUT,
	}
	for _, err := range netFail {
		require.Truef(t, isNetworkFailure(err), "%v should be a network failure", err)
	}
	for _, err := range []error{nil, io.EOF, io.ErrUnexpectedEOF} {
		require.Falsef(t, isNetworkFailure(err), "%v should be a clean drop", err)
	}
}

func TestDefaultBackoff_NeverStops(t *testing.T) {
	b := defaultBackoff(clock.System)()
	for range 20 {
		require.NotEqual(t, backoff.Stop, b.NextBackOff())
	}
}
