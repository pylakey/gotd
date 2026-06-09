package telegram

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPingDefaults_AndroidGeneric checks R1: the public client defaults to the
// Telegram Android generic-connection keepalive — ping every 19s with an
// announced disconnect_delay of 35s (decoupled), and a 35s pong window.
func TestPingDefaults_AndroidGeneric(t *testing.T) {
	opt := Options{}
	opt.setDefaults()
	require.Equal(t, 19*time.Second, opt.PingInterval, "Android generic ping cadence")
	require.Equal(t, 35*time.Second, opt.PingDisconnectDelay, "Android generic disconnect_delay")
	require.Equal(t, 35*time.Second, opt.PingTimeout, "pong window matches announced disconnect_delay")
}

// TestPingDefaults_RespectsOverride ensures explicit values are not clobbered.
func TestPingDefaults_RespectsOverride(t *testing.T) {
	opt := Options{
		PingInterval:        180 * time.Second,
		PingDisconnectDelay: 420 * time.Second,
		PingTimeout:         30 * time.Second,
	}
	opt.setDefaults()
	require.Equal(t, 180*time.Second, opt.PingInterval)
	require.Equal(t, 420*time.Second, opt.PingDisconnectDelay)
	require.Equal(t, 30*time.Second, opt.PingTimeout)
}
