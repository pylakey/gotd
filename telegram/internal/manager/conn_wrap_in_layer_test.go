package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

// readyConn builds a test connection whose gotConfig is already signaled, so
// Invoke does not block on waitSession. The layer is considered negotiated by
// init() for this conn's auth-key (Android per-auth-key init state), so Invoke
// must send the request without re-wrapping it in invokeWithLayer.
func readyConn(mode ConnMode, proto protoConn) *Conn {
	c := newTestConn(mode, proto)
	c.gotConfig.Signal()
	return c
}

// TestInvokeBareInUpdatesMode proves the Android wrapInLayer parity at the unit
// level: once inited, an Invoke in ConnModeUpdates sends the TRULY bare TL
// method — no invokeWithLayer, no invokeWithoutUpdates. Against current HEAD
// (which wraps every request in invokeWithLayer) this FAILS.
func TestInvokeBareInUpdatesMode(t *testing.T) {
	a := require.New(t)
	p := &captureProto{}
	c := readyConn(ConnModeUpdates, p)

	in := &tg.UsersGetUsersRequest{ID: []tg.InputUserClass{&tg.InputUserSelf{}}}
	a.NoError(c.Invoke(context.Background(), in, &tg.UserClassVector{}))

	a.Equal(1, p.invokeCalls)
	// Truly bare: the recorded request must be the raw method, with no
	// invokeWithLayer and no invokeWithoutUpdates wrapper.
	_, isWithLayer := p.lastInput.(*tg.InvokeWithLayerRequest)
	a.False(isWithLayer, "ConnModeUpdates must not wrap inited request in invokeWithLayer")
	_, isWithoutUpdates := p.lastInput.(*tg.InvokeWithoutUpdatesRequest)
	a.False(isWithoutUpdates, "ConnModeUpdates must not wrap in invokeWithoutUpdates")

	bare, ok := p.lastInput.(noopDecoder)
	a.True(ok, "request must be sent as the bare noopDecoder-wrapped method")
	_, ok = bare.Encoder.(*tg.UsersGetUsersRequest)
	a.True(ok, "innermost request must be the raw users.getUsers method")
}

// ConnModeData's preservation of the orthogonal invokeWithoutUpdates wrapper
// (without invokeWithLayer) once inited is asserted by
// TestConnInvokeDataKeepsInvokeWithoutUpdates in conn_cdn_test.go.

var _ bin.Encoder = (*tg.UsersGetUsersRequest)(nil)
