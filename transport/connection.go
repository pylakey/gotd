package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/go-faster/errors"

	"github.com/gotd/td/bin"
)

// Conn is transport connection.
type Conn interface {
	Send(ctx context.Context, b *bin.Buffer) error
	Recv(ctx context.Context, b *bin.Buffer) error
	// SetReadDeadline sets the deadline for future Recv calls and any
	// currently-blocked Recv. A zero value clears the deadline. It lets an
	// external watchdog abort a Recv blocked on a silent/half-open socket.
	SetReadDeadline(t time.Time) error
	// SetWriteDeadline sets the deadline for future Send calls and any
	// currently-blocked Send. A zero value clears the deadline. It lets an
	// external watchdog abort a Send blocked on a full kernel buffer of a
	// silent/half-open socket.
	SetWriteDeadline(t time.Time) error
	Close() error
}

var _ Conn = (*connection)(nil)

// connection is MTProto connection.
type connection struct {
	conn  net.Conn
	codec Codec

	readMux  sync.Mutex
	writeMux sync.Mutex
}

// Send sends message from buffer using MTProto connection.
func (c *connection) Send(ctx context.Context, b *bin.Buffer) error {
	// Serializing access to deadlines.
	c.writeMux.Lock()
	defer c.writeMux.Unlock()

	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return errors.Wrap(err, "reset write deadline")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return errors.Wrap(err, "set write deadline")
		}
	}

	if err := c.codec.Write(c.conn, b); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

// Recv reads message to buffer using MTProto connection.
func (c *connection) Recv(ctx context.Context, b *bin.Buffer) error {
	// Serializing access to deadlines.
	c.readMux.Lock()
	defer c.readMux.Unlock()

	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return errors.Wrap(err, "reset read deadline")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			return errors.Wrap(err, "set read deadline")
		}
	}

	if err := c.codec.Read(c.conn, b); err != nil {
		return errors.Wrap(err, "read")
	}

	return nil
}

// SetReadDeadline sets the read deadline on the underlying net.Conn. Setting a
// past deadline aborts a Recv currently blocked in the codec read.
func (c *connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline on the underlying net.Conn. Setting a
// past deadline aborts a Send currently blocked in the codec write.
func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Close closes MTProto connection.
func (c *connection) Close() error {
	return c.conn.Close()
}
