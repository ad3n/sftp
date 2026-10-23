package sftp

import (
	"context"
	"encoding"
	"fmt"
	"io"
	"sync"
)

type (
	conn struct {
		io.Reader
		io.WriteCloser

		alloc *allocator
		sync.Mutex
	}

	clientConn struct {
		err error

		wait func() error

		inflight map[uint32]chan<- result

		closed chan struct{}
		conn
		wg sync.WaitGroup

		sync.Mutex
	}

	result struct {
		err  error
		data []byte
		typ  fxp
	}

	idmarshaler interface {
		id() uint32
		encoding.BinaryMarshaler
	}

	serverConn struct {
		conn
	}
)

func (c *conn) recvPacket(orderID uint32) (fxp, []byte, error) {
	return recvPacket(c, c.alloc, orderID)
}

func (c *conn) sendPacket(m encoding.BinaryMarshaler) error {
	c.Lock()
	defer c.Unlock()

	return sendPacket(c, m)
}

func (c *conn) Close() error {
	c.Lock()
	defer c.Unlock()

	return c.WriteCloser.Close()
}

func (c *clientConn) Wait() error {
	<-c.closed

	if c.wait == nil {
		return c.err
	}

	if err := c.wait(); err != nil {
		if err.Error() == "ssh: session not started" {
			return c.err
		}

		return err
	}

	return c.err
}

func (c *clientConn) Close() error {
	defer c.wg.Wait()

	return c.conn.Close()
}

func (c *clientConn) recv() error {
	defer c.conn.Close()

	for {
		typ, data, err := c.recvPacket(0)
		if err != nil {
			return err
		}

		sid, _, err := unmarshalUint32Safe(data)
		if err != nil {
			return err
		}

		ch, ok := c.getChannel(sid)
		if !ok {
			return fmt.Errorf("sid not found: %d", sid)
		}

		ch <- result{typ: typ, data: data}
	}
}

func (c *clientConn) putChannel(ch chan<- result, sid uint32) bool {
	c.Lock()
	defer c.Unlock()

	select {
	case <-c.closed:

		ch <- result{err: ErrSSHFxConnectionLost}
		return false
	default:
	}

	c.inflight[sid] = ch
	return true
}

func (c *clientConn) getChannel(sid uint32) (chan<- result, bool) {
	c.Lock()
	defer c.Unlock()

	ch, ok := c.inflight[sid]
	delete(c.inflight, sid)

	return ch, ok
}

func (c *clientConn) sendPacket(ctx context.Context, ch chan result, p idmarshaler) (fxp, []byte, error) {
	if cap(ch) < 1 {
		ch = make(chan result, 1)
	}

	c.dispatchRequest(ch, p)

	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case s := <-ch:
		return s.typ, s.data, s.err
	}
}

func (c *clientConn) dispatchRequest(ch chan<- result, p idmarshaler) {
	sid := p.id()

	if !c.putChannel(ch, sid) {
		return
	}

	if err := c.conn.sendPacket(p); err != nil {
		if ch, ok := c.getChannel(sid); ok {
			ch <- result{err: err}
		}
	}
}

func (c *clientConn) broadcastErr(err error) {
	c.Lock()
	defer c.Unlock()

	bcastRes := result{err: ErrSSHFxConnectionLost}
	for sid, ch := range c.inflight {
		ch <- bcastRes

		c.inflight[sid] = make(chan<- result, 1)
	}

	c.err = err
	close(c.closed)
}

func (s *serverConn) sendError(id uint32, err error) error {
	return s.sendPacket(statusFromError(id, err))
}
