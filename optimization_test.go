package sftp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
)

type (
	allocationPacket struct{ data [9]byte }

	allocationReplyWriter struct {
		client  *clientConn
		reply   bool
		pending []chan<- result
	}
)

func (p *allocationPacket) id() uint32                     { return 1 }
func (p *allocationPacket) MarshalBinary() ([]byte, error) { return p.data[:], nil }

func (w *allocationReplyWriter) Write(p []byte) (int, error) {
	ch, _ := w.client.getChannel(binary.BigEndian.Uint32(p[5:]))
	if w.reply {
		ch <- result{typ: sshFxpStatus}
		return len(p), nil
	}

	w.pending = append(w.pending, ch)
	return len(p), nil
}

func (w *allocationReplyWriter) Close() error { return nil }

func newAllocationClient() (*Client, *allocationReplyWriter) {
	c := &Client{inflight: make(map[uint32]chan<- result), closed: make(chan struct{}), resPool: newResChanPool(1)}
	w := &allocationReplyWriter{client: &c.clientConn, reply: true}
	c.WriteCloser = w
	return c, w
}

func BenchmarkClientRequestChannel(b *testing.B) {
	c, _ := newAllocationClient()
	p := &allocationPacket{data: [9]byte{0, 0, 0, 0, sshFxpStat, 0, 0, 0, 1}}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := c.sendPacket(context.Background(), nil, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrderedPacketsSort(b *testing.B) {
	packets := make(orderedPackets, 32)
	for i := range packets {
		packets[i] = &orderedResponse{orderid: uint32(32 - i)}
	}

	b.ReportAllocs()
	for b.Loop() {
		packets.Sort()
		packets[0], packets[31] = packets[31], packets[0]
	}
}

func BenchmarkConnReceive(b *testing.B) {
	data := []byte{0, 0, 0, 5, sshFxpStatus, 0, 0, 0, 1}
	r := bytes.NewReader(data)
	c := &conn{Reader: r}
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(data)
		if _, _, err := c.recvPacket(0); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRequestChannelCancellationOwnership(t *testing.T) {
	c, w := newAllocationClient()
	w.reply = false
	p := &allocationPacket{data: [9]byte{0, 0, 0, 0, sshFxpStat, 0, 0, 0, 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.sendPacket(ctx, nil, p); err != context.Canceled {
		t.Fatalf("got %v", err)
	}

	w.pending[0] <- result{err: io.ErrUnexpectedEOF}
	w.reply = true
	if _, _, err := c.sendPacket(context.Background(), nil, p); err != nil {
		t.Fatalf("late response leaked into next request: %v", err)
	}
}

func TestSynchronousRequestChannelReuse(t *testing.T) {
	c, _ := newAllocationClient()
	p := &allocationPacket{data: [9]byte{0, 0, 0, 0, sshFxpStat, 0, 0, 0, 1}}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, _, err := c.sendPacket(context.Background(), nil, p); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("got %g allocations, want zero", allocs)
	}

	pooled := c.resPool.Get()
	c.resPool.Put(pooled)
	caller := make(chan result, 1)
	if _, _, err := c.sendPacket(context.Background(), caller, p); err != nil {
		t.Fatal(err)
	}

	if got := c.resPool.Get(); got != pooled {
		t.Fatal("caller-owned channel replaced pooled channel")
	}
}

func TestConnReceivePayloadOwnership(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 5, sshFxpStatus, 1, 2, 3, 4, 0, 0, 0, 5, sshFxpStatus, 5, 6, 7, 8})
	c := &conn{Reader: r}
	_, first, err := c.recvPacket(0)
	if err != nil {
		t.Fatal(err)
	}

	_, second, err := c.recvPacket(0)
	if err != nil {
		t.Fatal(err)
	}

	clear(second)
	if !bytes.Equal(first, []byte{1, 2, 3, 4}) {
		t.Fatalf("earlier packet changed: %x", first)
	}
}

func TestOrderedPacketsSortNoAllocations(t *testing.T) {
	packets := orderedPackets{&orderedResponse{orderid: ^uint32(0)}, &orderedResponse{orderid: 1}, &orderedResponse{orderid: 0}}
	if allocs := testing.AllocsPerRun(1000, func() {
		packets[0], packets[2] = packets[2], packets[0]
		packets.Sort()
	}); allocs != 0 {
		t.Fatalf("got %g allocations, want zero", allocs)
	}

	for i, id := range []uint32{0, 1, ^uint32(0)} {
		if packets[i].orderID() != id {
			t.Fatalf("position %d: got %d, want %d", i, packets[i].orderID(), id)
		}
	}
}
