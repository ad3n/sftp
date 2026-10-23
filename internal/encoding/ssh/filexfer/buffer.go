package sshfx

import (
	"encoding/binary"
	"errors"
)

var (
	ErrShortPacket = errors.New("packet too short")

	ErrLongPacket = errors.New("packet too long")
)

type Buffer struct {
	b   []byte
	off int
	Err error
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{
		b: buf,
	}
}

func NewMarshalBuffer(size int) *Buffer {
	return NewBuffer(make([]byte, 4+1+4+size))
}

func (b *Buffer) Bytes() []byte {
	return b.b[b.off:]
}

func (b *Buffer) Len() int { return len(b.b) - b.off }

func (b *Buffer) Cap() int { return cap(b.b) }

func (b *Buffer) Reset() {
	*b = Buffer{
		b: b.b[:0],
	}
}

func (b *Buffer) StartPacket(packetType PacketType, requestID uint32) {
	*b = Buffer{
		b: append(b.b[:0], make([]byte, 4)...),
	}

	b.AppendUint8(uint8(packetType))
	b.AppendUint32(requestID)
}

func (b *Buffer) Packet(payload []byte) (header, payloadPassThru []byte, err error) {
	b.PutLength(len(b.b) - 4 + len(payload))

	return b.b, payload, nil
}

func (b *Buffer) ConsumeUint8() uint8 {
	if b.Err != nil {
		return 0
	}

	if b.Len() < 1 {
		b.off = len(b.b)
		b.Err = ErrShortPacket
		return 0
	}

	var v uint8
	v, b.off = b.b[b.off], b.off+1
	return v
}

func (b *Buffer) AppendUint8(v uint8) {
	b.b = append(b.b, v)
}

func (b *Buffer) ConsumeBool() bool {
	return b.ConsumeUint8() != 0
}

func (b *Buffer) AppendBool(v bool) {
	switch {
	case v:

		b.AppendUint8(1)

	default:

		b.AppendUint8(0)
	}
}

func (b *Buffer) ConsumeUint16() uint16 {
	if b.Err != nil {
		return 0
	}

	if b.Len() < 2 {
		b.off = len(b.b)
		b.Err = ErrShortPacket
		return 0
	}

	v := binary.BigEndian.Uint16(b.b[b.off:])
	b.off += 2
	return v
}

func (b *Buffer) AppendUint16(v uint16) {
	b.b = append(b.b,
		byte(v>>8),
		byte(v>>0),
	)
}

func unmarshalUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b[:4])
}

func (b *Buffer) ConsumeUint32() uint32 {
	if b.Err != nil {
		return 0
	}

	if b.Len() < 4 {
		b.off = len(b.b)
		b.Err = ErrShortPacket
		return 0
	}

	v := binary.BigEndian.Uint32(b.b[b.off:])
	b.off += 4
	return v
}

func (b *Buffer) AppendUint32(v uint32) {
	b.b = append(b.b,
		byte(v>>24),
		byte(v>>16),
		byte(v>>8),
		byte(v>>0),
	)
}

func (b *Buffer) ConsumeCount() int {
	return int(b.ConsumeUint32())
}

func (b *Buffer) AppendCount(v int) {
	b.AppendUint32(uint32(v))
}

func (b *Buffer) ConsumeUint64() uint64 {
	if b.Err != nil {
		return 0
	}

	if b.Len() < 8 {
		b.off = len(b.b)
		b.Err = ErrShortPacket
		return 0
	}

	v := binary.BigEndian.Uint64(b.b[b.off:])
	b.off += 8
	return v
}

func (b *Buffer) AppendUint64(v uint64) {
	b.b = append(b.b,
		byte(v>>56),
		byte(v>>48),
		byte(v>>40),
		byte(v>>32),
		byte(v>>24),
		byte(v>>16),
		byte(v>>8),
		byte(v>>0),
	)
}

func (b *Buffer) ConsumeInt64() int64 {
	return int64(b.ConsumeUint64())
}

func (b *Buffer) AppendInt64(v int64) {
	b.AppendUint64(uint64(v))
}

func (b *Buffer) ConsumeByteSlice() []byte {
	length := int(b.ConsumeUint32())
	if b.Err != nil {
		return nil
	}

	if b.Len() < length || length < 0 {
		b.off = len(b.b)
		b.Err = ErrShortPacket
		return nil
	}

	v := b.b[b.off:]
	if len(v) > length || cap(v) > length {
		v = v[:length:length]
	}

	b.off += int(length)
	return v
}

func (b *Buffer) ConsumeByteSliceCopy(hint []byte) []byte {
	data := b.ConsumeByteSlice()

	if grow := len(data) - len(hint); grow > 0 {
		hint = append(hint, make([]byte, grow)...)
	}

	n := copy(hint, data)
	hint = hint[:n]
	return hint
}

func (b *Buffer) AppendByteSlice(v []byte) {
	b.AppendUint32(uint32(len(v)))
	b.b = append(b.b, v...)
}

func (b *Buffer) ConsumeString() string {
	return string(b.ConsumeByteSlice())
}

func (b *Buffer) AppendString(v string) {
	b.AppendByteSlice([]byte(v))
}

func (b *Buffer) PutLength(size int) {
	if len(b.b) < 4 {
		b.b = append(b.b, make([]byte, 4-len(b.b))...)
	}

	binary.BigEndian.PutUint32(b.b, uint32(size))
}

func (b *Buffer) MarshalBinary() ([]byte, error) {
	clone := make([]byte, len(b.b))
	n := copy(clone, b.b)
	return clone[:n], nil
}

func (b *Buffer) UnmarshalBinary(data []byte) error {
	if grow := len(data) - len(b.b); grow > 0 {
		b.b = append(b.b, make([]byte, grow)...)
	}

	n := copy(b.b, data)
	b.b = b.b[:n]
	b.off = 0
	return nil
}
