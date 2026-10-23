package sshfx

import (
	"encoding"
	"sync"
)

var extendedPacketTypes = struct {
	mu           sync.RWMutex
	constructors map[string]ExtendedDataConstructor
}{
	constructors: make(map[string]ExtendedDataConstructor),
}

type (
	ExtendedData = interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
	}

	ExtendedDataConstructor func() ExtendedData

	ExtendedPacket struct {
		ExtendedRequest string

		Data ExtendedData
	}

	ExtendedReplyPacket struct {
		Data ExtendedData
	}
)

func RegisterExtendedPacketType(extension string, constructor ExtendedDataConstructor) {
	extendedPacketTypes.mu.Lock()
	defer extendedPacketTypes.mu.Unlock()

	if _, exist := extendedPacketTypes.constructors[extension]; exist {
		panic("encoding/ssh/filexfer: multiple registration of extended packet type " + extension)
	}

	extendedPacketTypes.constructors[extension] = constructor
}

func newExtendedPacket(extension string) ExtendedData {
	extendedPacketTypes.mu.RLock()
	defer extendedPacketTypes.mu.RUnlock()

	if f := extendedPacketTypes.constructors[extension]; f != nil {
		return f()
	}

	return new(Buffer)
}

func (p *ExtendedPacket) Type() PacketType {
	return PacketTypeExtended
}

func (p *ExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.ExtendedRequest)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeExtended, reqid)
	buf.AppendString(p.ExtendedRequest)

	if p.Data != nil {
		payload, err = p.Data.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
	}

	return buf.Packet(payload)
}

func (p *ExtendedPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	p.ExtendedRequest = buf.ConsumeString()
	if buf.Err != nil {
		return buf.Err
	}

	if p.Data == nil {
		p.Data = newExtendedPacket(p.ExtendedRequest)
	}

	return p.Data.UnmarshalBinary(buf.Bytes())
}

func (p *ExtendedReplyPacket) Type() PacketType {
	return PacketTypeExtendedReply
}

func (p *ExtendedReplyPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		buf = NewMarshalBuffer(0)
	}

	buf.StartPacket(PacketTypeExtendedReply, reqid)

	if p.Data != nil {
		payload, err = p.Data.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
	}

	return buf.Packet(payload)
}

func (p *ExtendedReplyPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	if p.Data == nil {
		p.Data = new(Buffer)
	}

	return p.Data.UnmarshalBinary(buf.Bytes())
}
