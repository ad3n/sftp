package sshfx

import (
	"errors"
	"io"
)

const smallBufferSize = 64

type (
	RawPacket struct {
		PacketType PacketType
		RequestID  uint32

		Data Buffer
	}

	RequestPacket struct {
		RequestID uint32

		Request Packet
	}
)

func (p *RawPacket) Type() PacketType {
	return p.PacketType
}

func (p *RawPacket) Reset() {
	p.Data = Buffer{}
}

func (p *RawPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		buf = NewMarshalBuffer(0)
	}

	buf.StartPacket(p.PacketType, reqid)

	return buf.Packet(p.Data.Bytes())
}

func (p *RawPacket) MarshalBinary() ([]byte, error) {
	return ComposePacket(p.MarshalPacket(p.RequestID, nil))
}

func (p *RawPacket) UnmarshalFrom(buf *Buffer) error {
	*p = RawPacket{
		PacketType: PacketType(buf.ConsumeUint8()),
		RequestID:  buf.ConsumeUint32(),
	}

	p.Data = *buf

	return buf.Err
}

func (p *RawPacket) UnmarshalBinary(data []byte) error {
	clone := make([]byte, len(data))
	n := copy(clone, data)
	return p.UnmarshalFrom(NewBuffer(clone[:n]))
}

func readPacket(r io.Reader, b []byte, maxPacketLength uint32) ([]byte, error) {
	if cap(b) < 4 {
		b = make([]byte, smallBufferSize)
	}

	if _, err := io.ReadFull(r, b[:4]); err != nil {
		return nil, err
	}

	length := unmarshalUint32(b)
	if int(length) < 5 {
		if int(length) < 0 {
			return nil, ErrLongPacket
		}

		return nil, ErrShortPacket
	}

	if length > maxPacketLength {
		return nil, ErrLongPacket
	}

	if int(length) > cap(b) {
		b = make([]byte, length)
	}

	n, err := io.ReadFull(r, b[:length])
	return b[:n], err
}

func (p *RawPacket) ReadPacket(r io.Reader, b []byte, maxPacketLength uint32) error {
	b, err := readPacket(r, b, maxPacketLength)
	if err != nil {
		return err
	}

	return p.UnmarshalFrom(NewBuffer(b))
}

func (p *RequestPacket) Type() PacketType {
	return p.Request.Type()
}

func (p *RequestPacket) Reset() {
	p.Request = nil
}

func (p *RequestPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	if p.Request == nil {
		return nil, nil, errors.New("empty request packet")
	}

	return p.Request.MarshalPacket(reqid, b)
}

func (p *RequestPacket) MarshalBinary() ([]byte, error) {
	return ComposePacket(p.MarshalPacket(p.RequestID, nil))
}

func (p *RequestPacket) UnmarshalFrom(buf *Buffer) error {
	typ := PacketType(buf.ConsumeUint8())
	if buf.Err != nil {
		return buf.Err
	}

	req, err := newPacketFromType(typ)
	if err != nil {
		return err
	}

	*p = RequestPacket{
		RequestID: buf.ConsumeUint32(),
		Request:   req,
	}

	return p.Request.UnmarshalPacketBody(buf)
}

func (p *RequestPacket) UnmarshalBinary(data []byte) error {
	clone := make([]byte, len(data))
	n := copy(clone, data)
	return p.UnmarshalFrom(NewBuffer(clone[:n]))
}

func (p *RequestPacket) ReadPacket(r io.Reader, b []byte, maxPacketLength uint32) error {
	b, err := readPacket(r, b, maxPacketLength)
	if err != nil {
		return err
	}

	return p.UnmarshalFrom(NewBuffer(b))
}
