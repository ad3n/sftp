package sshfx

import (
	"fmt"
)

type (
	StatusPacket struct {
		StatusCode   Status
		ErrorMessage string
		LanguageTag  string
	}

	HandlePacket struct {
		Handle string
	}

	DataPacket struct {
		Data []byte
	}

	NamePacket struct {
		Entries []*NameEntry
	}

	AttrsPacket struct {
		Attrs Attributes
	}
)

func (p *StatusPacket) Error() string {
	if p.ErrorMessage == "" {
		return "sftp: " + p.StatusCode.String()
	}

	return fmt.Sprintf("sftp: %s: %q", p.StatusCode, p.ErrorMessage)
}

func (p *StatusPacket) Is(target error) bool {
	if target, ok := target.(*StatusPacket); ok {
		return p.StatusCode == target.StatusCode
	}

	return p.StatusCode == target
}

func (p *StatusPacket) Type() PacketType {
	return PacketTypeStatus
}

func (p *StatusPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + 4 + len(p.ErrorMessage) + 4 + len(p.LanguageTag)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeStatus, reqid)
	buf.AppendUint32(uint32(p.StatusCode))
	buf.AppendString(p.ErrorMessage)
	buf.AppendString(p.LanguageTag)

	return buf.Packet(payload)
}

func (p *StatusPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = StatusPacket{
		StatusCode:   Status(buf.ConsumeUint32()),
		ErrorMessage: buf.ConsumeString(),
		LanguageTag:  buf.ConsumeString(),
	}

	return buf.Err
}

func (p *HandlePacket) Type() PacketType {
	return PacketTypeHandle
}

func (p *HandlePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeHandle, reqid)
	buf.AppendString(p.Handle)

	return buf.Packet(payload)
}

func (p *HandlePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = HandlePacket{
		Handle: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *DataPacket) Type() PacketType {
	return PacketTypeData
}

func (p *DataPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeData, reqid)
	buf.AppendUint32(uint32(len(p.Data)))

	return buf.Packet(p.Data)
}

func (p *DataPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = DataPacket{
		Data: buf.ConsumeByteSliceCopy(p.Data),
	}

	return buf.Err
}

func (p *NamePacket) Type() PacketType {
	return PacketTypeName
}

func (p *NamePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4

		for _, e := range p.Entries {
			size += e.Len()
		}

		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeName, reqid)
	buf.AppendUint32(uint32(len(p.Entries)))

	for _, e := range p.Entries {
		e.MarshalInto(buf)
	}

	return buf.Packet(payload)
}

func (p *NamePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	count := buf.ConsumeCount()
	if buf.Err != nil {
		return buf.Err
	}

	*p = NamePacket{
		Entries: make([]*NameEntry, 0, count),
	}

	for range count {
		var e NameEntry
		if err := e.UnmarshalFrom(buf); err != nil {
			return err
		}

		p.Entries = append(p.Entries, &e)
	}

	return buf.Err
}

func (p *AttrsPacket) Type() PacketType {
	return PacketTypeAttrs
}

func (p *AttrsPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := p.Attrs.Len()
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeAttrs, reqid)
	p.Attrs.MarshalInto(buf)

	return buf.Packet(payload)
}

func (p *AttrsPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	return p.Attrs.UnmarshalFrom(buf)
}
