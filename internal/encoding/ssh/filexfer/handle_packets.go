package sshfx

type (
	ClosePacket struct {
		Handle string
	}

	ReadPacket struct {
		Handle string
		Offset uint64
		Length uint32
	}

	WritePacket struct {
		Handle string
		Offset uint64
		Data   []byte
	}

	FStatPacket struct {
		Handle string
	}

	FSetstatPacket struct {
		Handle string
		Attrs  Attributes
	}

	ReadDirPacket struct {
		Handle string
	}
)

func (p *ClosePacket) Type() PacketType {
	return PacketTypeClose
}

func (p *ClosePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeClose, reqid)
	buf.AppendString(p.Handle)

	return buf.Packet(payload)
}

func (p *ClosePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = ClosePacket{
		Handle: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *ReadPacket) Type() PacketType {
	return PacketTypeRead
}

func (p *ReadPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle) + 8 + 4
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeRead, reqid)
	buf.AppendString(p.Handle)
	buf.AppendUint64(p.Offset)
	buf.AppendUint32(p.Length)

	return buf.Packet(payload)
}

func (p *ReadPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = ReadPacket{
		Handle: buf.ConsumeString(),
		Offset: buf.ConsumeUint64(),
		Length: buf.ConsumeUint32(),
	}

	return buf.Err
}

func (p *WritePacket) Type() PacketType {
	return PacketTypeWrite
}

func (p *WritePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle) + 8 + 4
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeWrite, reqid)
	buf.AppendString(p.Handle)
	buf.AppendUint64(p.Offset)
	buf.AppendUint32(uint32(len(p.Data)))

	return buf.Packet(p.Data)
}

func (p *WritePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = WritePacket{
		Handle: buf.ConsumeString(),
		Offset: buf.ConsumeUint64(),
		Data:   buf.ConsumeByteSliceCopy(p.Data),
	}

	return buf.Err
}

func (p *FStatPacket) Type() PacketType {
	return PacketTypeFStat
}

func (p *FStatPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeFStat, reqid)
	buf.AppendString(p.Handle)

	return buf.Packet(payload)
}

func (p *FStatPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = FStatPacket{
		Handle: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *FSetstatPacket) Type() PacketType {
	return PacketTypeFSetstat
}

func (p *FSetstatPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle) + p.Attrs.Len()
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeFSetstat, reqid)
	buf.AppendString(p.Handle)

	p.Attrs.MarshalInto(buf)

	return buf.Packet(payload)
}

func (p *FSetstatPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = FSetstatPacket{
		Handle: buf.ConsumeString(),
	}

	return p.Attrs.UnmarshalFrom(buf)
}

func (p *ReadDirPacket) Type() PacketType {
	return PacketTypeReadDir
}

func (p *ReadDirPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Handle)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeReadDir, reqid)
	buf.AppendString(p.Handle)

	return buf.Packet(payload)
}

func (p *ReadDirPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = ReadDirPacket{
		Handle: buf.ConsumeString(),
	}

	return buf.Err
}
