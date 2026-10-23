package sshfx

const (
	FlagRead = 1 << iota
	FlagWrite
	FlagAppend
	FlagCreate
	FlagTruncate
	FlagExclusive
)

type (
	OpenPacket struct {
		Filename string
		PFlags   uint32
		Attrs    Attributes
	}

	OpenDirPacket struct {
		Path string
	}
)

func (p *OpenPacket) Type() PacketType {
	return PacketTypeOpen
}

func (p *OpenPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Filename) + 4 + p.Attrs.Len()
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeOpen, reqid)
	buf.AppendString(p.Filename)
	buf.AppendUint32(p.PFlags)

	p.Attrs.MarshalInto(buf)

	return buf.Packet(payload)
}

func (p *OpenPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = OpenPacket{
		Filename: buf.ConsumeString(),
		PFlags:   buf.ConsumeUint32(),
	}

	return p.Attrs.UnmarshalFrom(buf)
}

func (p *OpenDirPacket) Type() PacketType {
	return PacketTypeOpenDir
}

func (p *OpenDirPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeOpenDir, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *OpenDirPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = OpenDirPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}
