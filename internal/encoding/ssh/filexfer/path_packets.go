package sshfx

type (
	LStatPacket struct {
		Path string
	}

	SetstatPacket struct {
		Path  string
		Attrs Attributes
	}

	RemovePacket struct {
		Path string
	}

	MkdirPacket struct {
		Path  string
		Attrs Attributes
	}

	RmdirPacket struct {
		Path string
	}

	RealPathPacket struct {
		Path string
	}

	StatPacket struct {
		Path string
	}

	RenamePacket struct {
		OldPath string
		NewPath string
	}

	ReadLinkPacket struct {
		Path string
	}

	SymlinkPacket struct {
		LinkPath   string
		TargetPath string
	}
)

func (p *LStatPacket) Type() PacketType {
	return PacketTypeLStat
}

func (p *LStatPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeLStat, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *LStatPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = LStatPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *SetstatPacket) Type() PacketType {
	return PacketTypeSetstat
}

func (p *SetstatPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path) + p.Attrs.Len()
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeSetstat, reqid)
	buf.AppendString(p.Path)

	p.Attrs.MarshalInto(buf)

	return buf.Packet(payload)
}

func (p *SetstatPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = SetstatPacket{
		Path: buf.ConsumeString(),
	}

	return p.Attrs.UnmarshalFrom(buf)
}

func (p *RemovePacket) Type() PacketType {
	return PacketTypeRemove
}

func (p *RemovePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeRemove, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *RemovePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = RemovePacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *MkdirPacket) Type() PacketType {
	return PacketTypeMkdir
}

func (p *MkdirPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path) + p.Attrs.Len()
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeMkdir, reqid)
	buf.AppendString(p.Path)

	p.Attrs.MarshalInto(buf)

	return buf.Packet(payload)
}

func (p *MkdirPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = MkdirPacket{
		Path: buf.ConsumeString(),
	}

	return p.Attrs.UnmarshalFrom(buf)
}

func (p *RmdirPacket) Type() PacketType {
	return PacketTypeRmdir
}

func (p *RmdirPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeRmdir, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *RmdirPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = RmdirPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *RealPathPacket) Type() PacketType {
	return PacketTypeRealPath
}

func (p *RealPathPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeRealPath, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *RealPathPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = RealPathPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *StatPacket) Type() PacketType {
	return PacketTypeStat
}

func (p *StatPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeStat, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *StatPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = StatPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *RenamePacket) Type() PacketType {
	return PacketTypeRename
}

func (p *RenamePacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.OldPath) + 4 + len(p.NewPath)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeRename, reqid)
	buf.AppendString(p.OldPath)
	buf.AppendString(p.NewPath)

	return buf.Packet(payload)
}

func (p *RenamePacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = RenamePacket{
		OldPath: buf.ConsumeString(),
		NewPath: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *ReadLinkPacket) Type() PacketType {
	return PacketTypeReadLink
}

func (p *ReadLinkPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.Path)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeReadLink, reqid)
	buf.AppendString(p.Path)

	return buf.Packet(payload)
}

func (p *ReadLinkPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = ReadLinkPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (p *SymlinkPacket) Type() PacketType {
	return PacketTypeSymlink
}

func (p *SymlinkPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	buf := NewBuffer(b)
	if buf.Cap() < 9 {
		size := 4 + len(p.TargetPath) + 4 + len(p.LinkPath)
		buf = NewMarshalBuffer(size)
	}

	buf.StartPacket(PacketTypeSymlink, reqid)

	buf.AppendString(p.TargetPath)
	buf.AppendString(p.LinkPath)

	return buf.Packet(payload)
}

func (p *SymlinkPacket) UnmarshalPacketBody(buf *Buffer) (err error) {
	*p = SymlinkPacket{
		TargetPath: buf.ConsumeString(),
		LinkPath:   buf.ConsumeString(),
	}

	return buf.Err
}
