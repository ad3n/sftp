package openssh

import (
	sshfx "github.com/ad3n/sftp/internal/encoding/ssh/filexfer"
)

const (
	extensionStatVFS = "statvfs@openssh.com"

	extensionFStatVFS = "fstatvfs@openssh.com"

	MountFlagsReadOnly = 0x1

	MountFlagsNoSUID = 0x2
)

type (
	StatVFSExtendedPacket struct {
		Path string
	}

	FStatVFSExtendedPacket struct {
		Path string
	}

	StatVFSExtendedReplyPacket struct {
		BlockSize     uint64
		FragmentSize  uint64
		Blocks        uint64
		BlocksFree    uint64
		BlocksAvail   uint64
		Files         uint64
		FilesFree     uint64
		FilesAvail    uint64
		FilesystemID  uint64
		MountFlags    uint64
		MaxNameLength uint64
	}
)

func RegisterExtensionStatVFS() {
	sshfx.RegisterExtendedPacketType(extensionStatVFS, func() sshfx.ExtendedData {
		return new(StatVFSExtendedPacket)
	})
}

func ExtensionStatVFS() *sshfx.ExtensionPair {
	return &sshfx.ExtensionPair{
		Name: extensionStatVFS,
		Data: "2",
	}
}

func (ep *StatVFSExtendedPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtended
}

func (ep *StatVFSExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedPacket{
		ExtendedRequest: extensionStatVFS,

		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *StatVFSExtendedPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendString(ep.Path)
}

func (ep *StatVFSExtendedPacket) MarshalBinary() ([]byte, error) {
	size := 4 + len(ep.Path)

	buf := sshfx.NewBuffer(make([]byte, 0, size))

	ep.MarshalInto(buf)

	return buf.Bytes(), nil
}

func (ep *StatVFSExtendedPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = StatVFSExtendedPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (ep *StatVFSExtendedPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}

func RegisterExtensionFStatVFS() {
	sshfx.RegisterExtendedPacketType(extensionFStatVFS, func() sshfx.ExtendedData {
		return new(FStatVFSExtendedPacket)
	})
}

func ExtensionFStatVFS() *sshfx.ExtensionPair {
	return &sshfx.ExtensionPair{
		Name: extensionFStatVFS,
		Data: "2",
	}
}

func (ep *FStatVFSExtendedPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtended
}

func (ep *FStatVFSExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedPacket{
		ExtendedRequest: extensionFStatVFS,

		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *FStatVFSExtendedPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendString(ep.Path)
}

func (ep *FStatVFSExtendedPacket) MarshalBinary() ([]byte, error) {
	size := 4 + len(ep.Path)

	buf := sshfx.NewBuffer(make([]byte, 0, size))

	ep.MarshalInto(buf)

	return buf.Bytes(), nil
}

func (ep *FStatVFSExtendedPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = FStatVFSExtendedPacket{
		Path: buf.ConsumeString(),
	}

	return buf.Err
}

func (ep *FStatVFSExtendedPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}

func (ep *StatVFSExtendedReplyPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtendedReply
}

func (ep *StatVFSExtendedReplyPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedReplyPacket{
		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *StatVFSExtendedReplyPacket) UnmarshalPacketBody(buf *sshfx.Buffer) (err error) {
	p := &sshfx.ExtendedReplyPacket{
		Data: ep,
	}
	return p.UnmarshalPacketBody(buf)
}

func (ep *StatVFSExtendedReplyPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendUint64(ep.BlockSize)
	buf.AppendUint64(ep.FragmentSize)
	buf.AppendUint64(ep.Blocks)
	buf.AppendUint64(ep.BlocksFree)
	buf.AppendUint64(ep.BlocksAvail)
	buf.AppendUint64(ep.Files)
	buf.AppendUint64(ep.FilesFree)
	buf.AppendUint64(ep.FilesAvail)
	buf.AppendUint64(ep.FilesystemID)
	buf.AppendUint64(ep.MountFlags)
	buf.AppendUint64(ep.MaxNameLength)
}

func (ep *StatVFSExtendedReplyPacket) MarshalBinary() ([]byte, error) {
	size := 11 * 8

	b := sshfx.NewBuffer(make([]byte, 0, size))
	ep.MarshalInto(b)
	return b.Bytes(), nil
}

func (ep *StatVFSExtendedReplyPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = StatVFSExtendedReplyPacket{
		BlockSize:     buf.ConsumeUint64(),
		FragmentSize:  buf.ConsumeUint64(),
		Blocks:        buf.ConsumeUint64(),
		BlocksFree:    buf.ConsumeUint64(),
		BlocksAvail:   buf.ConsumeUint64(),
		Files:         buf.ConsumeUint64(),
		FilesFree:     buf.ConsumeUint64(),
		FilesAvail:    buf.ConsumeUint64(),
		FilesystemID:  buf.ConsumeUint64(),
		MountFlags:    buf.ConsumeUint64(),
		MaxNameLength: buf.ConsumeUint64(),
	}

	return buf.Err
}

func (ep *StatVFSExtendedReplyPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}
