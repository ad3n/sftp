package openssh

import (
	sshfx "github.com/ad3n/sftp/internal/encoding/ssh/filexfer"
)

const extensionHardlink = "hardlink@openssh.com"

type HardlinkExtendedPacket struct {
	OldPath string
	NewPath string
}

func RegisterExtensionHardlink() {
	sshfx.RegisterExtendedPacketType(extensionHardlink, func() sshfx.ExtendedData {
		return new(HardlinkExtendedPacket)
	})
}

func ExtensionHardlink() *sshfx.ExtensionPair {
	return &sshfx.ExtensionPair{
		Name: extensionHardlink,
		Data: "1",
	}
}

func (ep *HardlinkExtendedPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtended
}

func (ep *HardlinkExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedPacket{
		ExtendedRequest: extensionHardlink,

		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *HardlinkExtendedPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendString(ep.OldPath)
	buf.AppendString(ep.NewPath)
}

func (ep *HardlinkExtendedPacket) MarshalBinary() ([]byte, error) {
	size := 4 + len(ep.OldPath) + 4 + len(ep.NewPath)

	buf := sshfx.NewBuffer(make([]byte, 0, size))
	ep.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (ep *HardlinkExtendedPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = HardlinkExtendedPacket{
		OldPath: buf.ConsumeString(),
		NewPath: buf.ConsumeString(),
	}

	return buf.Err
}

func (ep *HardlinkExtendedPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}
