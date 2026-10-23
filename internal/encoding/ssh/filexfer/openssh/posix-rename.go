package openssh

import (
	sshfx "github.com/ad3n/sftp/internal/encoding/ssh/filexfer"
)

const extensionPOSIXRename = "posix-rename@openssh.com"

type POSIXRenameExtendedPacket struct {
	OldPath string
	NewPath string
}

func RegisterExtensionPOSIXRename() {
	sshfx.RegisterExtendedPacketType(extensionPOSIXRename, func() sshfx.ExtendedData {
		return new(POSIXRenameExtendedPacket)
	})
}

func ExtensionPOSIXRename() *sshfx.ExtensionPair {
	return &sshfx.ExtensionPair{
		Name: extensionPOSIXRename,
		Data: "1",
	}
}

func (ep *POSIXRenameExtendedPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtended
}

func (ep *POSIXRenameExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedPacket{
		ExtendedRequest: extensionPOSIXRename,

		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *POSIXRenameExtendedPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendString(ep.OldPath)
	buf.AppendString(ep.NewPath)
}

func (ep *POSIXRenameExtendedPacket) MarshalBinary() ([]byte, error) {
	size := 4 + len(ep.OldPath) + 4 + len(ep.NewPath)

	buf := sshfx.NewBuffer(make([]byte, 0, size))
	ep.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (ep *POSIXRenameExtendedPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = POSIXRenameExtendedPacket{
		OldPath: buf.ConsumeString(),
		NewPath: buf.ConsumeString(),
	}

	return buf.Err
}

func (ep *POSIXRenameExtendedPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}
