package openssh

import (
	sshfx "github.com/ad3n/sftp/internal/encoding/ssh/filexfer"
)

const extensionFSync = "fsync@openssh.com"

type FSyncExtendedPacket struct {
	Handle string
}

func RegisterExtensionFSync() {
	sshfx.RegisterExtendedPacketType(extensionFSync, func() sshfx.ExtendedData {
		return new(FSyncExtendedPacket)
	})
}

func ExtensionFSync() *sshfx.ExtensionPair {
	return &sshfx.ExtensionPair{
		Name: extensionFSync,
		Data: "1",
	}
}

func (ep *FSyncExtendedPacket) Type() sshfx.PacketType {
	return sshfx.PacketTypeExtended
}

func (ep *FSyncExtendedPacket) MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error) {
	p := &sshfx.ExtendedPacket{
		ExtendedRequest: extensionFSync,

		Data: ep,
	}
	return p.MarshalPacket(reqid, b)
}

func (ep *FSyncExtendedPacket) MarshalInto(buf *sshfx.Buffer) {
	buf.AppendString(ep.Handle)
}

func (ep *FSyncExtendedPacket) MarshalBinary() ([]byte, error) {
	size := 4 + len(ep.Handle)

	buf := sshfx.NewBuffer(make([]byte, 0, size))
	ep.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (ep *FSyncExtendedPacket) UnmarshalFrom(buf *sshfx.Buffer) (err error) {
	*ep = FSyncExtendedPacket{
		Handle: buf.ConsumeString(),
	}

	return buf.Err
}

func (ep *FSyncExtendedPacket) UnmarshalBinary(data []byte) (err error) {
	return ep.UnmarshalFrom(sshfx.NewBuffer(data))
}
