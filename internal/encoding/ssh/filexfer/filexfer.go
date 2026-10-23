package sshfx

const (
	DefaultMaxPacketLength = 34000

	DefaultMaxDataLength = 32768
)

type (
	PacketMarshaller interface {
		MarshalPacket(reqid uint32, b []byte) (header, payload []byte, err error)
	}

	Packet interface {
		PacketMarshaller

		Type() PacketType

		UnmarshalPacketBody(buf *Buffer) error
	}
)

func ComposePacket(header, payload []byte, err error) ([]byte, error) {
	return append(header, payload...), err
}
