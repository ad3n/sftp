package sshfx

type (
	InitPacket struct {
		Version    uint32
		Extensions []*ExtensionPair
	}

	VersionPacket struct {
		Version    uint32
		Extensions []*ExtensionPair
	}
)

func (p *InitPacket) MarshalBinary() ([]byte, error) {
	size := 1 + 4

	for _, ext := range p.Extensions {
		size += ext.Len()
	}

	b := NewBuffer(make([]byte, 4, 4+size))
	b.AppendUint8(uint8(PacketTypeInit))
	b.AppendUint32(p.Version)

	for _, ext := range p.Extensions {
		ext.MarshalInto(b)
	}

	b.PutLength(size)

	return b.Bytes(), nil
}

func (p *InitPacket) UnmarshalBinary(data []byte) (err error) {
	buf := NewBuffer(data)

	*p = InitPacket{
		Version: buf.ConsumeUint32(),
	}

	for buf.Len() > 0 {
		var ext ExtensionPair
		if err := ext.UnmarshalFrom(buf); err != nil {
			return err
		}

		p.Extensions = append(p.Extensions, &ext)
	}

	return buf.Err
}

func (p *VersionPacket) MarshalBinary() ([]byte, error) {
	size := 1 + 4

	for _, ext := range p.Extensions {
		size += ext.Len()
	}

	b := NewBuffer(make([]byte, 4, 4+size))
	b.AppendUint8(uint8(PacketTypeVersion))
	b.AppendUint32(p.Version)

	for _, ext := range p.Extensions {
		ext.MarshalInto(b)
	}

	b.PutLength(size)

	return b.Bytes(), nil
}

func (p *VersionPacket) UnmarshalBinary(data []byte) (err error) {
	buf := NewBuffer(data)

	*p = VersionPacket{
		Version: buf.ConsumeUint32(),
	}

	for buf.Len() > 0 {
		var ext ExtensionPair
		if err := ext.UnmarshalFrom(buf); err != nil {
			return err
		}

		p.Extensions = append(p.Extensions, &ext)
	}

	return nil
}
