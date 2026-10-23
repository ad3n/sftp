package sshfx

type ExtensionPair struct {
	Name string
	Data string
}

func (e *ExtensionPair) Len() int {
	return 4 + len(e.Name) + 4 + len(e.Data)
}

func (e *ExtensionPair) MarshalInto(buf *Buffer) {
	buf.AppendString(e.Name)
	buf.AppendString(e.Data)
}

func (e *ExtensionPair) MarshalBinary() ([]byte, error) {
	buf := NewBuffer(make([]byte, 0, e.Len()))
	e.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (e *ExtensionPair) UnmarshalFrom(buf *Buffer) (err error) {
	*e = ExtensionPair{
		Name: buf.ConsumeString(),
		Data: buf.ConsumeString(),
	}

	return buf.Err
}

func (e *ExtensionPair) UnmarshalBinary(data []byte) error {
	return e.UnmarshalFrom(NewBuffer(data))
}
