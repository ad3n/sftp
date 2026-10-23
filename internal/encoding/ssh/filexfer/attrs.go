package sshfx

const (
	AttrSize = 1 << iota
	AttrUIDGID
	AttrPermissions
	AttrACModTime

	AttrExtended = 1 << 31
)

type (
	Attributes struct {
		Flags uint32

		Size uint64

		UID uint32
		GID uint32

		Permissions FileMode

		ATime uint32
		MTime uint32

		ExtendedAttributes []ExtendedAttribute
	}

	ExtendedAttribute struct {
		Type string
		Data string
	}

	NameEntry struct {
		Filename string
		Longname string
		Attrs    Attributes
	}
)

func (a *Attributes) GetSize() (size uint64, ok bool) {
	return a.Size, a.Flags&AttrSize != 0
}

func (a *Attributes) SetSize(size uint64) {
	a.Flags |= AttrSize
	a.Size = size
}

func (a *Attributes) GetUIDGID() (uid, gid uint32, ok bool) {
	return a.UID, a.GID, a.Flags&AttrUIDGID != 0
}

func (a *Attributes) SetUIDGID(uid, gid uint32) {
	a.Flags |= AttrUIDGID
	a.UID = uid
	a.GID = gid
}

func (a *Attributes) GetPermissions() (perms FileMode, ok bool) {
	return a.Permissions, a.Flags&AttrPermissions != 0
}

func (a *Attributes) SetPermissions(perms FileMode) {
	a.Flags |= AttrPermissions
	a.Permissions = perms
}

func (a *Attributes) GetACModTime() (atime, mtime uint32, ok bool) {
	return a.ATime, a.MTime, a.Flags&AttrACModTime != 0
}

func (a *Attributes) SetACModTime(atime, mtime uint32) {
	a.Flags |= AttrACModTime
	a.ATime = atime
	a.MTime = mtime
}

func (a *Attributes) Len() int {
	length := 4

	if a.Flags&AttrSize != 0 {
		length += 8
	}

	if a.Flags&AttrUIDGID != 0 {
		length += 4 + 4
	}

	if a.Flags&AttrPermissions != 0 {
		length += 4
	}

	if a.Flags&AttrACModTime != 0 {
		length += 4 + 4
	}

	if a.Flags&AttrExtended != 0 {
		length += 4

		for _, ext := range a.ExtendedAttributes {
			length += ext.Len()
		}
	}

	return length
}

func (a *Attributes) MarshalInto(buf *Buffer) {
	buf.AppendUint32(a.Flags)

	if a.Flags&AttrSize != 0 {
		buf.AppendUint64(a.Size)
	}

	if a.Flags&AttrUIDGID != 0 {
		buf.AppendUint32(a.UID)
		buf.AppendUint32(a.GID)
	}

	if a.Flags&AttrPermissions != 0 {
		buf.AppendUint32(uint32(a.Permissions))
	}

	if a.Flags&AttrACModTime != 0 {
		buf.AppendUint32(a.ATime)
		buf.AppendUint32(a.MTime)
	}

	if a.Flags&AttrExtended != 0 {
		buf.AppendUint32(uint32(len(a.ExtendedAttributes)))

		for _, ext := range a.ExtendedAttributes {
			ext.MarshalInto(buf)
		}
	}
}

func (a *Attributes) MarshalBinary() ([]byte, error) {
	buf := NewBuffer(make([]byte, 0, a.Len()))
	a.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (a *Attributes) UnmarshalFrom(buf *Buffer) (err error) {
	flags := buf.ConsumeUint32()

	return a.XXX_UnmarshalByFlags(flags, buf)
}

func (a *Attributes) XXX_UnmarshalByFlags(flags uint32, buf *Buffer) (err error) {
	a.Flags = flags

	if a.Flags == 0 {
		return buf.Err
	}

	if a.Flags&AttrSize != 0 {
		a.Size = buf.ConsumeUint64()
	}

	if a.Flags&AttrUIDGID != 0 {
		a.UID = buf.ConsumeUint32()
		a.GID = buf.ConsumeUint32()
	}

	if a.Flags&AttrPermissions != 0 {
		a.Permissions = FileMode(buf.ConsumeUint32())
	}

	if a.Flags&AttrACModTime != 0 {
		a.ATime = buf.ConsumeUint32()
		a.MTime = buf.ConsumeUint32()
	}

	if a.Flags&AttrExtended != 0 {
		count := buf.ConsumeCount()

		a.ExtendedAttributes = make([]ExtendedAttribute, count)
		for i := range a.ExtendedAttributes {
			a.ExtendedAttributes[i].UnmarshalFrom(buf)
		}
	}

	return buf.Err
}

func (a *Attributes) UnmarshalBinary(data []byte) error {
	return a.UnmarshalFrom(NewBuffer(data))
}

func (e *ExtendedAttribute) Len() int {
	return 4 + len(e.Type) + 4 + len(e.Data)
}

func (e *ExtendedAttribute) MarshalInto(buf *Buffer) {
	buf.AppendString(e.Type)
	buf.AppendString(e.Data)
}

func (e *ExtendedAttribute) MarshalBinary() ([]byte, error) {
	buf := NewBuffer(make([]byte, 0, e.Len()))
	e.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (e *ExtendedAttribute) UnmarshalFrom(buf *Buffer) (err error) {
	*e = ExtendedAttribute{
		Type: buf.ConsumeString(),
		Data: buf.ConsumeString(),
	}

	return buf.Err
}

func (e *ExtendedAttribute) UnmarshalBinary(data []byte) error {
	return e.UnmarshalFrom(NewBuffer(data))
}

func (e *NameEntry) Len() int {
	return 4 + len(e.Filename) + 4 + len(e.Longname) + e.Attrs.Len()
}

func (e *NameEntry) MarshalInto(buf *Buffer) {
	buf.AppendString(e.Filename)
	buf.AppendString(e.Longname)

	e.Attrs.MarshalInto(buf)
}

func (e *NameEntry) MarshalBinary() ([]byte, error) {
	buf := NewBuffer(make([]byte, 0, e.Len()))
	e.MarshalInto(buf)
	return buf.Bytes(), nil
}

func (e *NameEntry) UnmarshalFrom(buf *Buffer) (err error) {
	*e = NameEntry{
		Filename: buf.ConsumeString(),
		Longname: buf.ConsumeString(),
	}

	return e.Attrs.UnmarshalFrom(buf)
}

func (e *NameEntry) UnmarshalBinary(data []byte) error {
	return e.UnmarshalFrom(NewBuffer(data))
}
