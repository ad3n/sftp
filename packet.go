package sftp

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
)

const (
	maxMsgLength = 256 * 1024

	debugDumpTxPacket = false

	debugDumpRxPacket = false

	debugDumpTxPacketBytes = false

	debugDumpRxPacketBytes = false

	dataHeaderLen = 4 + 1 + 4 + 4
)

var (
	errLongPacket = errors.New("packet too long")

	errShortPacket = errors.New("packet too short")

	errUnknownExtendedPacket = errors.New("unknown extended packet")
)

type (
	packetMarshaler interface {
		marshalPacket() (header, payload []byte, err error)
	}

	extensionPair struct {
		Name string
		Data string
	}

	sshFxInitPacket struct {
		Extensions []extensionPair
		Version    uint32
	}

	sshFxVersionPacket struct {
		Extensions []sshExtensionPair
		Version    uint32
	}

	sshExtensionPair struct {
		Name, Data string
	}

	sshFxpReaddirPacket struct {
		Handle string
		ID     uint32
	}

	sshFxpOpendirPacket struct {
		Path string
		ID   uint32
	}

	sshFxpLstatPacket struct {
		Path string
		ID   uint32
	}

	sshFxpStatPacket struct {
		Path string
		ID   uint32
	}

	sshFxpFstatPacket struct {
		Handle string
		ID     uint32
	}

	sshFxpClosePacket struct {
		Handle string
		ID     uint32
	}

	sshFxpRemovePacket struct {
		Filename string
		ID       uint32
	}

	sshFxpRmdirPacket struct {
		Path string
		ID   uint32
	}

	sshFxpSymlinkPacket struct {
		Targetpath string
		Linkpath   string
		ID         uint32
	}

	sshFxpHardlinkPacket struct {
		Oldpath string
		Newpath string
		ID      uint32
	}

	sshFxpReadlinkPacket struct {
		Path string
		ID   uint32
	}

	sshFxpRealpathPacket struct {
		Path string
		ID   uint32
	}

	sshFxpNameAttr struct {
		Name     string
		LongName string
		Attrs    []any
	}

	sshFxpNamePacket struct {
		NameAttrs []*sshFxpNameAttr
		ID        uint32
	}

	sshFxpOpenPacket struct {
		Attrs  any
		Path   string
		ID     uint32
		Pflags uint32
		Flags  uint32
	}

	sshFxpReadPacket struct {
		Handle string
		Offset uint64
		ID     uint32
		Len    uint32
	}

	sshFxpRenamePacket struct {
		Oldpath string
		Newpath string
		ID      uint32
	}

	sshFxpPosixRenamePacket struct {
		Oldpath string
		Newpath string
		ID      uint32
	}

	sshFxpWritePacket struct {
		Handle string
		Data   []byte
		Offset uint64
		ID     uint32
		Length uint32
	}

	sshFxpMkdirPacket struct {
		Path  string
		ID    uint32
		Flags uint32
	}

	sshFxpSetstatPacket struct {
		Attrs any
		Path  string
		ID    uint32
		Flags uint32
	}

	sshFxpFsetstatPacket struct {
		Attrs  any
		Handle string
		ID     uint32
		Flags  uint32
	}

	sshFxpHandlePacket struct {
		Handle string
		ID     uint32
	}

	sshFxpStatusPacket struct {
		StatusError
		ID uint32
	}

	sshFxpDataPacket struct {
		Data   []byte
		ID     uint32
		Length uint32
	}

	sshFxpStatvfsPacket struct {
		Path string
		ID   uint32
	}

	StatVFS struct {
		ID      uint32
		Bsize   uint64
		Frsize  uint64
		Blocks  uint64
		Bfree   uint64
		Bavail  uint64
		Files   uint64
		Ffree   uint64
		Favail  uint64
		Fsid    uint64
		Flag    uint64
		Namemax uint64
	}

	sshFxpFsyncPacket struct {
		Handle string
		ID     uint32
	}

	sshFxpExtendedPacket struct {
		SpecificPacket interface {
			serverRespondablePacket
			readonly() bool
		}
		ExtendedRequest string
		ID              uint32
	}

	sshFxpExtendedPacketStatVFS struct {
		ExtendedRequest string
		Path            string
		ID              uint32
	}

	sshFxpExtendedPacketPosixRename struct {
		ExtendedRequest string
		Oldpath         string
		Newpath         string
		ID              uint32
	}

	sshFxpExtendedPacketHardlink struct {
		ExtendedRequest string
		Oldpath         string
		Newpath         string
		ID              uint32
	}
)

func marshalUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func marshalUint64(b []byte, v uint64) []byte {
	return marshalUint32(marshalUint32(b, uint32(v>>32)), uint32(v))
}

func marshalString(b []byte, v string) []byte {
	return append(marshalUint32(b, uint32(len(v))), v...)
}

func marshalFileInfo(b []byte, fi os.FileInfo) []byte {
	flags, fileStat := fileStatFromInfo(fi)

	b = marshalUint32(b, flags)

	return marshalFileStat(b, flags, fileStat)
}

func marshalFileStat(b []byte, flags uint32, fileStat *FileStat) []byte {
	if flags&sshFileXferAttrSize != 0 {
		b = marshalUint64(b, fileStat.Size)
	}

	if flags&sshFileXferAttrUIDGID != 0 {
		b = marshalUint32(b, fileStat.UID)
		b = marshalUint32(b, fileStat.GID)
	}

	if flags&sshFileXferAttrPermissions != 0 {
		b = marshalUint32(b, fileStat.Mode)
	}

	if flags&sshFileXferAttrACmodTime != 0 {
		b = marshalUint32(b, fileStat.Atime)
		b = marshalUint32(b, fileStat.Mtime)
	}

	if flags&sshFileXferAttrExtended != 0 {
		b = marshalUint32(b, uint32(len(fileStat.Extended)))

		for _, attr := range fileStat.Extended {
			b = marshalString(b, attr.ExtType)
			b = marshalString(b, attr.ExtData)
		}
	}

	return b
}

func marshalStatus(b []byte, err StatusError) []byte {
	b = marshalUint32(b, err.Code)
	b = marshalString(b, err.msg)
	b = marshalString(b, err.lang)
	return b
}

func marshal(b []byte, v any) []byte {
	switch v := v.(type) {
	case nil:
		return b
	case uint8:
		return append(b, v)
	case uint32:
		return marshalUint32(b, v)
	case uint64:
		return marshalUint64(b, v)
	case string:
		return marshalString(b, v)
	case []byte:
		return append(b, v...)
	case os.FileInfo:
		return marshalFileInfo(b, v)
	default:
		switch d := reflect.ValueOf(v); d.Kind() {
		case reflect.Struct:
			for i, n := 0, d.NumField(); i < n; i++ {
				b = marshal(b, d.Field(i).Interface())
			}

			return b
		case reflect.Slice:
			for i, n := 0, d.Len(); i < n; i++ {
				b = marshal(b, d.Index(i).Interface())
			}

			return b
		default:
			panic(fmt.Sprintf("marshal(%#v): cannot handle type %T", v, v))
		}
	}
}

func unmarshalUint32(b []byte) (uint32, []byte) {
	v := uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
	return v, b[4:]
}

func unmarshalUint32Safe(b []byte) (uint32, []byte, error) {
	var v uint32
	if len(b) < 4 {
		return 0, nil, errShortPacket
	}

	v, b = unmarshalUint32(b)
	return v, b, nil
}

func unmarshalUint64(b []byte) (uint64, []byte) {
	h, b := unmarshalUint32(b)
	l, b := unmarshalUint32(b)
	return uint64(h)<<32 | uint64(l), b
}

func unmarshalUint64Safe(b []byte) (uint64, []byte, error) {
	var v uint64
	if len(b) < 8 {
		return 0, nil, errShortPacket
	}

	v, b = unmarshalUint64(b)
	return v, b, nil
}

func unmarshalString(b []byte) (string, []byte) {
	n, b := unmarshalUint32(b)
	return string(b[:n]), b[n:]
}

func unmarshalStringSafe(b []byte) (string, []byte, error) {
	n, b, err := unmarshalCount(b)
	if err != nil {
		return "", nil, err
	}

	if n > len(b) {
		return "", nil, errShortPacket
	}

	return string(b[:n]), b[n:], nil
}

func unmarshalAttrs(b []byte) (*FileStat, []byte, error) {
	flags, b, err := unmarshalUint32Safe(b)
	if err != nil {
		return nil, b, err
	}

	return unmarshalFileStat(flags, b)
}

func unmarshalFileStat(flags uint32, b []byte) (*FileStat, []byte, error) {
	var fs FileStat
	var err error

	if flags&sshFileXferAttrSize == sshFileXferAttrSize {
		fs.Size, b, err = unmarshalUint64Safe(b)
		if err != nil {
			return nil, b, err
		}
	}

	if flags&sshFileXferAttrUIDGID == sshFileXferAttrUIDGID {
		fs.UID, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}

		fs.GID, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}
	}

	if flags&sshFileXferAttrPermissions == sshFileXferAttrPermissions {
		fs.Mode, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}
	}

	if flags&sshFileXferAttrACmodTime == sshFileXferAttrACmodTime {
		fs.Atime, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}

		fs.Mtime, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}
	}

	if flags&sshFileXferAttrExtended == sshFileXferAttrExtended {
		var count uint32
		count, b, err = unmarshalUint32Safe(b)
		if err != nil {
			return nil, b, err
		}

		if count > uint32(len(b)/8) {
			return nil, b, errShortPacket
		}

		ext := make([]StatExtended, count)
		for i := uint32(0); i < count; i++ {
			var typ string
			var data string
			typ, b, err = unmarshalStringSafe(b)
			if err != nil {
				return nil, b, err
			}

			data, b, err = unmarshalStringSafe(b)
			if err != nil {
				return nil, b, err
			}

			ext[i] = StatExtended{
				ExtType: typ,
				ExtData: data,
			}
		}

		fs.Extended = ext
	}

	return &fs, b, nil
}

func unmarshalSID(id uint32, b []byte) ([]byte, error) {
	sid, b, err := unmarshalUint32Safe(b)
	if err != nil {
		return nil, err
	}

	if sid != id {
		return nil, &unexpectedIDErr{id, sid}
	}

	return b, nil
}

func unmarshalCount(b []byte) (int, []byte, error) {
	v, b, err := unmarshalUint32Safe(b)
	if err != nil {
		return 0, nil, err
	}

	if int(v) < 0 {
		return 0, nil, errLongPacket
	}

	return int(v), b, nil
}

func unmarshalStatus(id uint32, data []byte) error {
	data, err := unmarshalSID(id, data)
	if err != nil {
		return err
	}

	code, data, err := unmarshalUint32Safe(data)
	if err != nil {
		return err
	}

	msg, data, _ := unmarshalStringSafe(data)
	lang, _, _ := unmarshalStringSafe(data)
	return &StatusError{
		Code: code,
		msg:  msg,
		lang: lang,
	}
}

func unmarshalDataReply(id uint32, data []byte) ([]byte, error) {
	data, err := unmarshalSID(id, data)
	if err != nil {
		return nil, err
	}

	l, data, err := unmarshalCount(data)
	if err != nil {
		return nil, err
	}

	if l > len(data) {
		return nil, errShortPacket
	}

	return data[:l], nil
}

func marshalPacket(m encoding.BinaryMarshaler) (header, payload []byte, err error) {
	if m, ok := m.(packetMarshaler); ok {
		return m.marshalPacket()
	}

	header, err = m.MarshalBinary()
	return
}

func sendPacket(w io.Writer, m encoding.BinaryMarshaler) error {
	header, payload, err := marshalPacket(m)
	if err != nil {
		return fmt.Errorf("binary marshaller failed: %w", err)
	}

	length := len(header) + len(payload) - 4
	switch {
	case debugDumpTxPacketBytes:

		debug("send packet: %s %d bytes %x%x", fxp(header[4]), length, header[5:], payload)

	case debugDumpTxPacket:

		debug("send packet: %s %d bytes", fxp(header[4]), length)
	}

	binary.BigEndian.PutUint32(header[:4], uint32(length))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("failed to send packet payload: %w", err)
		}
	}

	return nil
}

func recvPacket(r io.Reader, alloc *allocator, orderID uint32) (fxp, []byte, error) {
	var b []byte
	switch {
	case alloc != nil:

		b = alloc.GetPage(orderID)

	default:

		b = make([]byte, 4)
	}

	if n, err := io.ReadFull(r, b[:4]); err != nil {
		if err == io.EOF {
			return 0, nil, err
		}

		return 0, nil, fmt.Errorf("error reading packet length: %d of 4: %w", n, err)
	}

	length, _ := unmarshalUint32(b)
	if length > maxMsgLength {
		debug("recv packet %d bytes too long", length)
		return 0, nil, errLongPacket
	}

	if length == 0 {
		debug("recv packet of 0 bytes too short")
		return 0, nil, errShortPacket
	}

	if alloc == nil {
		b = make([]byte, length)
	}

	n, err := io.ReadFull(r, b[:length])
	b = b[:n]

	if err != nil {
		debug("recv packet error: %d of %d bytes: %x", n, length, b)

		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		if n == 0 {
			return 0, nil, fmt.Errorf("error reading packet body: %d of %d: %w", n, length, err)
		}

		return 0, nil, fmt.Errorf("error reading packet body: %d of %d: (%s) %w", n, length, fxp(b[0]), err)
	}

	typ, payload := fxp(b[0]), b[1:n]

	switch {
	case debugDumpRxPacketBytes:

		debug("recv packet: %s %d bytes %x", typ, length, payload)

	case debugDumpRxPacket:

		debug("recv packet: %s %d bytes", typ, length)
	}

	return typ, payload, nil
}

func unmarshalExtensionPair(b []byte) (extensionPair, []byte, error) {
	var ep extensionPair
	var err error
	ep.Name, b, err = unmarshalStringSafe(b)
	if err != nil {
		return ep, b, err
	}

	ep.Data, b, err = unmarshalStringSafe(b)
	return ep, b, err
}

func (p *sshFxInitPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4
	for _, e := range p.Extensions {
		l += 4 + len(e.Name) + 4 + len(e.Data)
	}

	b := make([]byte, 4, l)
	b = append(b, sshFxpInit)
	b = marshalUint32(b, p.Version)

	for _, e := range p.Extensions {
		b = marshalString(b, e.Name)
		b = marshalString(b, e.Data)
	}

	return b, nil
}

func (p *sshFxInitPacket) UnmarshalBinary(b []byte) error {
	var err error
	if p.Version, b, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	for len(b) > 0 {
		var ep extensionPair
		ep, b, err = unmarshalExtensionPair(b)
		if err != nil {
			return err
		}

		p.Extensions = append(p.Extensions, ep)
	}

	return nil
}

func (p *sshFxVersionPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4
	for _, e := range p.Extensions {
		l += 4 + len(e.Name) + 4 + len(e.Data)
	}

	b := make([]byte, 4, l)
	b = append(b, sshFxpVersion)
	b = marshalUint32(b, p.Version)

	for _, e := range p.Extensions {
		b = marshalString(b, e.Name)
		b = marshalString(b, e.Data)
	}

	return b, nil
}

func marshalIDStringPacket(packetType byte, id uint32, str string) ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(str)

	b := make([]byte, 4, l)
	b = append(b, packetType)
	b = marshalUint32(b, id)
	b = marshalString(b, str)

	return b, nil
}

func unmarshalIDString(b []byte, id *uint32, str *string) error {
	var err error
	*id, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	*str, _, err = unmarshalStringSafe(b)
	return err
}

func (p *sshFxpReaddirPacket) id() uint32 { return p.ID }

func (p *sshFxpReaddirPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpReaddir, p.ID, p.Handle)
}

func (p *sshFxpReaddirPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Handle)
}

func (p *sshFxpOpendirPacket) id() uint32 { return p.ID }

func (p *sshFxpOpendirPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpOpendir, p.ID, p.Path)
}

func (p *sshFxpOpendirPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpLstatPacket) id() uint32 { return p.ID }

func (p *sshFxpLstatPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpLstat, p.ID, p.Path)
}

func (p *sshFxpLstatPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpStatPacket) id() uint32 { return p.ID }

func (p *sshFxpStatPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpStat, p.ID, p.Path)
}

func (p *sshFxpStatPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpFstatPacket) id() uint32 { return p.ID }

func (p *sshFxpFstatPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpFstat, p.ID, p.Handle)
}

func (p *sshFxpFstatPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Handle)
}

func (p *sshFxpClosePacket) id() uint32 { return p.ID }

func (p *sshFxpClosePacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpClose, p.ID, p.Handle)
}

func (p *sshFxpClosePacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Handle)
}

func (p *sshFxpRemovePacket) id() uint32 { return p.ID }

func (p *sshFxpRemovePacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpRemove, p.ID, p.Filename)
}

func (p *sshFxpRemovePacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Filename)
}

func (p *sshFxpRmdirPacket) id() uint32 { return p.ID }

func (p *sshFxpRmdirPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpRmdir, p.ID, p.Path)
}

func (p *sshFxpRmdirPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpSymlinkPacket) id() uint32 { return p.ID }

func (p *sshFxpSymlinkPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Targetpath) +
		4 + len(p.Linkpath)

	b := make([]byte, 4, l)
	b = append(b, sshFxpSymlink)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Targetpath)
	b = marshalString(b, p.Linkpath)

	return b, nil
}

func (p *sshFxpSymlinkPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Targetpath, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Linkpath, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpHardlinkPacket) id() uint32 { return p.ID }

func (p *sshFxpHardlinkPacket) MarshalBinary() ([]byte, error) {
	const ext = "hardlink@openssh.com"
	l := 4 + 1 + 4 +
		4 + len(ext) +
		4 + len(p.Oldpath) +
		4 + len(p.Newpath)

	b := make([]byte, 4, l)
	b = append(b, sshFxpExtended)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, ext)
	b = marshalString(b, p.Oldpath)
	b = marshalString(b, p.Newpath)

	return b, nil
}

func (p *sshFxpReadlinkPacket) id() uint32 { return p.ID }

func (p *sshFxpReadlinkPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpReadlink, p.ID, p.Path)
}

func (p *sshFxpReadlinkPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpRealpathPacket) id() uint32 { return p.ID }

func (p *sshFxpRealpathPacket) MarshalBinary() ([]byte, error) {
	return marshalIDStringPacket(sshFxpRealpath, p.ID, p.Path)
}

func (p *sshFxpRealpathPacket) UnmarshalBinary(b []byte) error {
	return unmarshalIDString(b, &p.ID, &p.Path)
}

func (p *sshFxpNameAttr) MarshalBinary() ([]byte, error) {
	var b []byte
	b = marshalString(b, p.Name)
	b = marshalString(b, p.LongName)
	for _, attr := range p.Attrs {
		b = marshal(b, attr)
	}

	return b, nil
}

func (p *sshFxpNamePacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpName)
	b = marshalUint32(b, p.ID)
	b = marshalUint32(b, uint32(len(p.NameAttrs)))

	var payload []byte
	for _, na := range p.NameAttrs {
		ab, err := na.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}

		payload = append(payload, ab...)
	}

	return b, payload, nil
}

func (p *sshFxpNamePacket) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpOpenPacket) id() uint32 { return p.ID }

func (p *sshFxpOpenPacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Path) +
		4 + 4

	b := make([]byte, 4, l)
	b = append(b, sshFxpOpen)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Path)
	b = marshalUint32(b, p.Pflags)
	b = marshalUint32(b, p.Flags)

	switch attrs := p.Attrs.(type) {
	case []byte:
		return b, attrs, nil
	case os.FileInfo:
		_, fs := fileStatFromInfo(attrs)
		return b, marshalFileStat(nil, p.Flags, fs), nil
	case *FileStat:
		return b, marshalFileStat(nil, p.Flags, attrs), nil
	}

	return b, marshal(nil, p.Attrs), nil
}

func (p *sshFxpOpenPacket) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpOpenPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Path, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	p.Pflags, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	if p.Flags, b, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	p.Attrs = b
	return nil
}

func (p *sshFxpOpenPacket) unmarshalFileStat(flags uint32) (*FileStat, error) {
	switch attrs := p.Attrs.(type) {
	case *FileStat:
		return attrs, nil
	case []byte:
		fs, _, err := unmarshalFileStat(flags, attrs)
		return fs, err
	default:
		return nil, fmt.Errorf("invalid type in unmarshalFileStat: %T", attrs)
	}
}

func (p *sshFxpReadPacket) id() uint32 { return p.ID }

func (p *sshFxpReadPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Handle) +
		8 + 4

	b := make([]byte, 4, l)
	b = append(b, sshFxpRead)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Handle)
	b = marshalUint64(b, p.Offset)
	b = marshalUint32(b, p.Len)

	return b, nil
}

func (p *sshFxpReadPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Handle, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	p.Offset, b, err = unmarshalUint64Safe(b)
	if err != nil {
		return err
	}

	if p.Len, _, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpReadPacket) getDataSlice(alloc *allocator, orderID uint32, maxTxPacket uint32) []byte {
	dataLen := min(p.Len, maxTxPacket)

	if alloc != nil {
		return alloc.GetPage(orderID)[:dataLen]
	}

	return make([]byte, dataLen, dataLen+dataHeaderLen)
}

func (p *sshFxpRenamePacket) id() uint32 { return p.ID }

func (p *sshFxpRenamePacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Oldpath) +
		4 + len(p.Newpath)

	b := make([]byte, 4, l)
	b = append(b, sshFxpRename)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Oldpath)
	b = marshalString(b, p.Newpath)

	return b, nil
}

func (p *sshFxpRenamePacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Oldpath, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Newpath, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpPosixRenamePacket) id() uint32 { return p.ID }

func (p *sshFxpPosixRenamePacket) MarshalBinary() ([]byte, error) {
	const ext = "posix-rename@openssh.com"
	l := 4 + 1 + 4 +
		4 + len(ext) +
		4 + len(p.Oldpath) +
		4 + len(p.Newpath)

	b := make([]byte, 4, l)
	b = append(b, sshFxpExtended)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, ext)
	b = marshalString(b, p.Oldpath)
	b = marshalString(b, p.Newpath)

	return b, nil
}

func (p *sshFxpWritePacket) id() uint32 { return p.ID }

func (p *sshFxpWritePacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Handle) +
		8 +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpWrite)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Handle)
	b = marshalUint64(b, p.Offset)
	b = marshalUint32(b, p.Length)

	return b, p.Data, nil
}

func (p *sshFxpWritePacket) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpWritePacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Handle, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	p.Offset, b, err = unmarshalUint64Safe(b)
	if err != nil {
		return err
	}

	p.Length, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	if uint32(len(b)) < p.Length {
		return errShortPacket
	}

	p.Data = b[:p.Length]
	return nil
}

func (p *sshFxpMkdirPacket) id() uint32 { return p.ID }

func (p *sshFxpMkdirPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Path) +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpMkdir)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Path)
	b = marshalUint32(b, p.Flags)

	return b, nil
}

func (p *sshFxpMkdirPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Path, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Flags, _, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpSetstatPacket) id() uint32  { return p.ID }
func (p *sshFxpFsetstatPacket) id() uint32 { return p.ID }

func (p *sshFxpSetstatPacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Path) +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpSetstat)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Path)
	b = marshalUint32(b, p.Flags)

	switch attrs := p.Attrs.(type) {
	case []byte:
		return b, attrs, nil
	case os.FileInfo:
		_, fs := fileStatFromInfo(attrs)
		return b, marshalFileStat(nil, p.Flags, fs), nil
	case *FileStat:
		return b, marshalFileStat(nil, p.Flags, attrs), nil
	}

	return b, marshal(nil, p.Attrs), nil
}

func (p *sshFxpSetstatPacket) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpFsetstatPacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Handle) +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpFsetstat)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Handle)
	b = marshalUint32(b, p.Flags)

	switch attrs := p.Attrs.(type) {
	case []byte:
		return b, attrs, nil
	case os.FileInfo:
		_, fs := fileStatFromInfo(attrs)
		return b, marshalFileStat(nil, p.Flags, fs), nil
	case *FileStat:
		return b, marshalFileStat(nil, p.Flags, attrs), nil
	}

	return b, marshal(nil, p.Attrs), nil
}

func (p *sshFxpFsetstatPacket) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpSetstatPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Path, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Flags, b, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	p.Attrs = b
	return nil
}

func (p *sshFxpSetstatPacket) unmarshalFileStat(flags uint32) (*FileStat, error) {
	switch attrs := p.Attrs.(type) {
	case *FileStat:
		return attrs, nil
	case []byte:
		fs, _, err := unmarshalFileStat(flags, attrs)
		return fs, err
	default:
		return nil, fmt.Errorf("invalid type in unmarshalFileStat: %T", attrs)
	}
}

func (p *sshFxpFsetstatPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Handle, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Flags, b, err = unmarshalUint32Safe(b); err != nil {
		return err
	}

	p.Attrs = b
	return nil
}

func (p *sshFxpFsetstatPacket) unmarshalFileStat(flags uint32) (*FileStat, error) {
	switch attrs := p.Attrs.(type) {
	case *FileStat:
		return attrs, nil
	case []byte:
		fs, _, err := unmarshalFileStat(flags, attrs)
		return fs, err
	default:
		return nil, fmt.Errorf("invalid type in unmarshalFileStat: %T", attrs)
	}
}

func (p *sshFxpHandlePacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Handle)

	b := make([]byte, 4, l)
	b = append(b, sshFxpHandle)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Handle)

	return b, nil
}

func (p *sshFxpStatusPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 +
		4 + len(p.StatusError.msg) +
		4 + len(p.StatusError.lang)

	b := make([]byte, 4, l)
	b = append(b, sshFxpStatus)
	b = marshalUint32(b, p.ID)
	b = marshalStatus(b, p.StatusError)

	return b, nil
}

func (p *sshFxpDataPacket) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4 +
		4

	b := make([]byte, 4, l)
	b = append(b, sshFxpData)
	b = marshalUint32(b, p.ID)
	b = marshalUint32(b, p.Length)

	return b, p.Data, nil
}

func (p *sshFxpDataPacket) MarshalBinary() ([]byte, error) {
	b := append(p.Data, make([]byte, dataHeaderLen)...)
	copy(b[dataHeaderLen:], p.Data[:p.Length])

	b[4] = sshFxpData
	binary.BigEndian.PutUint32(b[5:9], p.ID)
	binary.BigEndian.PutUint32(b[9:13], p.Length)
	return b, nil
}

func (p *sshFxpDataPacket) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.Length, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	if uint32(len(b)) < p.Length {
		return errShortPacket
	}

	p.Data = b[:p.Length]
	return nil
}

func (p *sshFxpStatvfsPacket) id() uint32 { return p.ID }

func (p *sshFxpStatvfsPacket) MarshalBinary() ([]byte, error) {
	const ext = "statvfs@openssh.com"
	l := 4 + 1 + 4 +
		4 + len(ext) +
		4 + len(p.Path)

	b := make([]byte, 4, l)
	b = append(b, sshFxpExtended)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, ext)
	b = marshalString(b, p.Path)

	return b, nil
}

func (p *StatVFS) TotalSpace() uint64 {
	return p.Frsize * p.Blocks
}

func (p *StatVFS) FreeSpace() uint64 {
	return p.Frsize * p.Bfree
}

func (p *StatVFS) marshalPacket() ([]byte, []byte, error) {
	if p == nil {
		return []byte{0, 0, 0, 0, sshFxpExtendedReply}, nil, binary.Write(io.Discard, binary.BigEndian, p)
	}

	b := make([]byte, 5, 5+4+11*8)
	b[4] = sshFxpExtendedReply
	b = marshalUint32(b, p.ID)
	b = marshalUint64(b, p.Bsize)
	b = marshalUint64(b, p.Frsize)
	b = marshalUint64(b, p.Blocks)
	b = marshalUint64(b, p.Bfree)
	b = marshalUint64(b, p.Bavail)
	b = marshalUint64(b, p.Files)
	b = marshalUint64(b, p.Ffree)
	b = marshalUint64(b, p.Favail)
	b = marshalUint64(b, p.Fsid)
	b = marshalUint64(b, p.Flag)
	b = marshalUint64(b, p.Namemax)

	return b[:5], b[5:], nil
}

func (p *StatVFS) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpFsyncPacket) id() uint32 { return p.ID }

func (p *sshFxpFsyncPacket) MarshalBinary() ([]byte, error) {
	const ext = "fsync@openssh.com"
	l := 4 + 1 + 4 +
		4 + len(ext) +
		4 + len(p.Handle)

	b := make([]byte, 4, l)
	b = append(b, sshFxpExtended)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, ext)
	b = marshalString(b, p.Handle)

	return b, nil
}

func (p *sshFxpExtendedPacket) id() uint32 { return p.ID }
func (p *sshFxpExtendedPacket) readonly() bool {
	if p.SpecificPacket == nil {
		return true
	}

	return p.SpecificPacket.readonly()
}

func (p *sshFxpExtendedPacket) respond(svr *Server) responsePacket {
	if p.SpecificPacket == nil {
		return statusFromError(p.ID, nil)
	}

	return p.SpecificPacket.respond(svr)
}

func (p *sshFxpExtendedPacket) UnmarshalBinary(b []byte) error {
	var err error
	bOrig := b

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	if p.ExtendedRequest, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	switch p.ExtendedRequest {
	case "statvfs@openssh.com":
		p.SpecificPacket = &sshFxpExtendedPacketStatVFS{}
	case "posix-rename@openssh.com":
		p.SpecificPacket = &sshFxpExtendedPacketPosixRename{}
	case "hardlink@openssh.com":
		p.SpecificPacket = &sshFxpExtendedPacketHardlink{}
	default:
		return fmt.Errorf("packet type %v: %w", p.SpecificPacket, errUnknownExtendedPacket)
	}

	return p.SpecificPacket.UnmarshalBinary(bOrig)
}

func (p *sshFxpExtendedPacketStatVFS) id() uint32     { return p.ID }
func (p *sshFxpExtendedPacketStatVFS) readonly() bool { return true }
func (p *sshFxpExtendedPacketStatVFS) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.ExtendedRequest, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Path, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpExtendedPacketPosixRename) id() uint32     { return p.ID }
func (p *sshFxpExtendedPacketPosixRename) readonly() bool { return false }
func (p *sshFxpExtendedPacketPosixRename) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.ExtendedRequest, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	p.Oldpath, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Newpath, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpExtendedPacketPosixRename) respond(s *Server) responsePacket {
	err := os.Rename(s.toLocalPath(p.Oldpath), s.toLocalPath(p.Newpath))
	return statusFromError(p.ID, err)
}

func (p *sshFxpExtendedPacketHardlink) id() uint32     { return p.ID }
func (p *sshFxpExtendedPacketHardlink) readonly() bool { return false }
func (p *sshFxpExtendedPacketHardlink) UnmarshalBinary(b []byte) error {
	var err error

	p.ID, b, err = unmarshalUint32Safe(b)
	if err != nil {
		return err
	}

	p.ExtendedRequest, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	p.Oldpath, b, err = unmarshalStringSafe(b)
	if err != nil {
		return err
	}

	if p.Newpath, _, err = unmarshalStringSafe(b); err != nil {
		return err
	}

	return nil
}

func (p *sshFxpExtendedPacketHardlink) respond(s *Server) responsePacket {
	err := os.Link(s.toLocalPath(p.Oldpath), s.toLocalPath(p.Newpath))
	return statusFromError(p.ID, err)
}
