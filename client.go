package sftp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kr/fs"
	"golang.org/x/crypto/ssh"

	"github.com/ad3n/sftp/internal/encoding/ssh/filexfer/openssh"
)

const sftpProtocolVersion = 3

var (
	ErrInternalInconsistency = errors.New("internal inconsistency")

	InternalInconsistency = ErrInternalInconsistency
)

type (
	ClientOption func(*Client) error

	Client struct {
		clientConn

		stderrTo io.Writer

		ext map[string]string

		maxPacket             int
		maxConcurrentRequests int
		resPool               resChanPool
		nextid                uint32

		useConcurrentWrites    bool
		useFstat               bool
		disableConcurrentReads bool
	}

	File struct {
		c    *Client
		path string

		mu     sync.RWMutex
		handle string
		offset int64
	}
)

func MaxPacketChecked(size int) ClientOption {
	return func(c *Client) error {
		if size < 1 {
			return errors.New("size must be greater or equal to 1")
		}

		if size > 32768 {
			return errors.New("sizes larger than 32KB might not work with all servers")
		}

		c.maxPacket = size
		return nil
	}
}

func MaxPacketUnchecked(size int) ClientOption {
	return func(c *Client) error {
		if size < 1 {
			return errors.New("size must be greater or equal to 1")
		}

		c.maxPacket = size
		return nil
	}
}

func MaxPacket(size int) ClientOption {
	return MaxPacketChecked(size)
}

func MaxConcurrentRequestsPerFile(n int) ClientOption {
	return func(c *Client) error {
		if n < 1 {
			return errors.New("n must be greater or equal to 1")
		}

		c.maxConcurrentRequests = n
		return nil
	}
}

func UseConcurrentWrites(value bool) ClientOption {
	return func(c *Client) error {
		c.useConcurrentWrites = value
		return nil
	}
}

func UseConcurrentReads(value bool) ClientOption {
	return func(c *Client) error {
		c.disableConcurrentReads = !value
		return nil
	}
}

func UseFstat(value bool) ClientOption {
	return func(c *Client) error {
		c.useFstat = value
		return nil
	}
}

func CopyStderrTo(wr io.Writer) ClientOption {
	return func(c *Client) error {
		c.stderrTo = wr
		return nil
	}
}

func NewClient(conn *ssh.Client, opts ...ClientOption) (*Client, error) {
	s, err := conn.NewSession()
	if err != nil {
		return nil, err
	}

	pw, err := s.StdinPipe()
	if err != nil {
		return nil, err
	}

	pr, err := s.StdoutPipe()
	if err != nil {
		return nil, err
	}

	perr, err := s.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err := s.RequestSubsystem("sftp"); err != nil {
		return nil, err
	}

	return newClientPipe(pr, perr, pw, s.Wait, opts...)
}

func NewClientPipe(rd io.Reader, wr io.WriteCloser, opts ...ClientOption) (*Client, error) {
	return newClientPipe(rd, nil, wr, nil, opts...)
}

func newClientPipe(rd, stderr io.Reader, wr io.WriteCloser, wait func() error, opts ...ClientOption) (*Client, error) {
	c := &Client{
		Reader:      rd,
		WriteCloser: wr,
		inflight:    make(map[uint32]chan<- result),
		closed:      make(chan struct{}),
		wait:        wait,

		ext: make(map[string]string),

		maxPacket:             1 << 15,
		maxConcurrentRequests: 64,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			wr.Close()
			return nil, err
		}
	}

	c.resPool = newResChanPool(c.maxConcurrentRequests)

	if stderr != nil {
		wr := io.Discard
		if c.stderrTo != nil {
			wr = c.stderrTo
		}

		go func() {
			if _, err := io.Copy(wr, stderr); err != nil {
				debug("error copying stderr: %v", err)
			}
		}()
	}

	if err := c.sendInit(); err != nil {
		wr.Close()
		return nil, fmt.Errorf("error sending init packet to server: %w", err)
	}

	if err := c.recvVersion(); err != nil {
		wr.Close()
		return nil, fmt.Errorf("error receiving version packet from server: %w", err)
	}

	c.clientConn.wg.Go(func() {
		if err := c.clientConn.recv(); err != nil {
			c.clientConn.broadcastErr(err)
		}
	})

	return c, nil
}

func (c *Client) Create(path string) (*File, error) {
	return c.open(path, toPflags(os.O_RDWR|os.O_CREATE|os.O_TRUNC))
}

func (c *Client) sendInit() error {
	return c.clientConn.conn.sendPacket(&sshFxInitPacket{
		Version: sftpProtocolVersion,
	})
}

func (c *Client) nextID() uint32 {
	return atomic.AddUint32(&c.nextid, 1)
}

func (c *Client) recvVersion() error {
	typ, data, err := c.recvPacket(0)
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("server unexpectedly closed connection: %w", io.ErrUnexpectedEOF)
		}

		return err
	}

	if typ != sshFxpVersion {
		return &unexpectedPacketErr{sshFxpVersion, typ}
	}

	version, data, err := unmarshalUint32Safe(data)
	if err != nil {
		return err
	}

	if version != sftpProtocolVersion {
		return &unexpectedVersionErr{sftpProtocolVersion, version}
	}

	for len(data) > 0 {
		var ext extensionPair
		ext, data, err = unmarshalExtensionPair(data)
		if err != nil {
			return err
		}

		c.ext[ext.Name] = ext.Data
	}

	return nil
}

func (c *Client) HasExtension(name string) (string, bool) {
	data, ok := c.ext[name]
	return data, ok
}

func (c *Client) Walk(root string) *fs.Walker {
	return fs.WalkFS(root, c)
}

func (c *Client) ReadDir(p string) ([]os.FileInfo, error) {
	return c.ReadDirContext(context.Background(), p)
}

func (c *Client) ReadDirContext(ctx context.Context, p string) ([]os.FileInfo, error) {
	handle, err := c.opendir(ctx, p)
	if err != nil {
		return nil, err
	}
	defer c.close(handle)

	var entries []os.FileInfo
	var done = false
	for !done {
		id := c.nextID()
		typ, data, err1 := c.sendPacket(ctx, nil, &sshFxpReaddirPacket{
			ID:     id,
			Handle: handle,
		})
		if err1 != nil {
			err = err1
			done = true
			break
		}

		switch typ {
		case sshFxpName:
			data, err = unmarshalSID(id, data)
			if err != nil {
				return nil, err
			}

			var count int
			count, data, err = unmarshalCount(data)
			if err != nil {
				return nil, err
			}

			for range count {
				var filename string
				filename, data, err = unmarshalStringSafe(data)
				if err != nil {
					return nil, err
				}

				_, data, err = unmarshalStringSafe(data)
				if err != nil {
					return nil, err
				}

				var attr *FileStat
				attr, data, err = unmarshalAttrs(data)
				if err != nil {
					return nil, err
				}

				if filename == "." || filename == ".." {
					continue
				}

				entries = append(entries, fileInfoFromStat(attr, path.Base(filename)))
			}
		case sshFxpStatus:

			err = normaliseError(unmarshalStatus(id, data))
			done = true
		default:
			return nil, unimplementedPacketErr(typ)
		}
	}

	if err == io.EOF {
		err = nil
	}

	return entries, err
}

func (c *Client) opendir(ctx context.Context, path string) (string, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(ctx, nil, &sshFxpOpendirPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return "", err
	}

	switch typ {
	case sshFxpHandle:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return "", err
		}

		handle, _, err := unmarshalStringSafe(data)
		if err != nil {
			return "", err
		}

		return handle, nil
	case sshFxpStatus:
		return "", normaliseError(unmarshalStatus(id, data))
	default:
		return "", unimplementedPacketErr(typ)
	}
}

func (c *Client) Stat(p string) (os.FileInfo, error) {
	fs, err := c.stat(p)
	if err != nil {
		return nil, err
	}

	return fileInfoFromStat(fs, path.Base(p)), nil
}

func (c *Client) Lstat(p string) (os.FileInfo, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpLstatPacket{
		ID:   id,
		Path: p,
	})
	if err != nil {
		return nil, err
	}

	switch typ {
	case sshFxpAttrs:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return nil, err
		}

		attr, _, err := unmarshalAttrs(data)
		if err != nil {
			return nil, err
		}

		return fileInfoFromStat(attr, path.Base(p)), nil
	case sshFxpStatus:
		return nil, normaliseError(unmarshalStatus(id, data))
	default:
		return nil, unimplementedPacketErr(typ)
	}
}

func (c *Client) ReadLink(p string) (string, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpReadlinkPacket{
		ID:   id,
		Path: p,
	})
	if err != nil {
		return "", err
	}

	switch typ {
	case sshFxpName:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return "", err
		}

		count, data, err := unmarshalCount(data)
		if err != nil {
			return "", err
		}

		if count != 1 {
			return "", unexpectedCount(1, uint32(count))
		}

		filename, _, err := unmarshalStringSafe(data)
		if err != nil {
			return "", err
		}

		return filename, nil
	case sshFxpStatus:
		return "", normaliseError(unmarshalStatus(id, data))
	default:
		return "", unimplementedPacketErr(typ)
	}
}

func (c *Client) Link(oldname, newname string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpHardlinkPacket{
		ID:      id,
		Oldpath: oldname,
		Newpath: newname,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) Symlink(oldname, newname string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpSymlinkPacket{
		ID:         id,
		Linkpath:   newname,
		Targetpath: oldname,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) fsetstat(handle string, flags uint32, attrs any) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpFsetstatPacket{
		ID:     id,
		Handle: handle,
		Flags:  flags,
		Attrs:  attrs,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) setstat(path string, flags uint32, attrs any) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpSetstatPacket{
		ID:    id,
		Path:  path,
		Flags: flags,
		Attrs: attrs,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) Chtimes(path string, atime time.Time, mtime time.Time) error {
	type times struct {
		Atime uint32
		Mtime uint32
	}
	attrs := times{uint32(atime.Unix()), uint32(mtime.Unix())}
	return c.setstat(path, sshFileXferAttrACmodTime, attrs)
}

func (c *Client) Chown(path string, uid, gid int) error {
	type owner struct {
		UID uint32
		GID uint32
	}
	attrs := owner{uint32(uid), uint32(gid)}
	return c.setstat(path, sshFileXferAttrUIDGID, attrs)
}

func (c *Client) Chmod(path string, mode os.FileMode) error {
	return c.setstat(path, sshFileXferAttrPermissions, toChmodPerm(mode))
}

func (c *Client) Truncate(path string, size int64) error {
	return c.setstat(path, sshFileXferAttrSize, uint64(size))
}

func (c *Client) SetExtendedData(path string, extended []StatExtended) error {
	attrs := &FileStat{
		Extended: extended,
	}
	return c.setstat(path, sshFileXferAttrExtended, attrs)
}

func (c *Client) Open(path string) (*File, error) {
	return c.open(path, toPflags(os.O_RDONLY))
}

func (c *Client) OpenFile(path string, f int) (*File, error) {
	return c.open(path, toPflags(f))
}

func (c *Client) open(path string, pflags uint32) (*File, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpOpenPacket{
		ID:     id,
		Path:   path,
		Pflags: pflags,
	})
	if err != nil {
		return nil, err
	}

	switch typ {
	case sshFxpHandle:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return nil, err
		}

		handle, _, err := unmarshalStringSafe(data)
		if err != nil {
			return nil, err
		}

		return &File{c: c, path: path, handle: handle}, nil
	case sshFxpStatus:
		return nil, normaliseError(unmarshalStatus(id, data))
	default:
		return nil, unimplementedPacketErr(typ)
	}
}

func (c *Client) close(handle string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpClosePacket{
		ID:     id,
		Handle: handle,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) stat(path string) (*FileStat, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpStatPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	switch typ {
	case sshFxpAttrs:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return nil, err
		}

		attr, _, err := unmarshalAttrs(data)
		return attr, err
	case sshFxpStatus:
		return nil, normaliseError(unmarshalStatus(id, data))
	default:
		return nil, unimplementedPacketErr(typ)
	}
}

func (c *Client) fstat(handle string) (*FileStat, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpFstatPacket{
		ID:     id,
		Handle: handle,
	})
	if err != nil {
		return nil, err
	}

	switch typ {
	case sshFxpAttrs:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return nil, err
		}

		attr, _, err := unmarshalAttrs(data)
		return attr, err
	case sshFxpStatus:
		return nil, normaliseError(unmarshalStatus(id, data))
	default:
		return nil, unimplementedPacketErr(typ)
	}
}

func (c *Client) StatVFS(path string) (*StatVFS, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpStatvfsPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	switch typ {
	case sshFxpExtendedReply:
		var response StatVFS
		err = binary.Read(bytes.NewReader(data), binary.BigEndian, &response)
		if err != nil {
			return nil, errors.New("can not parse reply")
		}

		return &response, nil

	case sshFxpStatus:
		return nil, normaliseError(unmarshalStatus(id, data))

	default:
		return nil, unimplementedPacketErr(typ)
	}
}

func (c *Client) Join(elem ...string) string { return path.Join(elem...) }

func (c *Client) Remove(path string) error {
	errF := c.removeFile(path)
	if errF == nil {
		return nil
	}

	errD := c.RemoveDirectory(path)
	if errD == nil {
		return nil
	}

	if errF, ok := errF.(*os.PathError); ok {
		if errD, ok := errD.(*os.PathError); ok && errors.Is(errF.Err, errD.Err) {
			return errF
		}
	}

	fi, err := c.Stat(path)
	if err != nil {
		return err
	}

	if fi.IsDir() {
		return errD
	}

	return errF
}

func (c *Client) removeFile(path string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpRemovePacket{
		ID:       id,
		Filename: path,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		err = normaliseError(unmarshalStatus(id, data))
		if err == nil {
			return nil
		}

		return &os.PathError{
			Op:   "remove",
			Path: path,
			Err:  err,
		}
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) RemoveDirectory(path string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpRmdirPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		err = normaliseError(unmarshalStatus(id, data))
		if err == nil {
			return nil
		}

		return &os.PathError{
			Op:   "remove",
			Path: path,
			Err:  err,
		}
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) Rename(oldname, newname string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpRenamePacket{
		ID:      id,
		Oldpath: oldname,
		Newpath: newname,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) PosixRename(oldname, newname string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpPosixRenamePacket{
		ID:      id,
		Oldpath: oldname,
		Newpath: newname,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) RealPath(path string) (string, error) {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpRealpathPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return "", err
	}

	switch typ {
	case sshFxpName:
		data, err := unmarshalSID(id, data)
		if err != nil {
			return "", err
		}

		count, data, err := unmarshalCount(data)
		if err != nil {
			return "", err
		}

		if count != 1 {
			return "", unexpectedCount(1, uint32(count))
		}

		filename, _, err := unmarshalStringSafe(data)
		if err != nil {
			return "", err
		}

		return filename, nil
	case sshFxpStatus:
		return "", normaliseError(unmarshalStatus(id, data))
	default:
		return "", unimplementedPacketErr(typ)
	}
}

func (c *Client) Getwd() (string, error) {
	return c.RealPath(".")
}

func (c *Client) Mkdir(path string) error {
	id := c.nextID()
	typ, data, err := c.sendPacket(context.Background(), nil, &sshFxpMkdirPacket{
		ID:   id,
		Path: path,
	})
	if err != nil {
		return err
	}

	switch typ {
	case sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return unimplementedPacketErr(typ)
	}
}

func (c *Client) MkdirAll(path string) error {
	dir, err := c.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}

		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	i := len(path)
	for i > 0 && path[i-1] == '/' {
		i--
	}

	j := i
	for j > 0 && path[j-1] != '/' {
		j--
	}

	if j > 1 {
		err = c.MkdirAll(path[0 : j-1])
		if err != nil {
			return err
		}
	}

	err = c.Mkdir(path)
	if err != nil {
		dir, err1 := c.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}

		return err
	}

	return nil
}

func (c *Client) RemoveAll(path string) error {
	fi, err := c.Stat(path)
	if err != nil {
		return err
	}

	if fi.IsDir() {
		files, err := c.ReadDir(path)
		if err != nil {
			return err
		}

		for _, file := range files {
			switch {
			case file.IsDir():

				err = c.RemoveAll(path + "/" + file.Name())
				if err != nil {
					return err
				}

			default:

				err = c.Remove(path + "/" + file.Name())
				if err != nil {
					return err
				}
			}
		}
	}

	return c.Remove(path)
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	handle := f.handle
	f.handle = ""

	return f.c.close(handle)
}

func (f *File) Name() string {
	return f.path
}

func (f *File) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.readAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *File) readChunkAt(ch chan result, b []byte, off int64) (n int, err error) {
	for err == nil && n < len(b) {
		id := f.c.nextID()
		typ, data, err := f.c.sendPacket(context.Background(), ch, &sshFxpReadPacket{
			ID:     id,
			Handle: f.handle,
			Offset: uint64(off) + uint64(n),
			Len:    uint32(len(b) - n),
		})
		if err != nil {
			return n, err
		}

		switch typ {
		case sshFxpStatus:
			return n, normaliseError(unmarshalStatus(id, data))

		case sshFxpData:
			data, err = unmarshalDataReply(id, data)
			if err != nil {
				return n, err
			}

			n += copy(b[n:], data)

		default:
			return n, unimplementedPacketErr(typ)
		}
	}

	return
}

func (f *File) readAtSequential(b []byte, off int64) (read int, err error) {
	for read < len(b) {
		rb := b[read:]
		if len(rb) > f.c.maxPacket {
			rb = rb[:f.c.maxPacket]
		}

		n, err := f.readChunkAt(nil, rb, off+int64(read))
		if n < 0 {
			panic("sftp.File: returned negative count from readChunkAt")
		}

		if n > 0 {
			read += n
		}

		if err != nil {
			return read, err
		}
	}

	return read, nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.readAt(b, off)
}

func (f *File) readAt(b []byte, off int64) (int, error) {
	if f.handle == "" {
		return 0, os.ErrClosed
	}

	if len(b) <= f.c.maxPacket {
		return f.readChunkAt(nil, b, off)
	}

	if f.c.disableConcurrentReads {
		return f.readAtSequential(b, off)
	}

	cancel := make(chan struct{})

	concurrency := len(b)/f.c.maxPacket + 1
	if concurrency > f.c.maxConcurrentRequests || concurrency < 1 {
		concurrency = f.c.maxConcurrentRequests
	}

	type work struct {
		id  uint32
		res chan result

		b   []byte
		off int64
	}
	workCh := make(chan work)

	go func() {
		defer close(workCh)

		b := b
		offset := off
		chunkSize := f.c.maxPacket

		for len(b) > 0 {
			rb := b
			if len(rb) > chunkSize {
				rb = rb[:chunkSize]
			}

			id := f.c.nextID()
			res := f.c.resPool.Get()

			f.c.dispatchRequest(res, &sshFxpReadPacket{
				ID:     id,
				Handle: f.handle,
				Offset: uint64(offset),
				Len:    uint32(len(rb)),
			})

			select {
			case workCh <- work{id, res, rb, offset}:
			case <-cancel:
				return
			}

			offset += int64(len(rb))
			b = b[len(rb):]
		}
	}()

	type rErr struct {
		off int64
		err error
	}
	errCh := make(chan rErr)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for packet := range workCh {
				var n int

				s := <-packet.res
				f.c.resPool.Put(packet.res)

				err := s.err
				if err == nil {
					switch s.typ {
					case sshFxpStatus:
						err = normaliseError(unmarshalStatus(packet.id, s.data))

					case sshFxpData:
						var data []byte
						data, err = unmarshalDataReply(packet.id, s.data)
						if err == nil {
							n = copy(packet.b, data)

							if n < len(packet.b) {
								err = io.EOF
							}
						}

					default:
						err = unimplementedPacketErr(s.typ)
					}
				}

				if err != nil {
					errCh <- rErr{packet.off + int64(n), err}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	firstErr := rErr{math.MaxInt64, nil}
	for rErr := range errCh {
		if rErr.off <= firstErr.off {
			firstErr = rErr
		}

		select {
		case <-cancel:
		default:

			close(cancel)
		}
	}

	if firstErr.err != nil {
		return int(firstErr.off - off), firstErr.err
	}

	return len(b), nil
}

func (f *File) writeToSequential(w io.Writer) (written int64, err error) {
	b := make([]byte, f.c.maxPacket)
	ch := make(chan result, 1)

	for {
		n, err := f.readChunkAt(ch, b, f.offset)
		if n < 0 {
			panic("sftp.File: returned negative count from readChunkAt")
		}

		if n > 0 {
			f.offset += int64(n)

			m, err := w.Write(b[:n])
			written += int64(m)

			if err != nil {
				return written, err
			}
		}

		if err != nil {
			if err == io.EOF {
				return written, nil
			}

			return written, err
		}
	}
}

func (f *File) WriteTo(w io.Writer) (written int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return 0, os.ErrClosed
	}

	if f.c.disableConcurrentReads {
		return f.writeToSequential(w)
	}

	var fileStat *FileStat
	switch {
	case f.c.useFstat:

		fileStat, err = f.c.fstat(f.handle)

	default:

		fileStat, err = f.c.stat(f.path)
	}

	if err != nil {
		return 0, err
	}

	fileSize := fileStat.Size
	if fileSize <= uint64(f.c.maxPacket) || !isRegular(fileStat.Mode) {
		return f.writeToSequential(w)
	}

	concurrency64 := fileSize/uint64(f.c.maxPacket) + 1
	if concurrency64 > uint64(f.c.maxConcurrentRequests) || concurrency64 < 1 {
		concurrency64 = uint64(f.c.maxConcurrentRequests)
	}

	concurrency := int(concurrency64)

	chunkSize := f.c.maxPacket
	pool := newBufPool(concurrency, chunkSize)
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(cancel)

		wg.Wait()
	}()

	type writeWork struct {
		err error

		next chan writeWork
		b    []byte
		off  int64
	}
	writeCh := make(chan writeWork)

	type readWork struct {
		res chan result

		cur, next chan writeWork
		off       int64

		id uint32
	}
	readCh := make(chan readWork)

	go func() {
		defer close(readCh)

		off := f.offset

		cur := writeCh
		for {
			id := f.c.nextID()
			res := f.c.resPool.Get()

			next := make(chan writeWork)
			readWork := readWork{
				id:  id,
				res: res,
				off: off,

				cur:  cur,
				next: next,
			}

			f.c.dispatchRequest(res, &sshFxpReadPacket{
				ID:     id,
				Handle: f.handle,
				Offset: uint64(off),
				Len:    uint32(chunkSize),
			})

			select {
			case readCh <- readWork:
			case <-cancel:
				return
			}

			off += int64(chunkSize)
			cur = next
		}
	}()

	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()

			for readWork := range readCh {
				var b []byte
				var n int

				s := <-readWork.res
				f.c.resPool.Put(readWork.res)

				err := s.err
				if err == nil {
					switch s.typ {
					case sshFxpStatus:
						err = normaliseError(unmarshalStatus(readWork.id, s.data))

					case sshFxpData:
						var data []byte
						data, err = unmarshalDataReply(readWork.id, s.data)
						if err == nil {
							b = pool.Get()
							n = copy(b, data)
							b = b[:n]
						}

					default:
						err = unimplementedPacketErr(s.typ)
					}
				}

				writeWork := writeWork{
					b:   b,
					off: readWork.off,
					err: err,

					next: readWork.next,
				}

				select {
				case readWork.cur <- writeWork:
				case <-cancel:
				}
			}
		}()
	}

	cur := writeCh
	for {
		packet, ok := <-cur
		if !ok {
			return written, errors.New("sftp.File.WriteTo: unexpectedly closed channel")
		}

		f.offset = packet.off + int64(len(packet.b))

		if len(packet.b) > 0 {
			n, err := w.Write(packet.b)
			written += int64(n)
			if err != nil {
				return written, err
			}
		}

		if packet.err != nil {
			if packet.err == io.EOF {
				return written, nil
			}

			return written, packet.err
		}

		pool.Put(packet.b)
		cur = packet.next
	}
}

func (f *File) Stat() (os.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return nil, os.ErrClosed
	}

	return f.stat()
}

func (f *File) stat() (os.FileInfo, error) {
	fs, err := f.c.fstat(f.handle)
	if err != nil {
		return nil, err
	}

	return fileInfoFromStat(fs, path.Base(f.path)), nil
}

func (f *File) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return 0, os.ErrClosed
	}

	n, err := f.writeAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *File) writeChunkAt(ch chan result, b []byte, off int64) (int, error) {
	typ, data, err := f.c.sendPacket(context.Background(), ch, &sshFxpWritePacket{
		ID:     f.c.nextID(),
		Handle: f.handle,
		Offset: uint64(off),
		Length: uint32(len(b)),
		Data:   b,
	})
	if err != nil {
		return 0, err
	}

	switch typ {
	case sshFxpStatus:
		id, _, err := unmarshalUint32Safe(data)
		if err != nil {
			return 0, err
		}

		err = normaliseError(unmarshalStatus(id, data))
		if err != nil {
			return 0, err
		}

	default:
		return 0, unimplementedPacketErr(typ)
	}

	return len(b), nil
}

func (f *File) writeAtConcurrent(b []byte, off int64) (int, error) {
	cancel := make(chan struct{})

	type work struct {
		id  uint32
		res chan result

		off int64
	}
	workCh := make(chan work)

	concurrency := len(b)/f.c.maxPacket + 1
	if concurrency > f.c.maxConcurrentRequests || concurrency < 1 {
		concurrency = f.c.maxConcurrentRequests
	}

	go func() {
		defer close(workCh)

		var read int
		chunkSize := f.c.maxPacket

		for read < len(b) {
			wb := b[read:]
			if len(wb) > chunkSize {
				wb = wb[:chunkSize]
			}

			id := f.c.nextID()
			res := f.c.resPool.Get()
			off := off + int64(read)

			f.c.dispatchRequest(res, &sshFxpWritePacket{
				ID:     id,
				Handle: f.handle,
				Offset: uint64(off),
				Length: uint32(len(wb)),
				Data:   wb,
			})

			select {
			case workCh <- work{id, res, off}:
			case <-cancel:
				return
			}

			read += len(wb)
		}
	}()

	type wErr struct {
		off int64
		err error
	}
	errCh := make(chan wErr)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for work := range workCh {
				s := <-work.res
				f.c.resPool.Put(work.res)

				err := s.err
				if err == nil {
					switch s.typ {
					case sshFxpStatus:
						err = normaliseError(unmarshalStatus(work.id, s.data))
					default:
						err = unimplementedPacketErr(s.typ)
					}
				}

				if err != nil {
					errCh <- wErr{work.off, err}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	firstErr := wErr{math.MaxInt64, nil}
	for wErr := range errCh {
		if wErr.off <= firstErr.off {
			firstErr = wErr
		}

		select {
		case <-cancel:
		default:

			close(cancel)
		}
	}

	if firstErr.err != nil {
		return int(firstErr.off - off), firstErr.err
	}

	return len(b), nil
}

func (f *File) WriteAt(b []byte, off int64) (written int, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return 0, os.ErrClosed
	}

	return f.writeAt(b, off)
}

func (f *File) writeAt(b []byte, off int64) (written int, err error) {
	if len(b) <= f.c.maxPacket {
		return f.writeChunkAt(nil, b, off)
	}

	if f.c.useConcurrentWrites {
		return f.writeAtConcurrent(b, off)
	}

	ch := make(chan result, 1)

	chunkSize := f.c.maxPacket

	for written < len(b) {
		wb := b[written:]
		if len(wb) > chunkSize {
			wb = wb[:chunkSize]
		}

		n, err := f.writeChunkAt(ch, wb, off+int64(written))
		if n > 0 {
			written += n
		}

		if err != nil {
			return written, err
		}
	}

	return len(b), nil
}

func (f *File) ReadFromWithConcurrency(r io.Reader, concurrency int) (read int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.readFromWithConcurrency(r, concurrency)
}

func (f *File) readFromWithConcurrency(r io.Reader, concurrency int) (read int64, err error) {
	if f.handle == "" {
		return 0, os.ErrClosed
	}

	cancel := make(chan struct{})

	type work struct {
		id  uint32
		res chan result

		off int64
	}
	workCh := make(chan work)

	type rwErr struct {
		off int64
		err error
	}
	errCh := make(chan rwErr)

	if concurrency > f.c.maxConcurrentRequests || concurrency < 1 {
		concurrency = f.c.maxConcurrentRequests
	}

	go func() {
		defer close(workCh)

		b := make([]byte, f.c.maxPacket)
		off := f.offset

		for {
			n, err := readFull(r, b)

			if n > 0 {
				read += int64(n)

				id := f.c.nextID()
				res := f.c.resPool.Get()

				f.c.dispatchRequest(res, &sshFxpWritePacket{
					ID:     id,
					Handle: f.handle,
					Offset: uint64(off),
					Length: uint32(n),
					Data:   b[:n],
				})

				select {
				case workCh <- work{id, res, off}:
				case <-cancel:
					return
				}

				off += int64(n)
			}

			if err != nil {
				if !errors.Is(err, io.EOF) {
					errCh <- rwErr{off, err}
				}

				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for work := range workCh {
				s := <-work.res
				f.c.resPool.Put(work.res)

				err := s.err
				if err == nil {
					switch s.typ {
					case sshFxpStatus:
						err = normaliseError(unmarshalStatus(work.id, s.data))
					default:
						err = unimplementedPacketErr(s.typ)
					}
				}

				if err != nil {
					errCh <- rwErr{work.off, err}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	firstErr := rwErr{math.MaxInt64, nil}
	for rwErr := range errCh {
		if rwErr.off <= firstErr.off {
			firstErr = rwErr
		}

		select {
		case <-cancel:
		default:

			close(cancel)
		}
	}

	if firstErr.err != nil {
		f.offset = firstErr.off

		return read, firstErr.err
	}

	f.offset += read
	return read, nil
}

func (f *File) ReadFrom(r io.Reader) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return 0, os.ErrClosed
	}

	if f.c.useConcurrentWrites {
		var remain int64
		switch r := r.(type) {
		case interface{ Len() int }:
			remain = int64(r.Len())

		case interface{ Size() int64 }:
			remain = r.Size()

		case *io.LimitedReader:
			remain = r.N

		case interface{ Stat() (os.FileInfo, error) }:
			info, err := r.Stat()
			if err == nil {
				remain = info.Size()
			}
		}

		if remain < 0 {
			return f.readFromWithConcurrency(r, f.c.maxConcurrentRequests)
		}

		if remain > int64(f.c.maxPacket) {
			concurrency64 := min(

				remain/int64(f.c.maxPacket)+1, int64(f.c.maxConcurrentRequests))

			return f.readFromWithConcurrency(r, int(concurrency64))
		}
	}

	ch := make(chan result, 1)

	b := make([]byte, f.c.maxPacket)

	var read int64
	for {
		n, err := readFull(r, b)
		if n < 0 {
			panic("sftp.File: reader returned negative count from Read")
		}

		if n > 0 {
			read += int64(n)

			m, err2 := f.writeChunkAt(ch, b[:n], f.offset)
			f.offset += int64(m)

			if err == nil || (err2 != nil && errors.Is(err, io.EOF)) {
				err = err2
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return read, nil
			}

			return read, err
		}
	}
}

func readFull(r io.Reader, b []byte) (n int, err error) {
	for n < len(b) && err == nil {
		var nn int
		nn, err = r.Read(b[n:])
		n += nn
	}

	return n, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += f.offset
	case io.SeekEnd:
		fi, err := f.stat()
		if err != nil {
			return f.offset, err
		}

		offset += fi.Size()
	default:
		return f.offset, unimplementedSeekWhence(whence)
	}

	if offset < 0 {
		return f.offset, os.ErrInvalid
	}

	f.offset = offset
	return f.offset, nil
}

func (f *File) Chown(uid, gid int) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	return f.c.fsetstat(f.handle, sshFileXferAttrUIDGID, &FileStat{
		UID: uint32(uid),
		GID: uint32(gid),
	})
}

func (f *File) Chmod(mode os.FileMode) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	return f.c.fsetstat(f.handle, sshFileXferAttrPermissions, toChmodPerm(mode))
}

func (f *File) SetExtendedData(path string, extended []StatExtended) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	attrs := &FileStat{
		Extended: extended,
	}

	return f.c.fsetstat(f.handle, sshFileXferAttrExtended, attrs)
}

func (f *File) Truncate(size int64) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	return f.c.fsetstat(f.handle, sshFileXferAttrSize, uint64(size))
}

func (f *File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handle == "" {
		return os.ErrClosed
	}

	if data, ok := f.c.HasExtension(openssh.ExtensionFSync().Name); !ok || data != "1" {
		return &StatusError{
			Code: sshFxOPUnsupported,
			msg:  "fsync not supported",
		}
	}

	id := f.c.nextID()
	typ, data, err := f.c.sendPacket(context.Background(), nil, &sshFxpFsyncPacket{
		ID:     id,
		Handle: f.handle,
	})

	switch {
	case err != nil:
		return err
	case typ == sshFxpStatus:
		return normaliseError(unmarshalStatus(id, data))
	default:
		return &unexpectedPacketErr{want: sshFxpStatus, got: typ}
	}
}

func normaliseError(err error) error {
	switch err := err.(type) {
	case *StatusError:
		switch err.Code {
		case sshFxEOF:
			return io.EOF
		case sshFxNoSuchFile:
			return os.ErrNotExist
		case sshFxPermissionDenied:
			return os.ErrPermission
		case sshFxOk:
			return nil
		default:
			return err
		}
	default:
		return err
	}
}

func toPflags(f int) uint32 {
	var out uint32
	switch f & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		out |= sshFxfRead
	case os.O_WRONLY:
		out |= sshFxfWrite
	case os.O_RDWR:
		out |= sshFxfRead | sshFxfWrite
	}

	if f&os.O_APPEND == os.O_APPEND {
		out |= sshFxfAppend
	}

	if f&os.O_CREATE == os.O_CREATE {
		out |= sshFxfCreat
	}

	if f&os.O_TRUNC == os.O_TRUNC {
		out |= sshFxfTrunc
	}

	if f&os.O_EXCL == os.O_EXCL {
		out |= sshFxfExcl
	}

	return out
}

func toChmodPerm(m os.FileMode) (perm uint32) {
	const mask = os.ModePerm | os.FileMode(s_ISUID|s_ISGID|s_ISVTX)
	perm = uint32(m & mask)

	if m&os.ModeSetuid != 0 {
		perm |= s_ISUID
	}

	if m&os.ModeSetgid != 0 {
		perm |= s_ISGID
	}

	if m&os.ModeSticky != 0 {
		perm |= s_ISVTX
	}

	return perm
}
