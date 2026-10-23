package sftp

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const SftpServerWorkerCount = 8

var emptyFileStat = []any{uint32(0)}

type (
	file interface {
		Stat() (os.FileInfo, error)
		ReadAt(b []byte, off int64) (int, error)
		WriteAt(b []byte, off int64) (int, error)
		Readdir(int) ([]os.FileInfo, error)
		Name() string
		Truncate(int64) error
		Chmod(mode fs.FileMode) error
		Chown(uid, gid int) error
		Close() error
	}

	Server struct {
		*serverConn
		debugStream   io.Writer
		readOnly      bool
		pktMgr        *packetManager
		openFiles     map[string]file
		openFilesLock sync.RWMutex
		handleCount   int
		workDir       string
		winRoot       bool
		maxTxPacket   uint32
	}

	serverRespondablePacket interface {
		encoding.BinaryUnmarshaler
		id() uint32
		respond(svr *Server) responsePacket
	}

	ServerOption func(*Server) error

	rxPacket struct {
		pktType  fxp
		pktBytes []byte
	}

	ider interface {
		id() uint32
	}

	sshFxpStatResponse struct {
		info os.FileInfo
		ID   uint32
	}
)

func (svr *Server) nextHandle(f file) string {
	svr.openFilesLock.Lock()
	defer svr.openFilesLock.Unlock()

	svr.handleCount++
	handle := strconv.Itoa(svr.handleCount)
	svr.openFiles[handle] = f
	return handle
}

func (svr *Server) closeHandle(handle string) error {
	svr.openFilesLock.Lock()
	defer svr.openFilesLock.Unlock()

	if f, ok := svr.openFiles[handle]; ok {
		delete(svr.openFiles, handle)
		return f.Close()
	}

	return EBADF
}

func (svr *Server) getHandle(handle string) (file, bool) {
	svr.openFilesLock.RLock()
	defer svr.openFilesLock.RUnlock()

	f, ok := svr.openFiles[handle]
	return f, ok
}

func NewServer(rwc io.ReadWriteCloser, options ...ServerOption) (*Server, error) {
	svrConn := &serverConn{
		Reader:      rwc,
		WriteCloser: rwc,
	}
	s := &Server{
		serverConn:  svrConn,
		debugStream: io.Discard,
		pktMgr:      newPktMgr(svrConn),
		openFiles:   make(map[string]file),
		maxTxPacket: defaultMaxTxPacket,
	}

	for _, o := range options {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func WithDebug(w io.Writer) ServerOption {
	return func(s *Server) error {
		s.debugStream = w
		return nil
	}
}

func ReadOnly() ServerOption {
	return func(s *Server) error {
		s.readOnly = true
		return nil
	}
}

func WindowsRootEnumeratesDrives() ServerOption {
	return func(s *Server) error {
		s.winRoot = true
		return nil
	}
}

func WithAllocator() ServerOption {
	return func(s *Server) error {
		alloc := newAllocator()
		s.pktMgr.alloc = alloc
		s.conn.alloc = alloc
		return nil
	}
}

func WithServerWorkingDirectory(workDir string) ServerOption {
	return func(s *Server) error {
		s.workDir = cleanPath(workDir)
		return nil
	}
}

func WithMaxTxPacket(size uint32) ServerOption {
	return func(s *Server) error {
		if size < defaultMaxTxPacket {
			return errors.New("size must be greater than or equal to 32768")
		}

		s.maxTxPacket = size

		return nil
	}
}

func (svr *Server) sftpServerWorker(pktChan chan orderedRequest) error {
	for pkt := range pktChan {
		readonly := true
		switch pkt := pkt.requestPacket.(type) {
		case notReadOnly:
			readonly = false
		case *sshFxpOpenPacket:
			readonly = pkt.readonly()
		case *sshFxpExtendedPacket:
			readonly = pkt.readonly()
		}

		if !readonly && svr.readOnly {
			svr.pktMgr.readyPacket(
				svr.pktMgr.newOrderedResponse(statusFromError(pkt.id(), syscall.EPERM), pkt.orderID()),
			)
			continue
		}

		if err := handlePacket(svr, pkt); err != nil {
			return err
		}
	}

	return nil
}

func handlePacket(s *Server, p orderedRequest) error {
	var rpkt responsePacket
	orderID := p.orderID()
	switch p := p.requestPacket.(type) {
	case *sshFxInitPacket:
		rpkt = &sshFxVersionPacket{
			Version:    sftpProtocolVersion,
			Extensions: sftpExtensions,
		}
	case *sshFxpStatPacket:

		info, err := os.Stat(s.toLocalPath(p.Path))
		rpkt = &sshFxpStatResponse{
			ID:   p.ID,
			info: info,
		}
		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}
	case *sshFxpLstatPacket:

		info, err := s.lstat(s.toLocalPath(p.Path))
		rpkt = &sshFxpStatResponse{
			ID:   p.ID,
			info: info,
		}
		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}
	case *sshFxpFstatPacket:
		f, ok := s.getHandle(p.Handle)
		var err error = EBADF
		var info os.FileInfo
		if ok {
			info, err = f.Stat()
			rpkt = &sshFxpStatResponse{
				ID:   p.ID,
				info: info,
			}
		}

		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}
	case *sshFxpMkdirPacket:

		err := os.Mkdir(s.toLocalPath(p.Path), 0o755)
		rpkt = statusFromError(p.ID, err)
	case *sshFxpRmdirPacket:
		err := os.Remove(s.toLocalPath(p.Path))
		rpkt = statusFromError(p.ID, err)
	case *sshFxpRemovePacket:
		err := os.Remove(s.toLocalPath(p.Filename))
		rpkt = statusFromError(p.ID, err)
	case *sshFxpRenamePacket:
		err := os.Rename(s.toLocalPath(p.Oldpath), s.toLocalPath(p.Newpath))
		rpkt = statusFromError(p.ID, err)
	case *sshFxpSymlinkPacket:
		err := os.Symlink(s.toLocalPath(p.Targetpath), s.toLocalPath(p.Linkpath))
		rpkt = statusFromError(p.ID, err)
	case *sshFxpClosePacket:
		rpkt = statusFromError(p.ID, s.closeHandle(p.Handle))
	case *sshFxpReadlinkPacket:
		f, err := os.Readlink(s.toLocalPath(p.Path))
		rpkt = &sshFxpNamePacket{
			ID: p.ID,
			NameAttrs: []*sshFxpNameAttr{
				{
					Name:     f,
					LongName: f,
					Attrs:    emptyFileStat,
				},
			},
		}
		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}
	case *sshFxpRealpathPacket:
		f, err := filepath.Abs(s.toLocalPath(p.Path))
		f = cleanPath(f)
		rpkt = &sshFxpNamePacket{
			ID: p.ID,
			NameAttrs: []*sshFxpNameAttr{
				{
					Name:     f,
					LongName: f,
					Attrs:    emptyFileStat,
				},
			},
		}
		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}
	case *sshFxpOpendirPacket:
		lp := s.toLocalPath(p.Path)

		switch stat, err := s.stat(lp); {
		case err != nil:

			rpkt = statusFromError(p.ID, err)

		case !stat.IsDir():

			rpkt = statusFromError(p.ID, &os.PathError{
				Path: lp, Err: syscall.ENOTDIR,
			})

		default:

			rpkt = (&sshFxpOpenPacket{
				ID:     p.ID,
				Path:   p.Path,
				Pflags: sshFxfRead,
			}).respond(s)
		}
	case *sshFxpReadPacket:
		var err error = EBADF
		f, ok := s.getHandle(p.Handle)
		if ok {
			err = nil
			data := p.getDataSlice(s.pktMgr.alloc, orderID, s.maxTxPacket)
			n, _err := f.ReadAt(data, int64(p.Offset))
			if _err != nil && (_err != io.EOF || n == 0) {
				err = _err
			}

			rpkt = &sshFxpDataPacket{
				ID:     p.ID,
				Length: uint32(n),
				Data:   data[:n],
			}
		}

		if err != nil {
			rpkt = statusFromError(p.ID, err)
		}

	case *sshFxpWritePacket:
		f, ok := s.getHandle(p.Handle)
		var err error = EBADF
		if ok {
			_, err = f.WriteAt(p.Data, int64(p.Offset))
		}

		rpkt = statusFromError(p.ID, err)
	case *sshFxpExtendedPacket:
		switch {
		case p.SpecificPacket == nil:

			rpkt = statusFromError(p.ID, ErrSSHFxOpUnsupported)

		default:

			rpkt = p.respond(s)
		}
	case serverRespondablePacket:
		rpkt = p.respond(s)
	default:
		return fmt.Errorf("unexpected packet type %T", p)
	}

	s.pktMgr.readyPacket(s.pktMgr.newOrderedResponse(rpkt, orderID))
	return nil
}

func (svr *Server) Serve() error {
	defer func() {
		if svr.pktMgr.alloc != nil {
			svr.pktMgr.alloc.Free()
		}
	}()

	var wg sync.WaitGroup
	runWorker := func(ch chan orderedRequest) {
		wg.Go(func() {
			if err := svr.sftpServerWorker(ch); err != nil {
				svr.conn.Close()
			}
		})
	}
	pktChan := svr.pktMgr.workerChan(runWorker)

	var err error
	var pkt requestPacket
	var pktType fxp
	var pktBytes []byte
recvLoop:
	for {
		pktType, pktBytes, err = svr.serverConn.recvPacket(svr.pktMgr.getNextOrderID())
		if err != nil {
			if err == io.EOF {
				err = nil
			}

			break
		}

		pkt, err = makePacket(rxPacket{pktType, pktBytes})
		if err != nil {
			switch {
			case errors.Is(err, errUnknownExtendedPacket):

			default:
				debug("makePacket err: %v", err)
				svr.conn.Close()
				break recvLoop
			}
		}

		pktChan <- svr.pktMgr.newOrderedRequest(pkt)
	}

	close(pktChan)
	wg.Wait()

	for handle, file := range svr.openFiles {
		fmt.Fprintf(svr.debugStream, "sftp server file with handle %q left open: %v\n", handle, file.Name())
		file.Close()
	}

	return err
}

func (p *sshFxInitPacket) id() uint32 { return 0 }

func (p *sshFxpStatResponse) marshalPacket() ([]byte, []byte, error) {
	l := 4 + 1 + 4

	b := make([]byte, 4, l)
	b = append(b, sshFxpAttrs)
	b = marshalUint32(b, p.ID)

	var payload []byte
	payload = marshalFileInfo(payload, p.info)

	return b, payload, nil
}

func (p *sshFxpStatResponse) MarshalBinary() ([]byte, error) {
	header, payload, err := p.marshalPacket()
	return append(header, payload...), err
}

func (p *sshFxpOpenPacket) readonly() bool {
	return p.Pflags&(sshFxfWrite|sshFxfCreat|sshFxfTrunc) == 0
}

func (p *sshFxpOpenPacket) hasPflags(flags ...uint32) bool {
	for _, f := range flags {
		if p.Pflags&f == 0 {
			return false
		}
	}

	return true
}

func (p *sshFxpOpenPacket) respond(svr *Server) responsePacket {
	var osFlags int
	switch {
	case p.hasPflags(sshFxfRead, sshFxfWrite):

		osFlags |= os.O_RDWR

	case p.hasPflags(sshFxfWrite):

		osFlags |= os.O_WRONLY

	case p.hasPflags(sshFxfRead):

		osFlags |= os.O_RDONLY

	default:

		return statusFromError(p.ID, syscall.EINVAL)
	}

	if p.hasPflags(sshFxfCreat) {
		osFlags |= os.O_CREATE
	}

	if p.hasPflags(sshFxfTrunc) {
		osFlags |= os.O_TRUNC
	}

	if p.hasPflags(sshFxfExcl) {
		osFlags |= os.O_EXCL
	}

	mode := os.FileMode(0o644)

	if p.Flags&sshFileXferAttrPermissions != 0 {
		fs, err := p.unmarshalFileStat(p.Flags)
		if err != nil {
			return statusFromError(p.ID, err)
		}

		mode = fs.FileMode() & os.ModePerm
	}

	f, err := svr.openfile(svr.toLocalPath(p.Path), osFlags, mode)
	if err != nil {
		return statusFromError(p.ID, err)
	}

	handle := svr.nextHandle(f)
	return &sshFxpHandlePacket{ID: p.ID, Handle: handle}
}

func (p *sshFxpReaddirPacket) respond(svr *Server) responsePacket {
	f, ok := svr.getHandle(p.Handle)
	if !ok {
		return statusFromError(p.ID, EBADF)
	}

	dirents, err := f.Readdir(128)
	if err != nil {
		return statusFromError(p.ID, err)
	}

	idLookup := osIDLookup{}

	ret := &sshFxpNamePacket{ID: p.ID}
	for _, dirent := range dirents {
		ret.NameAttrs = append(ret.NameAttrs, &sshFxpNameAttr{
			Name:     dirent.Name(),
			LongName: runLs(idLookup, dirent),
			Attrs:    []any{dirent},
		})
	}

	return ret
}

func (p *sshFxpSetstatPacket) respond(svr *Server) responsePacket {
	path := svr.toLocalPath(p.Path)

	debug("setstat name %q", path)

	fs, err := p.unmarshalFileStat(p.Flags)

	if err == nil && (p.Flags&sshFileXferAttrSize) != 0 {
		err = os.Truncate(path, int64(fs.Size))
	}

	if err == nil && (p.Flags&sshFileXferAttrPermissions) != 0 {
		err = os.Chmod(path, fs.FileMode())
	}

	if err == nil && (p.Flags&sshFileXferAttrUIDGID) != 0 {
		err = os.Chown(path, int(fs.UID), int(fs.GID))
	}

	if err == nil && (p.Flags&sshFileXferAttrACmodTime) != 0 {
		err = os.Chtimes(path, fs.AccessTime(), fs.ModTime())
	}

	return statusFromError(p.ID, err)
}

func (p *sshFxpFsetstatPacket) respond(svr *Server) responsePacket {
	f, ok := svr.getHandle(p.Handle)
	if !ok {
		return statusFromError(p.ID, EBADF)
	}

	path := f.Name()

	debug("fsetstat name %q", path)

	fs, err := p.unmarshalFileStat(p.Flags)

	if err == nil && (p.Flags&sshFileXferAttrSize) != 0 {
		err = f.Truncate(int64(fs.Size))
	}

	if err == nil && (p.Flags&sshFileXferAttrPermissions) != 0 {
		err = f.Chmod(fs.FileMode())
	}

	if err == nil && (p.Flags&sshFileXferAttrUIDGID) != 0 {
		err = f.Chown(int(fs.UID), int(fs.GID))
	}

	if err == nil && (p.Flags&sshFileXferAttrACmodTime) != 0 {
		type chtimer interface {
			Chtimes(atime, mtime time.Time) error
		}

		switch f := any(f).(type) {
		case chtimer:

			err = f.Chtimes(fs.AccessTime(), fs.ModTime())
		default:
			err = os.Chtimes(path, fs.AccessTime(), fs.ModTime())
		}
	}

	return statusFromError(p.ID, err)
}

func statusFromError(id uint32, err error) *sshFxpStatusPacket {
	ret := &sshFxpStatusPacket{
		ID: id,

		Code: sshFxOk,
	}
	if err == nil {
		return ret
	}

	debug("statusFromError: error is %T %#v", err, err)
	ret.StatusError.Code = sshFxFailure
	ret.StatusError.msg = err.Error()

	if os.IsNotExist(err) {
		ret.StatusError.Code = sshFxNoSuchFile
		return ret
	}

	if code, ok := translateSyscallError(err); ok {
		ret.StatusError.Code = code
		return ret
	}

	if errors.Is(err, io.EOF) {
		ret.StatusError.Code = sshFxEOF
		return ret
	}

	if e, ok := errors.AsType[fxerr](err); ok {
		ret.StatusError.Code = uint32(e)
		return ret
	}

	return ret
}
