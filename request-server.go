package sftp

import (
	"context"
	"errors"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"sync"
)

const defaultMaxTxPacket uint32 = 1 << 15

type (
	Handlers struct {
		FileGet  FileReader
		FilePut  FileWriter
		FileCmd  FileCmder
		FileList FileLister
	}

	RequestServer struct {
		Handlers Handlers

		*serverConn
		pktMgr *packetManager

		startDirectory string
		maxTxPacket    uint32

		mu           sync.RWMutex
		handleCount  int
		openRequests map[string]*Request
	}

	RequestServerOption func(*RequestServer)
)

func WithRSAllocator() RequestServerOption {
	return func(rs *RequestServer) {
		alloc := newAllocator()
		rs.pktMgr.alloc = alloc
		rs.conn.alloc = alloc
	}
}

func WithStartDirectory(startDirectory string) RequestServerOption {
	return func(rs *RequestServer) {
		rs.startDirectory = cleanPath(startDirectory)
	}
}

func WithRSMaxTxPacket(size uint32) RequestServerOption {
	return func(rs *RequestServer) {
		if size < defaultMaxTxPacket {
			return
		}

		rs.maxTxPacket = size
	}
}

func NewRequestServer(rwc io.ReadWriteCloser, h Handlers, options ...RequestServerOption) *RequestServer {
	svrConn := &serverConn{
		Reader:      rwc,
		WriteCloser: rwc,
	}
	rs := &RequestServer{
		Handlers: h,

		serverConn: svrConn,
		pktMgr:     newPktMgr(svrConn),

		startDirectory: "/",
		maxTxPacket:    defaultMaxTxPacket,

		openRequests: make(map[string]*Request),
	}

	for _, o := range options {
		o(rs)
	}

	return rs
}

func (rs *RequestServer) nextRequest(r *Request) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.handleCount++

	r.handle = strconv.Itoa(rs.handleCount)
	rs.openRequests[r.handle] = r

	return r.handle
}

func (rs *RequestServer) getRequest(handle string) (*Request, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	r, ok := rs.openRequests[handle]
	return r, ok
}

func (rs *RequestServer) closeRequest(handle string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if r, ok := rs.openRequests[handle]; ok {
		delete(rs.openRequests, handle)
		return r.close()
	}

	return EBADF
}

func (rs *RequestServer) Close() error { return rs.conn.Close() }

func (rs *RequestServer) serveLoop(pktChan chan<- orderedRequest) error {
	defer close(pktChan)

	var err error
	var pkt requestPacket
	var pktType fxp
	var pktBytes []byte

	for {
		pktType, pktBytes, err = rs.serverConn.recvPacket(rs.pktMgr.getNextOrderID())
		if err != nil {
			return err
		}

		pkt, err = makePacket(rxPacket{pktType, pktBytes})
		if err != nil {
			switch {
			case errors.Is(err, errUnknownExtendedPacket):

			default:
				debug("makePacket err: %v", err)
				rs.conn.Close()
				return err
			}
		}

		pktChan <- rs.pktMgr.newOrderedRequest(pkt)
	}
}

func (rs *RequestServer) Serve() error {
	defer func() {
		if rs.pktMgr.alloc != nil {
			rs.pktMgr.alloc.Free()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	runWorker := func(ch chan orderedRequest) {
		wg.Go(func() {
			if err := rs.packetWorker(ctx, ch); err != nil {
				rs.conn.Close()
			}
		})
	}
	pktChan := rs.pktMgr.workerChan(runWorker)

	err := rs.serveLoop(pktChan)

	wg.Wait()

	rs.mu.Lock()
	defer rs.mu.Unlock()

	for handle, req := range rs.openRequests {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}

		req.transferError(err)

		delete(rs.openRequests, handle)
		req.close()
	}

	return err
}

func (rs *RequestServer) packetWorker(ctx context.Context, pktChan chan orderedRequest) error {
	for pkt := range pktChan {
		orderID := pkt.orderID()
		if epkt, ok := pkt.requestPacket.(*sshFxpExtendedPacket); ok {
			if epkt.SpecificPacket != nil {
				pkt.requestPacket = epkt.SpecificPacket
			}
		}

		var rpkt responsePacket
		switch pkt := pkt.requestPacket.(type) {
		case *sshFxInitPacket:
			rpkt = &sshFxVersionPacket{Version: sftpProtocolVersion, Extensions: sftpExtensions}
		case *sshFxpClosePacket:
			handle := pkt.getHandle()
			rpkt = statusFromError(pkt.ID, rs.closeRequest(handle))
		case *sshFxpRealpathPacket:
			var realPath string
			var err error

			switch pather := rs.Handlers.FileList.(type) {
			case RealPathFileLister:
				realPath, err = pather.RealPath(pkt.getPath())
			case legacyRealPathFileLister:
				realPath = pather.RealPath(pkt.getPath())
			default:
				realPath = cleanPathWithBase(rs.startDirectory, pkt.getPath())
			}

			switch {
			case err != nil:

				rpkt = statusFromError(pkt.ID, err)

			default:

				rpkt = cleanPacketPath(pkt, realPath)
			}
		case *sshFxpOpendirPacket:
			request := requestFromPacket(ctx, pkt, rs.startDirectory)
			handle := rs.nextRequest(request)
			rpkt = request.opendir(rs.Handlers, pkt)
			if _, ok := rpkt.(*sshFxpHandlePacket); !ok {
				rs.closeRequest(handle)
			}
		case *sshFxpOpenPacket:
			request := requestFromPacket(ctx, pkt, rs.startDirectory)
			handle := rs.nextRequest(request)
			rpkt = request.open(rs.Handlers, pkt)
			if _, ok := rpkt.(*sshFxpHandlePacket); !ok {
				rs.closeRequest(handle)
			}
		case *sshFxpFstatPacket:
			handle := pkt.getHandle()
			request, ok := rs.getRequest(handle)
			switch {
			case !ok:

				rpkt = statusFromError(pkt.ID, EBADF)

			default:

				request = &Request{
					Method:   "Stat",
					Filepath: cleanPathWithBase(rs.startDirectory, request.Filepath),
				}
				rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
			}
		case *sshFxpFsetstatPacket:
			handle := pkt.getHandle()
			request, ok := rs.getRequest(handle)
			switch {
			case !ok:

				rpkt = statusFromError(pkt.ID, EBADF)

			default:

				request = &Request{
					Method:   "Setstat",
					Filepath: cleanPathWithBase(rs.startDirectory, request.Filepath),
				}
				rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
			}
		case *sshFxpExtendedPacketPosixRename:
			request := &Request{
				Method:   "PosixRename",
				Filepath: cleanPathWithBase(rs.startDirectory, pkt.Oldpath),
				Target:   cleanPathWithBase(rs.startDirectory, pkt.Newpath),
			}
			rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
		case *sshFxpExtendedPacketStatVFS:
			request := &Request{
				Method:   "StatVFS",
				Filepath: cleanPathWithBase(rs.startDirectory, pkt.Path),
			}
			rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
		case hasHandle:
			handle := pkt.getHandle()
			request, ok := rs.getRequest(handle)
			switch {
			case !ok:

				rpkt = statusFromError(pkt.id(), EBADF)

			default:

				rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
			}
		case hasPath:
			request := requestFromPacket(ctx, pkt, rs.startDirectory)
			rpkt = request.call(rs.Handlers, pkt, rs.pktMgr.alloc, orderID, rs.maxTxPacket)
			request.close()
		default:
			rpkt = statusFromError(pkt.id(), ErrSSHFxOpUnsupported)
		}

		rs.pktMgr.readyPacket(
			rs.pktMgr.newOrderedResponse(rpkt, orderID))
	}

	return nil
}

func cleanPacketPath(pkt *sshFxpRealpathPacket, realPath string) responsePacket {
	return &sshFxpNamePacket{
		ID: pkt.id(),
		NameAttrs: []*sshFxpNameAttr{
			{
				Name:     realPath,
				LongName: realPath,
				Attrs:    emptyFileStat,
			},
		},
	}
}

func cleanPath(p string) string {
	return cleanPathWithBase("/", p)
}

func cleanPathWithBase(base, p string) string {
	p = filepath.ToSlash(filepath.Clean(p))
	if !path.IsAbs(p) {
		return path.Join(base, p)
	}

	return p
}
