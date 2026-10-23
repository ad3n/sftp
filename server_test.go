package sftp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sshFxpTestBadExtendedPacket struct {
	ID        uint32
	Extension string
	Data      string
}

func clientServerPair(t *testing.T, options ...ServerOption) (*Client, *Server) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	if *testAllocator {
		options = append(options, WithAllocator())
	}

	server, err := NewServer(struct {
		io.Reader
		io.WriteCloser
	}{sr, sw}, options...)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve()
	client, err := NewClientPipe(cr, cw)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	return client, server
}

func (p sshFxpTestBadExtendedPacket) id() uint32 { return p.ID }

func (p sshFxpTestBadExtendedPacket) MarshalBinary() ([]byte, error) {
	l := 4 + 1 + 4 +
		4 + len(p.Extension) +
		4 + len(p.Data)

	b := make([]byte, 4, l)
	b = append(b, sshFxpExtended)
	b = marshalUint32(b, p.ID)
	b = marshalString(b, p.Extension)
	b = marshalString(b, p.Data)

	return b, nil
}

func checkServerAllocator(t *testing.T, server *Server) {
	if server.pktMgr.alloc == nil {
		return
	}

	checkAllocatorBeforeServerClose(t, server.pktMgr.alloc)
	server.Close()
	checkAllocatorAfterServerClose(t, server.pktMgr.alloc)
}

func TestInvalidExtendedPacket(t *testing.T) {
	client, server := clientServerPair(t)
	defer client.Close()

	defer server.Close()

	badPacket := sshFxpTestBadExtendedPacket{client.nextID(), "thisDoesn'tExist", "foobar"}
	typ, data, err := client.clientConn.sendPacket(context.Background(), nil, badPacket)
	if err != nil {
		t.Fatalf("unexpected error from sendPacket: %s", err)
	}

	if typ != sshFxpStatus {
		t.Fatalf("received non-FPX_STATUS packet: %v", typ)
	}

	err = unmarshalStatus(badPacket.id(), data)
	statusErr, ok := err.(*StatusError)
	if !ok {
		t.Fatal("failed to convert error from unmarshalStatus to *StatusError")
	}

	if statusErr.Code != sshFxOPUnsupported {
		t.Errorf("statusErr.Code => %d, wanted %d", statusErr.Code, sshFxOPUnsupported)
	}

	checkServerAllocator(t, server)
}

func TestConcurrentRequests(t *testing.T) {
	skipIfWindows(t)
	var filename string
	switch runtime.GOOS {
	case "plan9":
		filename = "/lib/ndb/local"
	case "zos":
		filename = "/etc/.shrc"
	default:
		filename = "/etc/passwd"
	}

	client, server := clientServerPair(t)
	defer client.Close()

	defer server.Close()

	concurrency := 2
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			for range 1024 {
				f, err := client.Open(filename)
				if err != nil {
					t.Errorf("failed to open file: %v", err)
					continue
				}

				if err := f.Close(); err != nil {
					t.Errorf("failed t close file: %v", err)
				}
			}
		}()
	}

	wg.Wait()
	checkServerAllocator(t, server)
}

func TestStatusFromError(t *testing.T) {
	type test struct {
		err error
		pkt *sshFxpStatusPacket
	}
	tpkt := func(id, code uint32) *sshFxpStatusPacket {
		return &sshFxpStatusPacket{
			ID:   id,
			Code: code,
		}
	}
	testCases := []test{
		{syscall.ENOENT, tpkt(1, sshFxNoSuchFile)},
		{&os.PathError{Err: syscall.ENOENT},
			tpkt(2, sshFxNoSuchFile)},
		{&os.PathError{Err: errors.New("foo")}, tpkt(3, sshFxFailure)},
		{ErrSSHFxEOF, tpkt(4, sshFxEOF)},
		{ErrSSHFxOpUnsupported, tpkt(5, sshFxOPUnsupported)},
		{io.EOF, tpkt(6, sshFxEOF)},
		{os.ErrNotExist, tpkt(7, sshFxNoSuchFile)},
	}
	for _, tc := range testCases {
		tc.pkt.StatusError.msg = tc.err.Error()
		assert.Equal(t, tc.pkt, statusFromError(tc.pkt.ID, tc.err))
	}
}

func TestOpenStatRace(t *testing.T) {
	client, server := clientServerPair(t)
	defer client.Close()

	defer server.Close()

	tmppath := path.Join(os.TempDir(), "stat_race")
	pflags := toPflags(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	ch := make(chan result, 3)
	id1 := client.nextID()
	id2 := client.nextID()
	client.dispatchRequest(ch, &sshFxpOpenPacket{
		ID:     id1,
		Path:   tmppath,
		Pflags: pflags,
	})
	client.dispatchRequest(ch, &sshFxpLstatPacket{
		ID:   id2,
		Path: tmppath,
	})
	testreply := func(id uint32) {
		r := <-ch
		require.NoError(t, r.err)
		switch r.typ {
		case sshFxpAttrs, sshFxpHandle:
		case sshFxpStatus:
			err := normaliseError(unmarshalStatus(id, r.data))
			assert.NoError(t, err, "race hit, stat before open")
		default:
			t.Fatal("unexpected type:", r.typ)
		}
	}
	testreply(id1)
	testreply(id2)
	os.Remove(tmppath)
	checkServerAllocator(t, server)
}

func TestOpenWithPermissions(t *testing.T) {
	ctx := t.Context()

	skipIfWindows(t)

	client, server := clientServerPair(t)
	defer client.Close()

	defer server.Close()

	tmppath := path.Join(os.TempDir(), "open_permissions")
	defer os.Remove(tmppath)

	pflags := toPflags(os.O_RDWR | os.O_CREATE | os.O_TRUNC)

	id1 := client.nextID()
	id2 := client.nextID()

	typ, data, err := client.sendPacket(ctx, nil, &sshFxpOpenPacket{
		ID:     id1,
		Path:   tmppath,
		Pflags: pflags,
		Flags:  sshFileXferAttrPermissions,
		Attrs: &FileStat{
			Mode: 0o745,
		},
	})
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	switch typ {
	case sshFxpHandle:

	case sshFxpStatus:
		t.Fatal("unexpected status:", normaliseError(unmarshalStatus(id1, data)))
	default:
		t.Fatal("unpexpected packet type:", unimplementedPacketErr(typ))
	}

	stat, err := os.Stat(tmppath)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	if stat.Mode()&os.ModePerm != 0o745 {
		t.Errorf("stat.Mode() = %v was expecting 0o745", stat.Mode())
	}

	typ, data, err = client.sendPacket(ctx, nil, &sshFxpOpenPacket{
		ID:     id2,
		Path:   tmppath,
		Pflags: pflags,
		Flags:  sshFileXferAttrPermissions,
		Attrs: &FileStat{
			Mode: 0o755,
		},
	})
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	switch typ {
	case sshFxpHandle:

	case sshFxpStatus:
		t.Fatal("unexpected status:", normaliseError(unmarshalStatus(id2, data)))
	default:
		t.Fatal("unpexpected packet type:", unimplementedPacketErr(typ))
	}

	if stat.Mode()&os.ModePerm != 0o745 {
		t.Errorf("stat.Mode() = %v, was expecting unchanged 0o745", stat.Mode())
	}

	checkServerAllocator(t, server)
}

func TestReadOnlyServerRejectsWrites(t *testing.T) {
	skipIfWindows(t)

	client, server := clientServerPair(t, ReadOnly())
	defer client.Close()

	defer server.Close()

	dir := t.TempDir()
	existing := path.Join(dir, "existing")
	require.NoError(t, os.WriteFile(existing, []byte("content"), 0o644))

	link := path.Join(dir, "link")
	assert.Error(t, client.Link(existing, link))
	assert.NoFileExists(t, link)

	created := path.Join(dir, "created")
	if f, err := client.OpenFile(created, os.O_RDONLY|os.O_CREATE); err == nil {
		f.Close()
		t.Error("expected error opening with O_CREATE on a read-only server")
	}

	assert.NoFileExists(t, created)

	if f, err := client.OpenFile(existing, os.O_RDONLY|os.O_TRUNC); err == nil {
		f.Close()
		t.Error("expected error opening with O_TRUNC on a read-only server")
	}

	b, err := os.ReadFile(existing)
	require.NoError(t, err)
	assert.Equal(t, "content", string(b))

	f, err := client.Open(existing)
	require.NoError(t, err)
	f.Close()
}

func TestStatNonExistent(t *testing.T) {
	client, server := clientServerPair(t)
	defer client.Close()

	defer server.Close()

	for _, file := range []string{"/doesnotexist", "/doesnotexist/a/b"} {
		_, err := client.Stat(file)
		if !os.IsNotExist(err) {
			t.Errorf("expected 'does not exist' err for file %q.  got: %v", file, err)
		}
	}
}

func TestServerWithBrokenClient(t *testing.T) {
	validInit := sp(&sshFxInitPacket{Version: 3})
	brokenOpen := sp(&sshFxpOpenPacket{Path: "foo"})
	brokenOpen = brokenOpen[:len(brokenOpen)-2]

	for _, clientInput := range [][]byte{
		{0, 0, 0, 0},
		append(validInit, 0, 0, 0, 0),

		append(validInit, brokenOpen...),
	} {
		srv, err := NewServer(struct {
			io.Reader
			io.WriteCloser
		}{
			bytes.NewReader(clientInput),
			&sink{},
		})
		require.NoError(t, err)

		err = srv.Serve()
		assert.Error(t, err)
		srv.Close()
	}
}

func TestServerDoesNotExecuteMalformedPacket(t *testing.T) {
	dir := path.Join(t.TempDir(), "should-not-exist")

	mkdir := sp(&sshFxpMkdirPacket{ID: 1, Path: dir})
	mkdir = mkdir[:len(mkdir)-4]
	binary.BigEndian.PutUint32(mkdir, uint32(len(mkdir)-4))

	srv, err := NewServer(struct {
		io.Reader
		io.WriteCloser
	}{
		bytes.NewReader(append(sp(&sshFxInitPacket{Version: 3}), mkdir...)),
		&sink{},
	})
	require.NoError(t, err)

	err = srv.Serve()
	assert.ErrorIs(t, err, errShortPacket)
	srv.Close()

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err), "malformed mkdir was executed: %v", err)
}
