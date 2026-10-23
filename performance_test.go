package sftp

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func BenchmarkNormaliseStatus(b *testing.B) {
	for _, tc := range []struct {
		name string
		code uint32
	}{
		{name: "OK", code: sshFxOk},
		{name: "EOF", code: sshFxEOF},
		{name: "NotExist", code: sshFxNoSuchFile},
		{name: "Permission", code: sshFxPermissionDenied},
		{name: "Failure", code: sshFxFailure},
	} {
		b.Run(tc.name, func(b *testing.B) {
			data := marshalUint32(nil, 42)
			data = marshalUint32(data, tc.code)
			data = marshalString(data, "server status message")
			data = marshalString(data, "en")
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_ = normaliseStatus(42, data)
			}
		})
	}
}

func BenchmarkMarshalStatVFS(b *testing.B) {
	p := &StatVFS{ID: 42, Bsize: 4096, Frsize: 4096, Blocks: 1 << 40}
	b.ReportAllocs()

	for b.Loop() {
		_, _ = p.MarshalBinary()
	}
}

func TestNormaliseStatusCompatibility(t *testing.T) {
	for _, code := range []uint32{sshFxOk, sshFxEOF, sshFxNoSuchFile, sshFxPermissionDenied, sshFxFailure, sshFxBadMessage, ^uint32(0)} {
		data := marshalUint32(nil, 42)
		data = marshalUint32(data, code)
		data = marshalString(data, "status message")
		data = marshalString(data, "en")

		for n := 0; n <= len(data); n++ {
			for _, id := range []uint32{42, 43} {
				got := normaliseStatus(id, data[:n])
				want := normaliseError(unmarshalStatus(id, data[:n]))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("code=%d id=%d length=%d: got %#v, want %#v", code, id, n, got, want)
				}
			}
		}
	}
}

func TestNormaliseStatusNoAllocations(t *testing.T) {
	for _, code := range []uint32{sshFxOk, sshFxEOF, sshFxNoSuchFile, sshFxPermissionDenied} {
		data := marshalUint32(nil, 42)
		data = marshalUint32(data, code)
		data = marshalString(data, "status message")
		data = marshalString(data, "en")
		allocs := testing.AllocsPerRun(1000, func() {
			_ = normaliseStatus(42, data)
		})
		if allocs != 0 {
			t.Fatalf("code=%d: got %g allocations, want zero", code, allocs)
		}
	}
}

func TestStatVFSWireCompatibility(t *testing.T) {
	for _, p := range []*StatVFS{
		nil,
		{},
		{ID: 0x12345678, Bsize: 1, Frsize: 2, Blocks: 3, Bfree: 4, Bavail: 5, Files: 6, Ffree: 7, Favail: 8, Fsid: 9, Flag: 10, Namemax: ^uint64(0)},
	} {
		var buf bytes.Buffer
		wantErr := binary.Write(&buf, binary.BigEndian, p)
		want := append([]byte{0, 0, 0, 0, sshFxpExtendedReply}, buf.Bytes()...)
		got, gotErr := p.MarshalBinary()
		if !reflect.DeepEqual(gotErr, wantErr) || !bytes.Equal(got, want) {
			t.Fatalf("got %x, %v; want %x, %v", got, gotErr, want, wantErr)
		}
	}
}

func TestStatVFSMarshalOwnership(t *testing.T) {
	p := &StatVFS{ID: 42, Bsize: 4096}
	first, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	want := bytes.Clone(first)
	p.ID++
	second, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	clear(second)
	if !bytes.Equal(first, want) {
		t.Fatal("later marshal modified an earlier result")
	}
}

func FuzzNormaliseStatusCompatibility(f *testing.F) {
	f.Add(uint32(42), []byte{})
	for code := uint32(0); code <= 8; code++ {
		data := marshalUint32(nil, 42)
		data = marshalUint32(data, code)
		data = marshalString(data, "message")
		data = marshalString(data, "en")
		f.Add(uint32(42), data)
	}

	f.Fuzz(func(t *testing.T, id uint32, data []byte) {
		got := normaliseStatus(id, data)
		want := normaliseError(unmarshalStatus(id, data))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})
}
