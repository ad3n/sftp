package sshfx

import (
	"bytes"
	"testing"
)

var (
	_ Packet = &LStatPacket{}

	_ Packet = &SetstatPacket{}

	_ Packet = &RemovePacket{}

	_ Packet = &MkdirPacket{}

	_ Packet = &RmdirPacket{}

	_ Packet = &RealPathPacket{}

	_ Packet = &StatPacket{}

	_ Packet = &RenamePacket{}

	_ Packet = &ReadLinkPacket{}

	_ Packet = &SymlinkPacket{}
)

func TestLStatPacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &LStatPacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		7,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = LStatPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestSetstatPacket(t *testing.T) {
	const (
		id             = 42
		path           = "/foo"
		perms FileMode = 0x87654321
	)

	p := &SetstatPacket{
		Path: "/foo",
		Attrs: Attributes{
			Flags:       AttrPermissions,
			Permissions: perms,
		},
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 21,
		9,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x04,
		0x87, 0x65, 0x43, 0x21,
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = SetstatPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}

	if p.Attrs.Flags != AttrPermissions {
		t.Errorf("UnmarshalPacketBody(): Attrs.Flags was %#x, but expected %#x", p.Attrs.Flags, AttrPermissions)
	}

	if p.Attrs.Permissions != perms {
		t.Errorf("UnmarshalPacketBody(): Attrs.Permissions was %#v, but expected %#v", p.Attrs.Permissions, perms)
	}
}

func TestRemovePacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &RemovePacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		13,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = RemovePacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestMkdirPacket(t *testing.T) {
	const (
		id             = 42
		path           = "/foo"
		perms FileMode = 0x87654321
	)

	p := &MkdirPacket{
		Path: "/foo",
		Attrs: Attributes{
			Flags:       AttrPermissions,
			Permissions: perms,
		},
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 21,
		14,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x04,
		0x87, 0x65, 0x43, 0x21,
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = MkdirPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}

	if p.Attrs.Flags != AttrPermissions {
		t.Errorf("UnmarshalPacketBody(): Attrs.Flags was %#x, but expected %#x", p.Attrs.Flags, AttrPermissions)
	}

	if p.Attrs.Permissions != perms {
		t.Errorf("UnmarshalPacketBody(): Attrs.Permissions was %#v, but expected %#v", p.Attrs.Permissions, perms)
	}
}

func TestRmdirPacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &RmdirPacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		15,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = RmdirPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestRealPathPacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &RealPathPacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		16,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = RealPathPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestStatPacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &StatPacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		17,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = StatPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestRenamePacket(t *testing.T) {
	const (
		id      = 42
		oldpath = "/foo"
		newpath = "/bar"
	)

	p := &RenamePacket{
		OldPath: oldpath,
		NewPath: newpath,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 21,
		18,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
		0x00, 0x00, 0x00, 4, '/', 'b', 'a', 'r',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = RenamePacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.OldPath != oldpath {
		t.Errorf("UnmarshalPacketBody(): OldPath was %q, but expected %q", p.OldPath, oldpath)
	}

	if p.NewPath != newpath {
		t.Errorf("UnmarshalPacketBody(): NewPath was %q, but expected %q", p.NewPath, newpath)
	}
}

func TestReadLinkPacket(t *testing.T) {
	const (
		id   = 42
		path = "/foo"
	)

	p := &ReadLinkPacket{
		Path: path,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 13,
		19,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = ReadLinkPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.Path != path {
		t.Errorf("UnmarshalPacketBody(): Path was %q, but expected %q", p.Path, path)
	}
}

func TestSymlinkPacket(t *testing.T) {
	const (
		id         = 42
		linkpath   = "/foo"
		targetpath = "/bar"
	)

	p := &SymlinkPacket{
		LinkPath:   linkpath,
		TargetPath: targetpath,
	}

	buf, err := ComposePacket(p.MarshalPacket(id, nil))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 21,
		20,
		0x00, 0x00, 0x00, 42,
		0x00, 0x00, 0x00, 4, '/', 'b', 'a', 'r',
		0x00, 0x00, 0x00, 4, '/', 'f', 'o', 'o',
	}

	if !bytes.Equal(buf, want) {
		t.Fatalf("MarshalPacket() = %X, but wanted %X", buf, want)
	}

	*p = SymlinkPacket{}

	if err := p.UnmarshalPacketBody(NewBuffer(buf[9:])); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if p.LinkPath != linkpath {
		t.Errorf("UnmarshalPacketBody(): LinkPath was %q, but expected %q", p.LinkPath, linkpath)
	}

	if p.TargetPath != targetpath {
		t.Errorf("UnmarshalPacketBody(): TargetPath was %q, but expected %q", p.TargetPath, targetpath)
	}
}
