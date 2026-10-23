package sftp

import (
	"os"
	"time"
)

const (
	sshFileXferAttrSize = 0x00000001

	sshFileXferAttrUIDGID = 0x00000002

	sshFileXferAttrPermissions = 0x00000004

	sshFileXferAttrACmodTime = 0x00000008

	sshFileXferAttrExtended = 0x80000000

	sshFileXferAttrAll = sshFileXferAttrSize | sshFileXferAttrUIDGID | sshFileXferAttrPermissions |
		sshFileXferAttrACmodTime | sshFileXferAttrExtended
)

type (
	fileInfo struct {
		stat *FileStat
		name string
	}

	FileStat struct {
		Size     uint64
		Mode     uint32
		Mtime    uint32
		Atime    uint32
		UID      uint32
		GID      uint32
		Extended []StatExtended
	}

	StatExtended struct {
		ExtType string
		ExtData string
	}

	FileInfoUidGid interface {
		os.FileInfo
		Uid() uint32
		Gid() uint32
	}

	FileInfoExtendedData interface {
		os.FileInfo
		Extended() []StatExtended
	}
)

func (fi *fileInfo) Name() string { return fi.name }

func (fi *fileInfo) Size() int64 { return int64(fi.stat.Size) }

func (fi *fileInfo) Mode() os.FileMode { return fi.stat.FileMode() }

func (fi *fileInfo) ModTime() time.Time { return fi.stat.ModTime() }

func (fi *fileInfo) IsDir() bool { return fi.Mode().IsDir() }

func (fi *fileInfo) Sys() any { return fi.stat }

func (fs *FileStat) ModTime() time.Time {
	return time.Unix(int64(fs.Mtime), 0)
}

func (fs *FileStat) AccessTime() time.Time {
	return time.Unix(int64(fs.Atime), 0)
}

func (fs *FileStat) FileMode() os.FileMode {
	return toFileMode(fs.Mode)
}

func fileInfoFromStat(stat *FileStat, name string) os.FileInfo {
	return &fileInfo{
		name: name,
		stat: stat,
	}
}

func fileStatFromInfo(fi os.FileInfo) (uint32, *FileStat) {
	mtime := fi.ModTime().Unix()
	atime := mtime
	var flags uint32 = sshFileXferAttrSize |
		sshFileXferAttrPermissions |
		sshFileXferAttrACmodTime

	fileStat := &FileStat{
		Size:  uint64(fi.Size()),
		Mode:  fromFileMode(fi.Mode()),
		Mtime: uint32(mtime),
		Atime: uint32(atime),
	}

	fileStatFromInfoOs(fi, &flags, fileStat)

	if fiExt, ok := fi.(FileInfoUidGid); ok {
		flags |= sshFileXferAttrUIDGID
		fileStat.UID = fiExt.Uid()
		fileStat.GID = fiExt.Gid()
	}

	if fiExt, ok := fi.(FileInfoExtendedData); ok {
		fileStat.Extended = fiExt.Extended()
		if len(fileStat.Extended) > 0 {
			flags |= sshFileXferAttrExtended
		}
	}

	return flags, fileStat
}
