package sftp

import (
	"syscall"
)

func statvfsFromStatfst(stat *syscall.Statfs_t) (*StatVFS, error) {
	return &StatVFS{
		Bsize:   uint64(stat.Bsize),
		Frsize:  uint64(stat.Bsize),
		Blocks:  stat.Blocks,
		Bfree:   stat.Bfree,
		Bavail:  stat.Bavail,
		Files:   stat.Files,
		Ffree:   stat.Ffree,
		Favail:  stat.Ffree,
		Fsid:    uint64(uint64(stat.Fsid.Val[1])<<32 | uint64(stat.Fsid.Val[0])),
		Flag:    uint64(stat.Flags),
		Namemax: 1024,
	}, nil
}
