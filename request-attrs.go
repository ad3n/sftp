package sftp

type (
	FileOpenFlags struct {
		Read, Write, Append, Creat, Trunc, Excl bool
	}

	FileAttrFlags struct {
		Size, UidGid, Permissions, Acmodtime bool
	}
)

func newFileOpenFlags(flags uint32) FileOpenFlags {
	return FileOpenFlags{
		Read:   flags&sshFxfRead != 0,
		Write:  flags&sshFxfWrite != 0,
		Append: flags&sshFxfAppend != 0,
		Creat:  flags&sshFxfCreat != 0,
		Trunc:  flags&sshFxfTrunc != 0,
		Excl:   flags&sshFxfExcl != 0,
	}
}

func (r *Request) Pflags() FileOpenFlags {
	return newFileOpenFlags(r.Flags)
}

func newFileAttrFlags(flags uint32) FileAttrFlags {
	return FileAttrFlags{
		Size:        (flags & sshFileXferAttrSize) != 0,
		UidGid:      (flags & sshFileXferAttrUIDGID) != 0,
		Permissions: (flags & sshFileXferAttrPermissions) != 0,
		Acmodtime:   (flags & sshFileXferAttrACmodTime) != 0,
	}
}

func (r *Request) AttrFlags() FileAttrFlags {
	return newFileAttrFlags(r.Flags)
}

func (r *Request) Attributes() *FileStat {
	fs, _, _ := unmarshalFileStat(r.Flags, r.Attrs)
	return fs
}
