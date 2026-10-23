package sftp

import (
	"io"
	"os"
)

type (
	WriterAtReaderAt interface {
		io.WriterAt
		io.ReaderAt
	}

	FileReader interface {
		Fileread(*Request) (io.ReaderAt, error)
	}

	FileWriter interface {
		Filewrite(*Request) (io.WriterAt, error)
	}

	OpenFileWriter interface {
		FileWriter
		OpenFile(*Request) (WriterAtReaderAt, error)
	}

	FileCmder interface {
		Filecmd(*Request) error
	}

	PosixRenameFileCmder interface {
		FileCmder
		PosixRename(*Request) error
	}

	StatVFSFileCmder interface {
		FileCmder
		StatVFS(*Request) (*StatVFS, error)
	}

	FileLister interface {
		Filelist(*Request) (ListerAt, error)
	}

	LstatFileLister interface {
		FileLister
		Lstat(*Request) (ListerAt, error)
	}

	RealPathFileLister interface {
		FileLister
		RealPath(string) (string, error)
	}

	ReadlinkFileLister interface {
		FileLister
		Readlink(string) (string, error)
	}

	legacyRealPathFileLister interface {
		FileLister
		RealPath(string) string
	}

	NameLookupFileLister interface {
		FileLister
		LookupUserName(string) string
		LookupGroupName(string) string
	}

	ListerAt interface {
		ListAt([]os.FileInfo, int64) (int, error)
	}

	TransferError interface {
		TransferError(err error)
	}
)
