package sshfx

import (
	"fmt"
)

const (
	StatusOK = Status(iota)
	StatusEOF
	StatusNoSuchFile
	StatusPermissionDenied
	StatusFailure
	StatusBadMessage
	StatusNoConnection
	StatusConnectionLost
	StatusOPUnsupported

	StatusV4InvalidHandle
	StatusV4NoSuchPath
	StatusV4FileAlreadyExists
	StatusV4WriteProtect

	StatusV4NoMedia

	StatusV5NoSpaceOnFilesystem
	StatusV5QuotaExceeded
	StatusV5UnknownPrincipal
	StatusV5LockConflict

	StatusV6DirNotEmpty
	StatusV6NotADirectory
	StatusV6InvalidFilename
	StatusV6LinkLoop

	StatusV6CannotDelete
	StatusV6InvalidParameter
	StatusV6FileIsADirectory
	StatusV6ByteRangeLockConflict
	StatusV6ByteRangeLockRefused
	StatusV6DeletePending

	StatusV6FileCorrupt

	StatusV6OwnerInvalid
	StatusV6GroupInvalid

	StatusV6NoMatchingByteRangeLock
)

type Status uint32

func (s Status) Error() string {
	return s.String()
}

func (s Status) Is(target error) bool {
	if target, ok := target.(*StatusPacket); ok {
		return target.StatusCode == s
	}

	return s == target
}

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "SSH_FX_OK"
	case StatusEOF:
		return "SSH_FX_EOF"
	case StatusNoSuchFile:
		return "SSH_FX_NO_SUCH_FILE"
	case StatusPermissionDenied:
		return "SSH_FX_PERMISSION_DENIED"
	case StatusFailure:
		return "SSH_FX_FAILURE"
	case StatusBadMessage:
		return "SSH_FX_BAD_MESSAGE"
	case StatusNoConnection:
		return "SSH_FX_NO_CONNECTION"
	case StatusConnectionLost:
		return "SSH_FX_CONNECTION_LOST"
	case StatusOPUnsupported:
		return "SSH_FX_OP_UNSUPPORTED"
	case StatusV4InvalidHandle:
		return "SSH_FX_INVALID_HANDLE"
	case StatusV4NoSuchPath:
		return "SSH_FX_NO_SUCH_PATH"
	case StatusV4FileAlreadyExists:
		return "SSH_FX_FILE_ALREADY_EXISTS"
	case StatusV4WriteProtect:
		return "SSH_FX_WRITE_PROTECT"
	case StatusV4NoMedia:
		return "SSH_FX_NO_MEDIA"
	case StatusV5NoSpaceOnFilesystem:
		return "SSH_FX_NO_SPACE_ON_FILESYSTEM"
	case StatusV5QuotaExceeded:
		return "SSH_FX_QUOTA_EXCEEDED"
	case StatusV5UnknownPrincipal:
		return "SSH_FX_UNKNOWN_PRINCIPAL"
	case StatusV5LockConflict:
		return "SSH_FX_LOCK_CONFLICT"
	case StatusV6DirNotEmpty:
		return "SSH_FX_DIR_NOT_EMPTY"
	case StatusV6NotADirectory:
		return "SSH_FX_NOT_A_DIRECTORY"
	case StatusV6InvalidFilename:
		return "SSH_FX_INVALID_FILENAME"
	case StatusV6LinkLoop:
		return "SSH_FX_LINK_LOOP"
	case StatusV6CannotDelete:
		return "SSH_FX_CANNOT_DELETE"
	case StatusV6InvalidParameter:
		return "SSH_FX_INVALID_PARAMETER"
	case StatusV6FileIsADirectory:
		return "SSH_FX_FILE_IS_A_DIRECTORY"
	case StatusV6ByteRangeLockConflict:
		return "SSH_FX_BYTE_RANGE_LOCK_CONFLICT"
	case StatusV6ByteRangeLockRefused:
		return "SSH_FX_BYTE_RANGE_LOCK_REFUSED"
	case StatusV6DeletePending:
		return "SSH_FX_DELETE_PENDING"
	case StatusV6FileCorrupt:
		return "SSH_FX_FILE_CORRUPT"
	case StatusV6OwnerInvalid:
		return "SSH_FX_OWNER_INVALID"
	case StatusV6GroupInvalid:
		return "SSH_FX_GROUP_INVALID"
	case StatusV6NoMatchingByteRangeLock:
		return "SSH_FX_NO_MATCHING_BYTE_RANGE_LOCK"
	default:
		return fmt.Sprintf("SSH_FX_UNKNOWN(%d)", s)
	}
}
