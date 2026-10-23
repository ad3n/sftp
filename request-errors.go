package sftp

const (
	ErrSSHFxOk = fxerr(sshFxOk)

	ErrSSHFxEOF = fxerr(sshFxEOF)

	ErrSSHFxNoSuchFile = fxerr(sshFxNoSuchFile)

	ErrSSHFxPermissionDenied = fxerr(sshFxPermissionDenied)

	ErrSSHFxFailure = fxerr(sshFxFailure)

	ErrSSHFxBadMessage = fxerr(sshFxBadMessage)

	ErrSSHFxNoConnection = fxerr(sshFxNoConnection)

	ErrSSHFxConnectionLost = fxerr(sshFxConnectionLost)

	ErrSSHFxOpUnsupported = fxerr(sshFxOPUnsupported)

	ErrSshFxOk = ErrSSHFxOk

	ErrSshFxEof = ErrSSHFxEOF

	ErrSshFxNoSuchFile = ErrSSHFxNoSuchFile

	ErrSshFxPermissionDenied = ErrSSHFxPermissionDenied

	ErrSshFxFailure = ErrSSHFxFailure

	ErrSshFxBadMessage = ErrSSHFxBadMessage

	ErrSshFxNoConnection = ErrSSHFxNoConnection

	ErrSshFxConnectionLost = ErrSSHFxConnectionLost

	ErrSshFxOpUnsupported = ErrSSHFxOpUnsupported
)

type fxerr uint32

func (e fxerr) Error() string {
	switch e {
	case ErrSSHFxOk:
		return "OK"
	case ErrSSHFxEOF:
		return "EOF"
	case ErrSSHFxNoSuchFile:
		return "no such file"
	case ErrSSHFxPermissionDenied:
		return "permission denied"
	case ErrSSHFxBadMessage:
		return "bad message"
	case ErrSSHFxNoConnection:
		return "no connection"
	case ErrSSHFxConnectionLost:
		return "connection lost"
	case ErrSSHFxOpUnsupported:
		return "operation unsupported"
	default:
		return "failure"
	}
}
