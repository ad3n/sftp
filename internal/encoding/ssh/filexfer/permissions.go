package sshfx

const (
	ModePerm FileMode = 0o0777

	ModeUserRead FileMode = 0o0400

	ModeUserWrite FileMode = 0o0200

	ModeUserExec FileMode = 0o0100

	ModeGroupRead FileMode = 0o0040

	ModeGroupWrite FileMode = 0o0020

	ModeGroupExec FileMode = 0o0010

	ModeOtherRead FileMode = 0o0004

	ModeOtherWrite FileMode = 0o0002

	ModeOtherExec FileMode = 0o0001

	ModeSetUID FileMode = 0o4000

	ModeSetGID FileMode = 0o2000

	ModeSticky FileMode = 0o1000

	ModeType FileMode = 0xF000

	ModeNamedPipe FileMode = 0x1000

	ModeCharDevice FileMode = 0x2000

	ModeDir FileMode = 0x4000

	ModeDevice FileMode = 0x6000

	ModeRegular FileMode = 0x8000

	ModeSymlink FileMode = 0xA000

	ModeSocket FileMode = 0xC000
)

type FileMode uint32

func (m FileMode) IsDir() bool {
	return (m & ModeType) == ModeDir
}

func (m FileMode) IsRegular() bool {
	return (m & ModeType) == ModeRegular
}

func (m FileMode) Perm() FileMode {
	return (m & ModePerm)
}

func (m FileMode) Type() FileMode {
	return (m & ModeType)
}

func (m FileMode) String() string {
	var buf [10]byte

	switch m.Type() {
	case ModeRegular:
		buf[0] = '-'
	case ModeDir:
		buf[0] = 'd'
	case ModeSymlink:
		buf[0] = 'l'
	case ModeDevice:
		buf[0] = 'b'
	case ModeCharDevice:
		buf[0] = 'c'
	case ModeNamedPipe:
		buf[0] = 'p'
	case ModeSocket:
		buf[0] = 's'
	default:
		buf[0] = '?'
	}

	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		switch {
		case m&(1<<uint(9-1-i)) != 0:

			buf[i+1] = byte(c)

		default:

			buf[i+1] = '-'
		}
	}

	if m&ModeSetUID != 0 {
		switch {
		case buf[3] == 'x':

			buf[3] = 's'

		default:

			buf[3] = 'S'
		}
	}

	if m&ModeSetGID != 0 {
		switch {
		case buf[6] == 'x':

			buf[6] = 's'

		default:

			buf[6] = 'S'
		}
	}

	if m&ModeSticky != 0 {
		switch {
		case buf[9] == 'x':

			buf[9] = 't'

		default:

			buf[9] = 'T'
		}
	}

	return string(buf[:])
}
