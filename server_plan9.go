package sftp

import (
	"path"
	"path/filepath"
)

func (s *Server) toLocalTarget(p string) string {
	lp := filepath.FromSlash(p)

	if path.IsAbs(p) {
		tmp := lp[1:]

		if filepath.IsAbs(tmp) {
			return tmp
		}
	}

	return lp
}
