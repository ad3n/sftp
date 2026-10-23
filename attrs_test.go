package sftp

import (
	"os"
)

var _ os.FileInfo = new(fileInfo)
