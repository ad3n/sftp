package sftp

import (
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxSymlinkFollows = 5

var errTooManySymlinks = errors.New("too many symbolic links")

type (
	listerat []os.FileInfo

	root struct {
		mockErr error

		rootFile *memFile
		files    map[string]*memFile

		mu sync.Mutex
	}

	memFile struct {
		modtime time.Time
		err     error
		name    string
		symlink string
		content []byte

		mu    sync.RWMutex
		isdir bool
	}
)

func InMemHandler() Handlers {
	root := &root{
		rootFile: &memFile{name: "/", modtime: time.Now(), isdir: true},
		files:    make(map[string]*memFile),
	}
	return Handlers{root, root, root, root}
}

func (fs *root) Fileread(r *Request) (io.ReaderAt, error) {
	flags := r.Pflags()
	if !flags.Read {
		return nil, os.ErrInvalid
	}

	return fs.OpenFile(r)
}

func (fs *root) Filewrite(r *Request) (io.WriterAt, error) {
	flags := r.Pflags()
	if !flags.Write {
		return nil, os.ErrInvalid
	}

	return fs.OpenFile(r)
}

func (fs *root) OpenFile(r *Request) (WriterAtReaderAt, error) {
	if fs.mockErr != nil {
		return nil, fs.mockErr
	}

	_ = r.WithContext(r.Context())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.openfile(r.Filepath, r.Flags)
}

func (fs *root) putfile(pathname string, file *memFile) error {
	pathname, err := fs.canonName(pathname)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(pathname, "/") {
		return os.ErrInvalid
	}

	if _, err := fs.lfetch(pathname); err != os.ErrNotExist {
		return os.ErrExist
	}

	file.name = pathname
	fs.files[pathname] = file

	return nil
}

func (fs *root) openfile(pathname string, flags uint32) (*memFile, error) {
	pflags := newFileOpenFlags(flags)

	file, err := fs.fetch(pathname)
	if err == os.ErrNotExist {
		if !pflags.Creat {
			return nil, os.ErrNotExist
		}

		var count int

		link, err := fs.lfetch(pathname)
		for err == nil && link.symlink != "" {
			if pflags.Excl {
				return nil, os.ErrInvalid
			}

			if count++; count > maxSymlinkFollows {
				return nil, errTooManySymlinks
			}

			pathname = link.symlink
			link, err = fs.lfetch(pathname)
		}

		file := &memFile{
			modtime: time.Now(),
		}

		if err := fs.putfile(pathname, file); err != nil {
			return nil, err
		}

		return file, nil
	}

	if err != nil {
		return nil, err
	}

	if pflags.Creat && pflags.Excl {
		return nil, os.ErrExist
	}

	if file.IsDir() {
		return nil, os.ErrInvalid
	}

	if pflags.Trunc {
		if err := file.Truncate(0); err != nil {
			return nil, err
		}
	}

	return file, nil
}

func (fs *root) Filecmd(r *Request) error {
	if fs.mockErr != nil {
		return fs.mockErr
	}

	_ = r.WithContext(r.Context())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	switch r.Method {
	case "Setstat":
		file, err := fs.openfile(r.Filepath, sshFxfWrite)
		if err != nil {
			return err
		}

		if r.AttrFlags().Size {
			return file.Truncate(int64(r.Attributes().Size))
		}

		return nil

	case "Rename":

		if fs.exists(r.Target) {
			return os.ErrExist
		}

		return fs.rename(r.Filepath, r.Target)

	case "Rmdir":
		return fs.rmdir(r.Filepath)

	case "Remove":

		return fs.unlink(r.Filepath)

	case "Mkdir":
		return fs.mkdir(r.Filepath)

	case "Link":
		return fs.link(r.Filepath, r.Target)

	case "Symlink":

		return fs.symlink(r.Filepath, r.Target)
	}

	return errors.New("unsupported")
}

func (fs *root) rename(oldpath, newpath string) error {
	file, err := fs.lfetch(oldpath)
	if err != nil {
		return err
	}

	newpath, err = fs.canonName(newpath)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(newpath, "/") {
		return os.ErrInvalid
	}

	target, err := fs.lfetch(newpath)
	if err != os.ErrNotExist {
		if target == file {
			return nil
		}

		switch {
		case file.IsDir():

			if err := fs.rmdir(newpath); err != nil {
				return err
			}

		case target.IsDir():

			return syscall.EISDIR
		}
	}

	fs.files[newpath] = file

	if file.IsDir() {
		dirprefix := file.name + "/"

		for name, file := range fs.files {
			if after, ok := strings.CutPrefix(name, dirprefix); ok {
				newname := path.Join(newpath, after)

				fs.files[newname] = file
				file.name = newname
				delete(fs.files, name)
			}
		}
	}

	file.name = newpath
	delete(fs.files, oldpath)

	return nil
}

func (fs *root) PosixRename(r *Request) error {
	if fs.mockErr != nil {
		return fs.mockErr
	}

	_ = r.WithContext(r.Context())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.rename(r.Filepath, r.Target)
}

func (fs *root) StatVFS(r *Request) (*StatVFS, error) {
	if fs.mockErr != nil {
		return nil, fs.mockErr
	}

	return getStatVFSForPath(r.Filepath)
}

func (fs *root) mkdir(pathname string) error {
	dir := &memFile{
		modtime: time.Now(),
		isdir:   true,
	}

	return fs.putfile(pathname, dir)
}

func (fs *root) rmdir(pathname string) error {
	dir, err := fs.lfetch(pathname)
	if err != nil {
		return err
	}

	if !dir.IsDir() {
		return syscall.ENOTDIR
	}

	pathname = dir.name

	for name := range fs.files {
		if path.Dir(name) == pathname {
			return errors.New("directory not empty")
		}
	}

	delete(fs.files, pathname)

	return nil
}

func (fs *root) link(oldpath, newpath string) error {
	file, err := fs.lfetch(oldpath)
	if err != nil {
		return err
	}

	if file.IsDir() {
		return errors.New("hard link not allowed for directory")
	}

	return fs.putfile(newpath, file)
}

func (fs *root) symlink(target, linkpath string) error {
	link := &memFile{
		modtime: time.Now(),
		symlink: target,
	}

	return fs.putfile(linkpath, link)
}

func (fs *root) unlink(pathname string) error {
	file, err := fs.lfetch(pathname)
	if err != nil {
		return err
	}

	if file.IsDir() {
		return os.ErrInvalid
	}

	delete(fs.files, pathname)

	return nil
}

func (f listerat) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	var n int
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}

	n = copy(ls, f[offset:])
	if n < len(ls) {
		return n, io.EOF
	}

	return n, nil
}

func (fs *root) Filelist(r *Request) (ListerAt, error) {
	if fs.mockErr != nil {
		return nil, fs.mockErr
	}

	_ = r.WithContext(r.Context())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	switch r.Method {
	case "List":
		files, err := fs.readdir(r.Filepath)
		if err != nil {
			return nil, err
		}

		return listerat(files), nil

	case "Stat":
		file, err := fs.fetch(r.Filepath)
		if err != nil {
			return nil, err
		}

		return listerat{file}, nil
	}

	return nil, errors.New("unsupported")
}

func (fs *root) readdir(pathname string) ([]os.FileInfo, error) {
	dir, err := fs.fetch(pathname)
	if err != nil {
		return nil, err
	}

	if !dir.IsDir() {
		return nil, syscall.ENOTDIR
	}

	var files []os.FileInfo

	for name, file := range fs.files {
		if path.Dir(name) == dir.name {
			files = append(files, file)
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	return files, nil
}

func (fs *root) Readlink(pathname string) (string, error) {
	file, err := fs.lfetch(pathname)
	if err != nil {
		return "", err
	}

	if file.symlink == "" {
		return "", os.ErrInvalid
	}

	return file.symlink, nil
}

func (fs *root) Lstat(r *Request) (ListerAt, error) {
	if fs.mockErr != nil {
		return nil, fs.mockErr
	}

	_ = r.WithContext(r.Context())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	file, err := fs.lfetch(r.Filepath)
	if err != nil {
		return nil, err
	}

	return listerat{file}, nil
}

func (fs *root) returnErr(err error) {
	fs.mockErr = err
}

func (fs *root) lfetch(path string) (*memFile, error) {
	if path == "/" {
		return fs.rootFile, nil
	}

	file, ok := fs.files[path]
	if file == nil {
		if ok {
			delete(fs.files, path)
		}

		return nil, os.ErrNotExist
	}

	return file, nil
}

func (fs *root) canonName(pathname string) (string, error) {
	dirname, filename := path.Dir(pathname), path.Base(pathname)

	dir, err := fs.fetch(dirname)
	if err != nil {
		return "", err
	}

	if !dir.IsDir() {
		return "", syscall.ENOTDIR
	}

	return path.Join(dir.name, filename), nil
}

func (fs *root) exists(path string) bool {
	path, err := fs.canonName(path)
	if err != nil {
		return false
	}

	_, err = fs.lfetch(path)

	return err != os.ErrNotExist
}

func (fs *root) fetch(pathname string) (*memFile, error) {
	file, err := fs.lfetch(pathname)
	if err != nil {
		return nil, err
	}

	var count int
	for file.symlink != "" {
		if count++; count > maxSymlinkFollows {
			return nil, errTooManySymlinks
		}

		linkTarget := file.symlink
		if !path.IsAbs(linkTarget) {
			linkTarget = path.Join(path.Dir(file.name), linkTarget)
		}

		file, err = fs.lfetch(linkTarget)
		if err != nil {
			return nil, err
		}
	}

	return file, nil
}

func (f *memFile) size() int64  { return int64(len(f.content)) }
func (f *memFile) grow(n int64) { f.content = append(f.content, make([]byte, n)...) }

func (f *memFile) Name() string { return path.Base(f.name) }
func (f *memFile) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.size()
}
func (f *memFile) Mode() os.FileMode {
	if f.isdir {
		return os.FileMode(0755) | os.ModeDir
	}

	if f.symlink != "" {
		return os.FileMode(0777) | os.ModeSymlink
	}

	return os.FileMode(0644)
}
func (f *memFile) ModTime() time.Time { return f.modtime }
func (f *memFile) IsDir() bool        { return f.isdir }
func (f *memFile) Sys() any {
	return fakeFileInfoSys()
}

func (f *memFile) ReadAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return 0, f.err
	}

	if off < 0 {
		return 0, errors.New("memFile.ReadAt: negative offset")
	}

	if off >= f.size() {
		return 0, io.EOF
	}

	n := copy(b, f.content[off:])
	if n < len(b) {
		return n, io.EOF
	}

	return n, nil
}

func (f *memFile) WriteAt(b []byte, off int64) (int, error) {
	time.Sleep(time.Microsecond * time.Duration(len(b)))

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return 0, f.err
	}

	grow := int64(len(b)) + off - f.size()
	if grow > 0 {
		f.grow(grow)
	}

	return copy(f.content[off:], b), nil
}

func (f *memFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}

	grow := size - f.size()
	switch {
	case grow <= 0:

		f.content = f.content[:size]

	default:

		f.grow(grow)
	}

	return nil
}

func (f *memFile) TransferError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.err = err
}
