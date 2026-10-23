package sftp

import (
	"path"
	"strings"
)

var ErrBadPattern = path.ErrBadPattern

func Match(pattern, name string) (matched bool, err error) {
	return path.Match(pattern, name)
}

func isPathSeparator(c byte) bool {
	return c == '/'
}

func Split(p string) (dir, file string) {
	return path.Split(p)
}

func (c *Client) Glob(pattern string) (matches []string, err error) {
	if !hasMeta(pattern) {
		file, err := c.Lstat(pattern)
		if err != nil {
			return nil, nil
		}

		dir, _ := Split(pattern)
		dir = cleanGlobPath(dir)
		return []string{Join(dir, file.Name())}, nil
	}

	dir, file := Split(pattern)
	dir = cleanGlobPath(dir)

	if !hasMeta(dir) {
		return c.glob(dir, file, nil)
	}

	if dir == pattern {
		return nil, ErrBadPattern
	}

	var m []string
	m, err = c.Glob(dir)
	if err != nil {
		return
	}

	for _, d := range m {
		matches, err = c.glob(d, file, matches)
		if err != nil {
			return
		}
	}

	return
}

func cleanGlobPath(path string) string {
	switch path {
	case "":
		return "."
	case "/":
		return path
	default:
		return path[0 : len(path)-1]
	}
}

func (c *Client) glob(dir, pattern string, matches []string) (m []string, e error) {
	m = matches
	fi, err := c.Stat(dir)
	if err != nil {
		return
	}

	if !fi.IsDir() {
		return
	}

	names, err := c.ReadDir(dir)
	if err != nil {
		return
	}

	for _, n := range names {
		matched, err := Match(pattern, n.Name())
		if err != nil {
			return m, err
		}

		if matched {
			m = append(m, Join(dir, n.Name()))
		}
	}

	return
}

func Join(elem ...string) string {
	return path.Join(elem...)
}

func hasMeta(path string) bool {
	return strings.ContainsAny(path, "\\*?[")
}
