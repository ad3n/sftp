package sftp

import (
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	typeDirectory = "d"

	typeFile = "[^d]"
)

func TestRunLsWithExamplesDirectory(t *testing.T) {
	path := "examples"
	item, _ := os.Stat(path)
	result := runLs(nil, item)
	runLsTestHelper(t, result, typeDirectory, path)
}

func TestRunLsWithSourceFile(t *testing.T) {
	path := "ls_formatting_test.go"
	item, _ := os.Stat(path)
	result := runLs(nil, item)
	runLsTestHelper(t, result, typeFile, path)
}

func TestRunLsWithExamplesDirectoryWithOSLookup(t *testing.T) {
	path := "examples"
	item, _ := os.Stat(path)
	result := runLs(osIDLookup{}, item)
	runLsTestHelper(t, result, typeDirectory, path)
}

func TestRunLsWithSourceFileWithOSLookup(t *testing.T) {
	path := "ls_formatting_test.go"
	item, _ := os.Stat(path)
	result := runLs(osIDLookup{}, item)
	runLsTestHelper(t, result, typeFile, path)
}

func runLsTestHelper(t *testing.T, result, expectedType, path string) {
	t.Log(result)

	sparce := strings.Split(result, " ")

	var fields []string
	for _, field := range sparce {
		if field == "" {
			continue
		}

		fields = append(fields, field)
	}

	perms, linkCnt, user, group, size := fields[0], fields[1], fields[2], fields[3], fields[4]
	dateTime := strings.Join(fields[5:8], " ")
	filename := fields[8]

	if runtime.GOOS == "zos" {
		user = strings.ToLower(user)
		group = strings.ToLower(group)
	}

	const (
		rwxs = "[-r][-w][-xsS]"
		rwxt = "[-r][-w][-xtT]"
	)
	if ok, err := regexp.MatchString("^"+expectedType+rwxs+rwxs+rwxt+"$", perms); !ok {
		if err != nil {
			t.Fatal("unexpected error:", err)
		}

		t.Errorf("runLs(%q): permission field mismatch, expected dir, got: %#v, err: %#v", path, perms, err)
	}

	const (
		number = "(?:[0-9]+)"
	)
	if ok, err := regexp.MatchString("^"+number+"$", linkCnt); !ok {
		if err != nil {
			t.Fatal("unexpected error:", err)
		}

		t.Errorf("runLs(%q): link count field mismatch, got: %#v, err: %#v", path, linkCnt, err)
	}

	const (
		name = "(?:[A-Za-z_][-A-Z.a-z0-9_]*)"
	)
	if ok, err := regexp.MatchString("^(?:"+number+"|"+name+")+$", user); !ok {
		if err != nil {
			t.Fatal("unexpected error:", err)
		}

		t.Errorf("runLs(%q): username / uid mismatch, expected user, got: %#v, err: %#v", path, user, err)
	}

	if ok, err := regexp.MatchString("^(?:"+number+"|"+name+")+$", group); !ok {
		if err != nil {
			t.Fatal("unexpected error:", err)
		}

		t.Errorf("runLs(%q): groupname / gid mismatch, expected group, got: %#v, err: %#v", path, group, err)
	}

	if ok, err := regexp.MatchString("^"+number+"$", size); !ok {
		if err != nil {
			t.Fatal("unexpected error:", err)
		}

		t.Errorf("runLs(%q): filesize field mismatch, expected size in bytes, got: %#v, err: %#v", path, size, err)
	}

	_, err := time.Parse("Jan 2 15:04", dateTime)
	if err != nil {
		_, err = time.Parse("Jan 2 2006", dateTime)
		if err != nil {
			t.Errorf("runLs.dateTime = %#v should match `Jan 2 15:04` or `Jan 2 2006`: %+v", dateTime, err)
		}
	}

	if path != filename {
		t.Errorf("runLs.filename = %#v, expected: %#v", filename, path)
	}
}
