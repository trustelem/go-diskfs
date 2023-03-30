package ext4

import (
	"bytes"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestTiny(t *testing.T) {
	fsys := openTestFilesystem(t, "tiny.ext4", true)
	entries, err := fsys.ReadDir("/")
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	filemap := make(map[string]fs.FileInfo)
	for _, e := range entries {
		filemap["/"+e.Name()] = e
		t.Logf("entry: /%s %s %d\n", e.Name(), e.Mode(), e.Size())
	}

	if len(entries) != 2 {
		t.Fatalf("invalid length for Readir(/): %d", len(entries))
	}

	const filename = "/thejungle.txt"
	f, err := fsys.OpenFile(filename, os.O_RDONLY)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	f.Close()

	if len(data) != int(filemap[filename].Size()) {
		t.Fatalf("invalid read data size for %s: %d", filename, len(data))
	}

	data2, err := os.ReadFile(filepath.Join("testdata", filename[1:]))
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !bytes.Equal(data2, data) {
		t.Fatalf("content for file %s does not match", filename)
	}
}

func TestRead(t *testing.T) {
	fsys := openTestFilesystem(t, "ext4.img", false)
	if fsys == nil {
		// image not generated, skip test
		t.Skip()
	}

	filemap := make(map[string]testFSInfo)
	dirListing(t, filemap, fsys, "/")

	for _, filename := range []string{"/shortfile.txt", "/ten-meg-file.dat", "/foo/bar/short2.txt"} {
		f, err := fsys.OpenFile(filename, os.O_RDONLY)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		data, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatalf("read error: %v", err)
		} else {
			if len(data) != int(filemap[filename].size) {
				t.Fatalf("read error: wrong size %d", len(data))
			}
		}
		f.Close()
	}

	// expected image content
	// /foo
	// /foo/bar
	// /foo/bar/short2.txt
	// /foo/dir0
	// /foo/dir1
	// ...
	// /foo/dir10000
	// /lost+found
	// /seven-k-file.dat
	// /shortfile.txt
	// /six-k-file.dat
	// /ten-meg-file.dat
	// /two-k-file.dat
	// we want 10010 entries
	if len(filemap) != 10010 {
		t.Fatalf("Wrong number of files: %d", len(filemap))
	}
}

func openTestFilesystem(t *testing.T, name string, mustExist bool) *FileSystem {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if !mustExist && os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("os.Open error: %v", err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat error: %v", err)
	}
	fsys, err := Read(f, st.Size(), 0, 0)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	return fsys
}

type testFSInfo struct {
	name string
	mode fs.FileMode
	size int64
}

func dirListing(t *testing.T, filemap map[string]testFSInfo, fsys *FileSystem, rootDir string) {
	t.Helper()
	entries, err := fsys.ReadDir(rootDir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	for _, e := range entries {
		filemap[rootDir+e.Name()] = testFSInfo{
			name: e.Name(),
			mode: e.Mode(),
			size: e.Size(),
		}
		if e.IsDir() {
			dirListing(t, filemap, fsys, rootDir+e.Name()+"/")
		}
	}

}
