package ext4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// File represents a single file in an ext4 filesystem
type File struct {
	fs             *FileSystem
	directoryEntry *directoryEntry
	inode          *inode
	isReadWrite    bool
	isAppend       bool
	offset         int64
	filesystem     *FileSystem
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF
// reads from the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Read(b []byte) (int, error) {
	// TODO: inefficient implementation, use extent tree to read only the necessary blocks instead
	data, err := fl.fs.readFileBytes(fl.inode)
	if err != nil {
		return 0, err
	}
	r := bytes.NewReader(data)
	n, err := r.ReadAt(b, fl.offset)
	fl.offset += int64(n)
	return n, err
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// returns a non-nil error when n != len(b)
// writes to the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Write(p []byte) (int, error) {
	return 0, errors.New("write support not implemented")
}

// Seek set the offset to a particular point in the file
func (fl *File) Seek(offset int64, whence int) (int64, error) {
	newOffset := int64(0)
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(fl.inode.size) + offset
	case io.SeekCurrent:
		newOffset = fl.offset + offset
	}
	if newOffset < 0 {
		return fl.offset, fmt.Errorf("cannot set offset %d before start of file", offset)
	}
	fl.offset = newOffset
	return fl.offset, nil
}

func (fl *File) Close() error {
	return nil
}
