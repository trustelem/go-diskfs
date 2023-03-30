package ext4

import (
	"encoding/binary"
	"fmt"
)

const (
	minDirEntryLength int = 12 // actually 9 for 1-byte file length, but must be multiple of 4 bytes
	maxDirEntryLength int = 263
)

var filetypeMap = [...]fileType{
	fileTypeUnknown, fileTypeRegularFile, fileTypeDirectory, fileTypeCharacterDevice, fileTypeBlockDevice, fileTypeFifo, fileTypeSocket, fileTypeSymbolicLink,
}

// directoryEntry is a single directory entry
type directoryEntry struct {
	inode    uint32
	filename string
	fileType fileType
}

func directoryEntryFromBytes(sb *superblock, b []byte) (*directoryEntry, error) {
	if len(b) < minDirEntryLength {
		return nil, fmt.Errorf("directory entry of length %d is less than minimum %d", len(b), minDirEntryLength)
	}

	inode := binary.LittleEndian.Uint32(b[0x0:0x4])
	if inode == 0 {
		return nil, nil
	}

	de := directoryEntry{
		inode: inode,
	}

	var nameLength uint8
	if sb.features.directoryEntriesRecordFileType {
		nameLength = uint8(b[0x6])
		ft := b[0x7]
		if int(ft) > len(filetypeMap) {
			if ft == 0xde {
				// fake directory entry with checksum
				return nil, nil
			}
			return nil, fmt.Errorf("invalid filetype %x for directory entry", ft)
		}
		de.fileType = filetypeMap[ft]
	} else {
		nameLength = uint8(binary.LittleEndian.Uint16(b[0x6:0x8]))
	}

	name := b[0x8 : 0x8+nameLength]
	de.filename = string(name)
	return &de, nil
}

// parse the data blocks to get the directory entries
func parseDirEntries(sb *superblock, b []byte, f *FileSystem) ([]*directoryEntry, error) {
	entries := make([]*directoryEntry, 0, 4)
	count := 0
	for i := 0; i < len(b); count++ {
		// read the length of the first entry
		length := binary.LittleEndian.Uint16(b[i+0x4 : i+0x6])
		de, err := directoryEntryFromBytes(sb, b[i:i+int(length)])
		if err != nil {
			return nil, fmt.Errorf("failed to parse directory entry %d: %v", count, err)
		}
		if de != nil && de.filename != "." && de.filename != ".." {
			entries = append(entries, de)
		}
		i += int(length)
	}
	return entries, nil
}

// parse the data blocks to get the directory entry for a single file
func findDirEntry(sb *superblock, b []byte, f *FileSystem, filename string) (*directoryEntry, error) {
	count := 0
	for i := 0; i < len(b); count++ {
		// read the length of the first entry
		length := binary.LittleEndian.Uint16(b[i+0x4 : i+0x6])
		de, err := directoryEntryFromBytes(sb, b[i:i+int(length)])
		if err != nil {
			return nil, fmt.Errorf("failed to parse directory entry %d: %v\n", count, err)
		}
		if de != nil && de.filename == filename {
			return de, nil
		}
		i += int(length)
	}
	return nil, nil
}
