package ext4

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/util"
	uuid "github.com/satori/go.uuid"
)

// SectorSize indicates what the sector size in bytes is
type SectorSize uint16

// BlockSize indicates how many sectors are in a block
type BlockSize uint8

// BlockGroupSize indicates how many blocks are in a group, standardly 8*block_size_in_bytes

const (
	// SectorSize512 is a sector size of 512 bytes, used as the logical size for all ext4 filesystems
	SectorSize512                SectorSize = 512
	minBlocksPerGroup            int64      = 256
	BootSectorSize               SectorSize = 2 * SectorSize512
	SuperblockSize               SectorSize = 2 * SectorSize512
	BlockGroupFactor                        = 8
	DefaultInodeRatio                       = 8192
	DefaultInodeSize                        = 256
	DefaultReservedBlocksPercent            = 5
	DefaultVolumeName                       = "diskfs_ext4"
	minClusterSize                          = 128
	maxClusterSize                          = 65529
	bytesPerSlot                            = 32
	maxCharsLongFilename                    = 13
	maxBlocksPerExtent                      = 32768
	million                                 = 1000000
	billion                                 = 1000 * million
	firstNonReservedInode                   = 11 // traditional

	minBlockLogSize int = 10 /* 1024 */
	maxBlockLogSize int = 16 /* 65536 */
	minBlockSize    int = (1 << minBlockLogSize)
	maxBlockSize    int = (1 << maxBlockLogSize)

	max32Num = (1 << 32)
)

type Params struct {
	Uuid                  *uuid.UUID
	SectorsPerBlock       uint8
	BlocksPerGroup        int64
	InodeRatio            int64
	InodeCount            int64
	SparseSuperVersion    uint8
	Checksum              bool
	ClusterSize           int64
	ReservedBlocksPercent uint8
	VolumeName            string
	Features              []FeatureOpt
}

// FileSystem implements the FileSystem interface
type FileSystem struct {
	bootSector       []byte
	superblock       *superblock
	groupDescriptors *groupDescriptors
	dataBlockBitmap  bitmap
	inodeBitmap      bitmap
	blockGroups      int64
	size             int64
	start            int64
	file             util.File
}

// Read reads a filesystem from a given disk.
//
// requires the util.File where to read the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File the filesystem is expected to begin,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to read a filesystem on the entire disk. You could have a disk of size
// 20GB, and a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for working with filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
func Read(file util.File, size int64, start int64, sectorsize int64) (*FileSystem, error) {
	// blocksize must be <=0 or exactly SectorSize512 or error
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	// we do not check for ext4 max size because it is theoreticallt 1YB, which is bigger than an int64! Even 1ZB is!
	if size < Ext4MinSize {
		return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d", Ext4MinSize)
	}

	// load the information from the disk
	// read boot sector code
	bs := make([]byte, BootSectorSize)
	n, err := file.ReadAt(bs, start)
	if err != nil {
		return nil, fmt.Errorf("could not read boot sector bytes from file: %w", err)
	}
	if uint16(n) < uint16(BootSectorSize) {
		return nil, fmt.Errorf("only could read %d boot sector bytes from file", n)
	}

	// read the superblock
	// the superblock is one minimal block, i.e. 2 sectors
	superblockBytes := make([]byte, SuperblockSize)
	n, err = file.ReadAt(superblockBytes, start+int64(BootSectorSize))
	if err != nil {
		return nil, fmt.Errorf("could not read superblock bytes from file: %w", err)
	}
	if uint16(n) < uint16(SuperblockSize) {
		return nil, fmt.Errorf("cnly could read %d superblock bytes from file", n)
	}

	// convert the bytes into a superblock structure
	sb, err := superblockFromBytes(superblockBytes)
	if err != nil {
		return nil, fmt.Errorf("could not interpret superblock data: %w", err)
	}

	// now read the GDT
	// how big should the GDT be?
	numblocks := sb.blockCount
	blockGroupCount := sb.blockGroupCount()
	gdSize := sb.getGroupDescriptorSize()
	gdtSize := uint64(gdSize) * blockGroupCount
	gdtBytes := make([]byte, gdtSize)
	n, err = file.ReadAt(gdtBytes, start+int64(BootSectorSize)+int64(SuperblockSize))
	if err != nil {
		return nil, fmt.Errorf("could not read Group Descriptor Table bytes from file: %v", err)
	}
	if uint64(n) < gdtSize {
		return nil, fmt.Errorf("only could read %d Group Descriptor Table bytes from file instead of %d", n, gdtSize)
	}

	gdt, err := groupDescriptorsFromBytes(gdtBytes, sb)
	if err != nil {
		return nil, fmt.Errorf("could not interpret Group Descriptor Table data: %v", err)
	}

	return &FileSystem{
		bootSector:       bs,
		superblock:       sb,
		groupDescriptors: gdt,
		blockGroups:      int64(numblocks),
		size:             size,
		start:            start,
		file:             file,
	}, nil
}

// Type returns the type code for the filesystem. Always returns filesystem.TypeExt4
func (fs *FileSystem) Type() filesystem.Type {
	return filesystem.TypeExt4
}

// Mkdir make a directory at the given path. It is equivalent to `mkdir -p`, i.e. idempotent, in that:
//
// * It will make the entire tree path if it does not exist
// * It will not return an error if the path already exists
func (fs *FileSystem) Mkdir(p string) error {
	return errors.New("Not implemented")
}

// ReadDir return the contents of a given directory in a given filesystem.
//
// Returns a slice of os.FileInfo with all of the entries in the directory.
//
// Will return an error if the directory does not exist or is a regular file and not a directory
func (fs *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	entries, err := fs.readDir(p)
	if err != nil {
		return nil, fmt.Errorf("error reading directory %s: %v", p, err)
	}
	// once we have made it here, looping is done. We have found the final entry
	// we need to return all of the file info
	count := len(entries)
	ret := make([]os.FileInfo, 0, count)
	// inodes := make([]inode, len(entries), len(entries))
	for i, e := range entries {
		in, err := fs.readInode(e.inode)
		if err != nil {
			return nil, fmt.Errorf("could not read inode %d (name=%s) at position %d in directory: %v", e.inode, e.filename, i, err)
		}
		ret = append(ret, FileInfo{
			modTime: time.Unix(int64(in.modificationTimeSeconds), int64(in.modificationTimeNanoseconds)),
			mode:    in.toUnixPerm(e.fileType),
			name:    e.filename,
			size:    int64(in.size),
			isDir:   e.fileType.matches(fileTypeDirectory),
		})
	}

	return ret, nil
}

// OpenFile returns an io.ReadWriter from which you can read the contents of a file
// or write contents to the file
//
// accepts normal os.OpenFile flags
//
// returns an error if the file does not exist
func (fs *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	if flag&(os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_RDWR) != 0 {
		return nil, errors.New("write support not implemented")
	}

	// get the path
	dir := path.Dir(p)
	filename := path.Base(p)
	// handle rootDir special case
	if filename == "/" {
		return nil, fmt.Errorf("cannot open directory %s as file", p)
	}
	// get the directory entries
	entries, err := fs.readDir(dir)
	if err != nil {
		return nil, fmt.Errorf("could not read directory entries for %s : %v", dir, err)
	}
	// we now know that the directory exists, see if the file exists
	var targetEntry *directoryEntry
	for _, e := range entries {
		if e.filename != filename {
			continue
		}
		// cannot do anything with directories
		if e.fileType&fileTypeDirectory == fileTypeDirectory {
			return nil, fmt.Errorf("cannot open directory %s as file", p)
		}
		// if we got this far, we have found the file
		targetEntry = e
	}

	// see if the file exists
	// if the file does not exist, and is not opened for os.O_CREATE, return an error
	if targetEntry == nil {
		return nil, fmt.Errorf("target file %s does not exist and was not asked to create", p)
	}
	// get the inode
	inodeNumber := targetEntry.inode
	inode, err := fs.readInode(inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not read inode number %d: %v", inodeNumber, err)
	}
	offset := int64(0)
	if flag&os.O_APPEND == os.O_APPEND {
		offset = int64(inode.size)
	}
	return &File{
		fs:             fs,
		directoryEntry: targetEntry,
		inode:          inode,
		isReadWrite:    flag&os.O_RDWR != 0,
		isAppend:       flag&os.O_APPEND != 0,
		offset:         offset,
		filesystem:     fs,
	}, nil
}

// readInode read a single inode from disk
func (fs *FileSystem) readInode(inodeNumber uint32) (*inode, error) {
	sb := fs.superblock
	inodeSize := sb.inodeSize
	inodesPerGroup := sb.inodesPerGroup
	// figure out which block group the inode is on
	bg := (int64(inodeNumber) - 1) / int64(inodesPerGroup)
	// read the group descriptor to find out the location of the inode table
	gd := fs.groupDescriptors.descriptors[bg]
	inodeTableBlock := gd.inodeTableLocation
	inodeBytes := make([]byte, inodeSize)
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := uint64(fs.start) + inodeTableBlock*sb.blockSize
	// offsetInode is how many inodes in our inode is
	offsetInode := (int64(inodeNumber) - 1) % int64(inodesPerGroup)
	// offset is how many bytes in our inode is
	offset := offsetInode * int64(inodeSize)
	read, err := fs.file.ReadAt(inodeBytes, int64(byteStart)+offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read inode %d from offset %d of block %d from block group %d: %v", inodeNumber, offset, inodeTableBlock, bg, err)
	}
	if read != int(inodeSize) {
		return nil, fmt.Errorf("read %d bytes for inode %d instead of inode size of %d", read, inodeNumber, inodeSize)
	}
	return inodeFromBytes(inodeBytes, sb, inodeNumber)
}

// read directory entries for a given directory inode
func (fs *FileSystem) readDirectory(in *inode) ([]*directoryEntry, error) {
	// read the contents of the file across all blocks
	b, err := fs.readFileBytes(in)
	if err != nil {
		return nil, fmt.Errorf("error reading file bytes for inode %d: %v", in.number, err)
	}

	// convert into directory entries
	return parseDirEntries(fs.superblock, b, fs)
}

func (fs *FileSystem) getExtents(in *inode) ([]extent, error) {
	in.m.Lock()
	defer in.m.Unlock()
	if in.extents != nil {
		return in.extents, nil
	}
	extents, err := flattenExtentTree(fs, in.extentTree)
	if err != nil {
		return nil, err
	}
	if extents == nil {
		extents = []extent{}
	}
	in.extents = extents
	return extents, nil
}

// readFileBytes read all of the bytes for an individual file pointed at by a given inode
// normally not very useful, but helpful when reading a directory
func (fs *FileSystem) readFileBytes(in *inode) ([]byte, error) {
	// Uncomment the next line to dump the extent tree
	// in.extentTree.print(os.Stderr, fmt.Sprintf("inode %d", in.number))

	// convert the extent tree into a sorted list of extents
	extents, err := fs.getExtents(in)
	if err != nil {
		return nil, err
	}
	// walk through each one, gobbling up the bytes
	res := make([]byte, in.size)
	b := res
	for i, e := range extents {
		start := e.startingBlock * fs.superblock.blockSize
		count := uint64(e.count) * fs.superblock.blockSize
		if count > uint64(len(b)) {
			count = uint64(len(b))
		}
		read, err := fs.file.ReadAt(b[:count], int64(start))
		if err != nil {
			return nil, fmt.Errorf("failed to read bytes for extent %d: %v", i, err)
		}
		if read != int(count) {
			return nil, fmt.Errorf("Read %d bytes instead of %d for extent %d", read, count, i)
		}
		b = b[read:]
		if len(b) == 0 {
			break
		}
	}
	if len(b) != 0 {
		panic("invalid size")
	}
	return res, nil
}

// read directory entry for a given directory inode
func (fs *FileSystem) findDirectoryEntry(in *inode, filename string) (*inode, fileType, error) {
	// read the contents of the file across all blocks
	b, err := fs.readFileBytes(in)
	if err != nil {
		return nil, fileTypeUnknown, fmt.Errorf("error reading file bytes for inode %d: %v", in.number, err)
	}

	// convert into directory entries
	entry, err := findDirEntry(fs.superblock, b, fs, filename)
	if err != nil {
		return nil, fileTypeUnknown, err
	}
	if entry == nil {
		return nil, fileTypeUnknown, nil
	}
	inode, err := fs.readInode(entry.inode)
	return inode, entry.fileType, err
}

// readDir - walks down a directory tree to the last entry
// if it does not exist, it may or may not make it
func (fs *FileSystem) readDir(p string) ([]*directoryEntry, error) {
	inode, err := fs.findDirInode(p)
	if err != nil {
		return nil, err
	}
	return fs.readDirectory(inode)
}

func (fs *FileSystem) findDirInode(p string) (*inode, error) {
	root, err := fs.readInode(2)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		return root, nil
	}

	inode := root
	currentPath := ""
	dirs := strings.Split(p, "/")
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		currentPath += "/" + dir
		var fileType fileType
		inode, fileType, err = fs.findDirectoryEntry(inode, dir)
		if err != nil {
			return nil, fmt.Errorf("error reading entry for %s: %v", currentPath, err)
		}
		if inode == nil {
			return nil, fmt.Errorf("error %s does not exist", currentPath)
		}
		if fileType != fileTypeUnknown && fileType != fileTypeDirectory {
			return nil, fmt.Errorf("error %s is not a directory", currentPath)
		}
	}

	return inode, nil
}
