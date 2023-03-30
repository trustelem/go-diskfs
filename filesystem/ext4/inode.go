package ext4

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"sync"
)

type inodeFlag uint32
type fileType uint16

func (ft fileType) matches(ft2 fileType) bool {
	return ft&ft2 == ft2
}

const (
	inodeSize                        int    = 128
	extentTreeHeaderLength                  = 12
	extentTreeEntryLength                   = 12
	extentHeaderSignature            uint16 = 0xf30a
	extentTreeMaxDepth               int    = 5
	extentInodeMaxEntries                   = 4
	inodeFlagSecureDeletion                 = 0x1
	inodeFlagPreserveForUndeletion          = 0x2
	inodeFlagCompressed                     = 0x4
	inodeFlagSynchronous                    = 0x8
	inodeFlagImmutable                      = 0x10
	inodeFlagAppendOnly                     = 0x20
	inodeFlagNoDump                         = 0x40
	inodeFlagNoAccessTimeUpdate             = 0x80
	inodeFlagDirtyCompressed                = 0x100
	inodeFlagCompressedClusters             = 0x200
	inodeFlagNoCompress                     = 0x400
	inodeFlagEncryptedInode                 = 0x800
	inodeFlagHashedDirectoryIndexes         = 0x1000
	inodeFlagAFSMagicDirectory              = 0x2000
	inodeFlagAlwaysJournal                  = 0x4000
	inodeFlagNoMergeTail                    = 0x8000
	inodeFlagSyncDirectoryData              = 0x10000
	inodeFlagTopDirectory                   = 0x20000
	inodeFlagHugeFile                       = 0x40000
	inodeFlagUsesExtents                    = 0x80000
	inodeFlagExtendedAttributes             = 0x200000
	inodeFlagBlocksPastEOF                  = 0x400000
	inodeFlagSnapshot                       = 0x1000000
	inodeFlagDeletingSnapshot               = 0x4000000
	inodeFlagCompletedSnapshotShrink        = 0x8000000
	inodeFlagInlineData                     = 0x10000000
	inodeFlagInheritProject                 = 0x20000000

	fileTypeUnknown         fileType = 0x0000
	fileTypeFifo            fileType = 0x1000
	fileTypeCharacterDevice fileType = 0x2000
	fileTypeDirectory       fileType = 0x4000
	fileTypeBlockDevice     fileType = 0x6000
	fileTypeRegularFile     fileType = 0x8000
	fileTypeSymbolicLink    fileType = 0xA000
	fileTypeSocket          fileType = 0xC000

	filePermissionsOwnerExecute = 0x40
	filePermissionsOwnerWrite   = 0x80
	filePermissionsOwnerRead    = 0x100
	filePermissionsGroupExecute = 0x8
	filePermissionsGroupWrite   = 0x10
	filePermissionsGroupRead    = 0x20
	filePermissionsOtherExecute = 0x1
	filePermissionsOtherWrite   = 0x2
	filePermissionsOtherRead    = 0x4
)

// mountOptions is a structure holding flags for an inode
type inodeFlags struct {
	secureDeletion          bool
	preserveForUndeletion   bool
	compressed              bool
	synchronous             bool
	immutable               bool
	appendOnly              bool
	noDump                  bool
	noAccessTimeUpdate      bool
	dirtyCompressed         bool
	compressedClusters      bool
	noCompress              bool
	encryptedInode          bool
	hashedDirectoryIndexes  bool
	AFSMagicDirectory       bool
	alwaysJournal           bool
	noMergeTail             bool
	syncDirectoryData       bool
	topDirectory            bool
	hugeFile                bool
	usesExtents             bool
	extendedAttributes      bool
	blocksPastEOF           bool
	snapshot                bool
	deletingSnapshot        bool
	completedSnapshotShrink bool
	inlineData              bool
	inheritProject          bool
}

type filePermissions struct {
	read    bool
	write   bool
	execute bool
}

// extentTree represents a tree of extents in an inode
//
//	it could be represented just as extents or []extent
//	but that would require recreation of the tree every time, which is a mess
type extentTree struct {
	depth       uint16                   // the depth of tree below here. 0 means now children trees, all extents
	entries     uint16                   // number of entries, either extents or children
	max         uint16                   // maximum number of entries - children or extents - allowed at this level
	blockNumber uint64                   // block number where the children live
	fileBlock   uint32                   // extents or children of this cover from file block fileBlock onwards
	extents     []extent                 // for depth = 0, the actual extents; for depth > 0, empty
	children    []extentTreeInternalNode // for depth = 0, empty; for depth > 0, the children
}

// func (e *extentTree) ReadAt(b []byte, off int64) (n int, err error) {
// }

// inode is a structure holding the data about an inode
type inode struct {
	number                      uint32
	mode                        uint16
	permissionsOther            filePermissions
	permissionsGroup            filePermissions
	permissionsOwner            filePermissions
	fileType                    fileType
	owner                       uint32
	group                       uint32
	size                        uint64
	accessTimeSeconds           uint64
	changeTimeSeconds           uint64
	creationTimeSeconds         uint64
	modificationTimeSeconds     uint64
	accessTimeNanoseconds       uint32
	changeTimeNanoseconds       uint32
	creationTimeNanoseconds     uint32
	modificationTimeNanoseconds uint32
	deletionTime                uint32
	hardLinks                   uint16
	blocks                      uint64
	filesystemBlocks            bool
	flags                       *inodeFlags
	version                     uint64
	nfsFileVersion              uint32
	inodeSize                   uint16
	project                     uint32
	extentTree                  *extentTree
	// cache
	m       sync.Mutex
	extents []extent
	// computed
	cSumSeed uint32
}

// inodeFromBytes create an inode struct from bytes
func inodeFromBytes(b []byte, sb *superblock, number uint32) (*inode, error) {
	//fmt.Fprintf(os.Stderr, "inodeFromBytes(%d) (len=%d)\n%s\n", number, len(b), hex.Dump(b))

	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	var owner [4]byte
	var size [8]byte
	var group [4]byte
	var accessTime [8]byte
	var changeTime [8]byte
	var modifyTime [8]byte
	var createTime [8]byte
	var version [8]byte
	var checksumBytes [4]byte

	iGeneration := binary.LittleEndian.Uint32(b[0x64:0x68])

	// checksum before using the data
	copy(checksumBytes[0:2], b[0x7c:0x7e])
	if len(b) > 128 {
		copy(checksumBytes[2:4], b[0x82:0x84])
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes[:])

	var checksumSeed uint32
	if sb.features.metadataChecksums {
		checksumSeed = crc32c_update_u32(sb.checksumSeed, number)
		checksumSeed = crc32c_update_u32(checksumSeed, iGeneration)

		actualChecksum := inodeChecksum(b, sb, checksumSeed)
		if actualChecksum != checksum {
			cerr := fmt.Errorf("inode checksum mismatch (i_generation=%d), on-disk %x vs calculated %x", iGeneration, checksum, actualChecksum)
			fmt.Fprintf(os.Stderr, "warning %s\n", cerr)
			// return nil, cerr // fmt.Errorf("inode checksum mismatch, on-disk %x vs calculated %x", checksum, actualChecksum)
		}

	}

	mode := binary.LittleEndian.Uint16(b[0x0:0x2])

	copy(owner[0:2], b[0x2:0x4])
	copy(owner[2:4], b[0x78:0x7a])
	copy(group[0:2], b[0x18:0x20])
	copy(group[2:4], b[0x7a:0x7c])
	copy(size[0:4], b[0x4:0x8])
	copy(size[4:8], b[0x6c:0x70])
	copy(version[0:4], b[0x24:0x28])
	if len(b) > 128 {
		copy(version[4:8], b[0x98:0x9c])
	}

	// get the the times
	// the structure is as follows:
	//  original 32 bits (0:4) are seconds. Add (to the left) 2 more bits from the 32
	//  the remaining 30 bites are nanoseconds
	copy(accessTime[0:4], b[0x8:0xc])
	copy(changeTime[0:4], b[0xc:0x10])
	copy(modifyTime[0:4], b[0x10:0x14])
	if len(b) > 128 {
		copy(createTime[0:4], b[0x90:0x94])
		// take the two bits relevant and add to fifth byte
		accessTime[4] = b[0x8c] & 0x3
		changeTime[4] = b[0x84] & 0x3
		modifyTime[4] = b[0x88] & 0x3
		createTime[4] = b[0x94] & 0x3
	}

	accessTimeSeconds := binary.LittleEndian.Uint64(accessTime[:])
	changeTimeSeconds := binary.LittleEndian.Uint64(changeTime[:])
	modifyTimeSeconds := binary.LittleEndian.Uint64(modifyTime[:])
	createTimeSeconds := binary.LittleEndian.Uint64(createTime[:])

	var accessTimeNanoseconds uint32
	var changeTimeNanoseconds uint32
	var modifyTimeNanoseconds uint32
	var createTimeNanoseconds uint32
	if len(b) > 128 {
		// now get the nanoseconds by using the upper 30 bits
		accessTimeNanoseconds = binary.LittleEndian.Uint32(b[0x8c:0x90]) >> 2
		changeTimeNanoseconds = binary.LittleEndian.Uint32(b[0x84:0x88]) >> 2
		modifyTimeNanoseconds = binary.LittleEndian.Uint32(b[0x88:0x8c]) >> 2
		createTimeNanoseconds = binary.LittleEndian.Uint32(b[0x94:0x98]) >> 2
	}

	flagsNum := binary.LittleEndian.Uint32(b[0x20:0x24])

	flags := parseInodeFlags(flagsNum)

	blocksLow := binary.LittleEndian.Uint32(b[0x1c:0x20])
	blocksHigh := binary.LittleEndian.Uint16(b[0x74:0x76])
	var (
		blocks           uint64
		filesystemBlocks bool
	)

	hugeFile := sb.features.hugeFile
	switch {
	case !hugeFile:
		// just 512-byte blocks
		blocks = uint64(blocksLow)
		filesystemBlocks = false
	case hugeFile && !flags.hugeFile:
		// larger number of 512-byte blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = false
	default:
		// larger number of filesystem blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = true
	}

	// last but not least, parse the extentTree
	extentTree, err := parseExtentTree(b[0x28:0x64], 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error parsing extent tree: %v", err)
	}

	var project uint32

	inodeSize := uint16(len(b))

	if len(b) > 128 {
		inodeSize = binary.LittleEndian.Uint16(b[0x80:0x82]) + 128
		binary.LittleEndian.Uint32(b[0x9c:0x100])
	}

	i := inode{
		number:                      number,
		mode:                        mode,
		permissionsGroup:            parseGroupPermissions(mode),
		permissionsOwner:            parseOwnerPermissions(mode),
		permissionsOther:            parseOtherPermissions(mode),
		fileType:                    parseFileType(mode),
		owner:                       binary.LittleEndian.Uint32(owner[:]),
		group:                       binary.LittleEndian.Uint32(group[:]),
		size:                        binary.LittleEndian.Uint64(size[:]),
		hardLinks:                   binary.LittleEndian.Uint16(b[0x1a:0x1c]),
		blocks:                      blocks,
		filesystemBlocks:            filesystemBlocks,
		flags:                       &flags,
		nfsFileVersion:              iGeneration,
		version:                     binary.LittleEndian.Uint64(version[:]),
		inodeSize:                   inodeSize,
		deletionTime:                binary.LittleEndian.Uint32(b[0x14:0x18]),
		accessTimeSeconds:           accessTimeSeconds,
		changeTimeSeconds:           changeTimeSeconds,
		creationTimeSeconds:         createTimeSeconds,
		modificationTimeSeconds:     modifyTimeSeconds,
		accessTimeNanoseconds:       accessTimeNanoseconds,
		changeTimeNanoseconds:       changeTimeNanoseconds,
		creationTimeNanoseconds:     createTimeNanoseconds,
		modificationTimeNanoseconds: modifyTimeNanoseconds,
		project:                     project,
		extentTree:                  extentTree,
		cSumSeed:                    checksumSeed,
	}

	return &i, nil
}

func parseOwnerPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOwnerExecute == filePermissionsOwnerExecute,
		write:   mode&filePermissionsOwnerWrite == filePermissionsOwnerWrite,
		read:    mode&filePermissionsOwnerRead == filePermissionsOwnerRead,
	}
}
func parseGroupPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsGroupExecute == filePermissionsGroupExecute,
		write:   mode&filePermissionsGroupWrite == filePermissionsGroupWrite,
		read:    mode&filePermissionsGroupRead == filePermissionsGroupRead,
	}
}
func parseOtherPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOtherExecute == filePermissionsOtherExecute,
		write:   mode&filePermissionsOtherWrite == filePermissionsOtherWrite,
		read:    mode&filePermissionsOtherRead == filePermissionsOtherRead,
	}
}

func (i *inode) toUnixPerm(ft fileType) fs.FileMode {
	p := fs.FileMode(i.mode)
	if ft.matches(fileTypeFifo) {
		p |= fs.ModeNamedPipe
	}
	if ft.matches(fileTypeCharacterDevice) {
		p |= fs.ModeCharDevice
	}
	if ft.matches(fileTypeDirectory) {
		p |= fs.ModeDir
	}
	if ft.matches(fileTypeBlockDevice) {
		p |= fs.ModeDevice
	}
	if ft.matches(fileTypeSocket) {
		p |= fs.ModeSocket
	}
	if ft.matches(fileTypeSymbolicLink) {
		p |= fs.ModeSymlink
	}
	return p
}

func parseFileType(mode uint16) fileType {
	var f fileType
	switch {
	case fileType(mode)&fileTypeFifo == fileTypeFifo:
		f = fileTypeFifo
	case fileType(mode)&fileTypeBlockDevice == fileTypeBlockDevice:
		f = fileTypeBlockDevice
	case fileType(mode)&fileTypeCharacterDevice == fileTypeCharacterDevice:
		f = fileTypeCharacterDevice
	case fileType(mode)&fileTypeDirectory == fileTypeDirectory:
		f = fileTypeDirectory
	case fileType(mode)&fileTypeRegularFile == fileTypeRegularFile:
		f = fileTypeRegularFile
	case fileType(mode)&fileTypeSocket == fileTypeSocket:
		f = fileTypeSocket
	case fileType(mode)&fileTypeSymbolicLink == fileTypeSymbolicLink:
		f = fileTypeSymbolicLink
	}
	return f
}

func parseInodeFlags(flags uint32) inodeFlags {
	return inodeFlags{
		secureDeletion:          flags&inodeFlagSecureDeletion == inodeFlagSecureDeletion,
		preserveForUndeletion:   flags&inodeFlagPreserveForUndeletion == inodeFlagPreserveForUndeletion,
		compressed:              flags&inodeFlagCompressed == inodeFlagCompressed,
		synchronous:             flags&inodeFlagSynchronous == inodeFlagSynchronous,
		immutable:               flags&inodeFlagImmutable == inodeFlagImmutable,
		appendOnly:              flags&inodeFlagAppendOnly == inodeFlagAppendOnly,
		noDump:                  flags&inodeFlagNoDump == inodeFlagNoDump,
		noAccessTimeUpdate:      flags&inodeFlagNoAccessTimeUpdate == inodeFlagNoAccessTimeUpdate,
		dirtyCompressed:         flags&inodeFlagDirtyCompressed == inodeFlagDirtyCompressed,
		compressedClusters:      flags&inodeFlagCompressedClusters == inodeFlagCompressedClusters,
		noCompress:              flags&inodeFlagNoCompress == inodeFlagNoCompress,
		encryptedInode:          flags&inodeFlagEncryptedInode == inodeFlagEncryptedInode,
		hashedDirectoryIndexes:  flags&inodeFlagHashedDirectoryIndexes == inodeFlagHashedDirectoryIndexes,
		AFSMagicDirectory:       flags&inodeFlagAFSMagicDirectory == inodeFlagAFSMagicDirectory,
		alwaysJournal:           flags&inodeFlagAlwaysJournal == inodeFlagAlwaysJournal,
		noMergeTail:             flags&inodeFlagNoMergeTail == inodeFlagNoMergeTail,
		syncDirectoryData:       flags&inodeFlagSyncDirectoryData == inodeFlagSyncDirectoryData,
		topDirectory:            flags&inodeFlagTopDirectory == inodeFlagTopDirectory,
		hugeFile:                flags&inodeFlagHugeFile == inodeFlagHugeFile,
		usesExtents:             flags&inodeFlagUsesExtents == inodeFlagUsesExtents,
		extendedAttributes:      flags&inodeFlagExtendedAttributes == inodeFlagExtendedAttributes,
		blocksPastEOF:           flags&inodeFlagBlocksPastEOF == inodeFlagBlocksPastEOF,
		snapshot:                flags&inodeFlagSnapshot == inodeFlagSnapshot,
		deletingSnapshot:        flags&inodeFlagDeletingSnapshot == inodeFlagDeletingSnapshot,
		completedSnapshotShrink: flags&inodeFlagCompletedSnapshotShrink == inodeFlagCompletedSnapshotShrink,
		inlineData:              flags&inodeFlagInlineData == inodeFlagInlineData,
		inheritProject:          flags&inodeFlagInheritProject == inodeFlagInheritProject,
	}
}

// inodeChecksum calculate the checksum for an inode
func inodeChecksum(b []byte, sb *superblock, checksumSeed uint32) uint32 {
	checksum := crc32c_update(checksumSeed, b[:0x7c])
	checksum = crc32c_update(checksum, []byte{0, 0})
	checksum = crc32c_update(checksum, b[0x7e:0x80])
	if sb.inodeSize > 128 /* old std inode size for ext2 / ext3 */ {
		checksum = crc32c_update(checksum, b[0x80:0x82])
		checksum = crc32c_update(checksum, []byte{0, 0})
		checksum = crc32c_update(checksum, b[0x84:])
	} else {
		checksum = checksum & 0xFFFF
	}
	return checksum
}
