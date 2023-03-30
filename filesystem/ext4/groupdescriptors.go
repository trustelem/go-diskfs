package ext4

import (
	"encoding/binary"
	"fmt"
)

type blockGroupFlag uint16

const (
	blockGroupFlagInodesUninitialized      blockGroupFlag = 0x1
	blockGroupFlagBlockBitmapUninitialized blockGroupFlag = 0x2
	blockGroupFlagInodeTableZeroed         blockGroupFlag = 0x3
)

type blockGroupFlags struct {
	inodesUninitialized      bool
	blockBitmapUninitialized bool
	inodeTableZeroed         bool
}

// groupdescriptors is a structure holding all of the group descriptors for all of the block groups
type groupDescriptors struct {
	descriptors []*groupDescriptor
}

// groupDescriptor is a structure holding the data about a single block group
type groupDescriptor struct {
	blockGroupNumber                uint32
	blockBitmapLocation             uint64
	inodeBitmapLocation             uint64
	inodeTableLocation              uint64
	freeBlocks                      uint32
	freeInodes                      uint32
	usedDirectories                 uint32
	flags                           blockGroupFlags
	snapshotExclusionBitmapLocation uint64
	blockBitmapChecksum             uint32
	inodeBitmapChecksum             uint32
	unusedInodes                    uint32
}

func (gd *groupDescriptors) equal(a *groupDescriptors) bool {
	if (gd == nil && a != nil) || (a == nil && gd != nil) {
		return false
	}
	if gd == nil && a == nil {
		return true
	}
	desc1 := gd.descriptors
	desc2 := a.descriptors
	if len(desc1) != len(desc2) {
		return false
	}
	for i := range gd.descriptors {
		if gd.descriptors[i] != a.descriptors[i] {
			return false
		}
	}
	return true
}

// groupDescriptorsFromBytes create a groupDescriptors struct from bytes
func groupDescriptorsFromBytes(b []byte, sb *superblock) (*groupDescriptors, error) {
	gds := groupDescriptors{}

	gdSize := sb.getGroupDescriptorSize()
	count := len(b) / gdSize
	gdSlice := make([]*groupDescriptor, count)

	// go through them gdSize bytes at a time
	for i := 0; i < count; i++ {
		start := i * gdSize
		end := start + gdSize
		gd, err := groupDescriptorFromBytes(b[start:end], sb, uint32(i))
		if err == nil {
			gdSlice[i] = gd
		} else {
			return nil, fmt.Errorf("groupDescriptorFromBytes [%d] error: %v", i, err)
		}
	}
	gds.descriptors = gdSlice

	return &gds, nil
}

// groupDescriptorFromBytes create a groupDescriptor struct from bytes
func groupDescriptorFromBytes(b []byte, sb *superblock, blockGroupNumber uint32) (*groupDescriptor, error) {
	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	blockBitmapLocation := make([]byte, 8, 8)
	inodeBitmapLocation := make([]byte, 8, 8)
	inodeTableLocation := make([]byte, 8, 8)
	freeBlocks := make([]byte, 4, 4)
	freeInodes := make([]byte, 4, 4)
	usedirectories := make([]byte, 4, 4)
	snapshotExclusionBitmapLocation := make([]byte, 8, 8)
	blockBitmapChecksum := make([]byte, 4, 4)
	inodeBitmapChecksum := make([]byte, 4, 4)
	unusedInodes := make([]byte, 4, 4)

	copy(blockBitmapLocation[0:4], b[0x0:0x4])
	copy(inodeBitmapLocation[0:4], b[0x4:0x8])
	copy(inodeTableLocation[0:4], b[0x8:0xc])
	copy(freeBlocks[0:2], b[0xc:0xe])
	copy(freeInodes[0:2], b[0xe:0x10])
	copy(usedirectories[0:2], b[0x10:0x12])
	copy(snapshotExclusionBitmapLocation[0:4], b[0x14:0x18])
	copy(blockBitmapChecksum[0:2], b[0x18:0x1a])
	copy(inodeBitmapChecksum[0:2], b[0x1a:0x1c])
	copy(unusedInodes[0:2], b[0x1c:0x1e])

	if sb.features.fs64Bit {
		copy(blockBitmapLocation[4:8], b[0x20:0x24])
		copy(inodeBitmapLocation[4:8], b[0x24:0x28])
		copy(inodeTableLocation[4:8], b[0x28:0x2c])
		copy(freeBlocks[2:4], b[0x2c:0x2e])
		copy(freeInodes[2:4], b[0x2e:0x30])
		copy(usedirectories[2:4], b[0x30:0x32])
		copy(unusedInodes[2:4], b[0x32:0x34])
		copy(snapshotExclusionBitmapLocation[4:8], b[0x34:0x38])
		copy(blockBitmapChecksum[2:4], b[0x38:0x3a])
		copy(inodeBitmapChecksum[2:4], b[0x3a:0x3c])
	}

	// only bother with checking the checksum if it was not type none (pre-checksums)
	if sb.features.metadataChecksums || sb.features.gdtChecksum {
		checksum := binary.LittleEndian.Uint16(b[0x1e:0x20])
		actualChecksum := groupDescriptorChecksum(b, sb, blockGroupNumber)
		if checksum != actualChecksum {
			return nil, fmt.Errorf("checksum mismatch, passed %x, actual %x", checksum, actualChecksum)
		}
	}

	gd := groupDescriptor{
		blockGroupNumber:                blockGroupNumber,
		blockBitmapLocation:             binary.LittleEndian.Uint64(blockBitmapLocation),
		inodeBitmapLocation:             binary.LittleEndian.Uint64(inodeBitmapLocation),
		inodeTableLocation:              binary.LittleEndian.Uint64(inodeTableLocation),
		freeBlocks:                      binary.LittleEndian.Uint32(freeBlocks),
		freeInodes:                      binary.LittleEndian.Uint32(freeInodes),
		usedDirectories:                 binary.LittleEndian.Uint32(usedirectories),
		snapshotExclusionBitmapLocation: binary.LittleEndian.Uint64(snapshotExclusionBitmapLocation),
		blockBitmapChecksum:             binary.LittleEndian.Uint32(blockBitmapChecksum),
		inodeBitmapChecksum:             binary.LittleEndian.Uint32(inodeBitmapChecksum),
		unusedInodes:                    binary.LittleEndian.Uint32(unusedInodes),
		flags:                           parseBlockGroupFlags(blockGroupFlag(binary.LittleEndian.Uint16(b[0x12:0x14]))),
	}

	return &gd, nil
}

func parseBlockGroupFlags(flags blockGroupFlag) blockGroupFlags {
	f := blockGroupFlags{
		inodeTableZeroed:         flags&blockGroupFlagInodeTableZeroed == blockGroupFlagInodeTableZeroed,
		inodesUninitialized:      flags&blockGroupFlagInodesUninitialized == blockGroupFlagInodesUninitialized,
		blockBitmapUninitialized: flags&blockGroupFlagBlockBitmapUninitialized == blockGroupFlagBlockBitmapUninitialized,
	}

	return f
}

func (f *blockGroupFlags) toInt() uint16 {
	var (
		flags blockGroupFlag
	)

	// compatible flags
	if f.inodeTableZeroed {
		flags = flags | blockGroupFlagInodeTableZeroed
	}
	if f.inodesUninitialized {
		flags = flags | blockGroupFlagInodesUninitialized
	}
	if f.blockBitmapUninitialized {
		flags = flags | blockGroupFlagBlockBitmapUninitialized
	}
	return uint16(flags)
}

// groupDescriptorChecksum calculate the checksum for a block group descriptor
func groupDescriptorChecksum(b []byte, sb *superblock, blockGroup uint32) uint16 {
	const checkSumOffSet = 0x1e

	if sb.features.metadataChecksums {
		checksum32 := crc32c_update_u32(sb.checksumSeed, blockGroup)
		checksum32 = crc32c_update(checksum32, b[:checkSumOffSet])
		checksum32 = crc32c_update(checksum32, []byte{0, 0})
		offset := checkSumOffSet + 2
		if offset < len(b) {
			checksum32 = crc32c_update(checksum32, b[offset:])
		}
		checksum := uint16(checksum32 & 0xffff)

		return checksum
	}

	if sb.features.gdtChecksum {
		checksum := crc16_update(^uint16(0), sb.uuid[:])
		checksum = crc16_update_u32(checksum, blockGroup)
		checksum = crc16_update(checksum, b[:checkSumOffSet])
		offset := checkSumOffSet + 2
		if sb.features.fs64Bit && offset < len(b) {
			checksum = crc16_update(checksum, b[offset:])
		}

		return checksum
	}

	return 0
}
