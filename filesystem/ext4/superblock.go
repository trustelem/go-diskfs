package ext4

import (
	"encoding/binary"
	"fmt"
	"time"
)

type filesystemState uint16
type errorBehaviour uint16
type osFlag uint32
type feature uint32
type hashAlgorithm byte
type mountOption uint32
type encryptionAlgorithm byte

const (
	// superblockSIgnature is the signature for every superblock
	superblockSignature uint16 = 0xef53
	// optional states for the filesystem
	fsStateCleanlyUnmounted filesystemState = 0x0001
	fsStateErrors           filesystemState = 0x0002
	fsStateOrphansRecovered filesystemState = 0x0004
	// how to handle erorrs
	errorsContinue        errorBehaviour = 1
	errorsRemountReadOnly errorBehaviour = 2
	errorsPanic           errorBehaviour = 3
	// checksum type
	crc32c byte = 1
	// oses
	osLinux   osFlag = 0
	osHurd    osFlag = 1
	osMasix   osFlag = 2
	osFreeBSD osFlag = 3
	osLites   osFlag = 4
	// compatible, incompatible, and compatibleReadOnly feature flags
	compatFeatureDirectoryPreAllocate               feature = 0x1
	compatFeatureImagicInodes                       feature = 0x2
	compatFeatureHasJournal                         feature = 0x4
	compatFeatureExtendedAttributes                 feature = 0x8
	compatFeatureReservedGDTBlocksForExpansion      feature = 0x10
	compatFeatureDirectoryIndices                   feature = 0x20
	compatFeatureLazyBlockGroup                     feature = 0x40
	compatFeatureExcludeInode                       feature = 0x80
	compatFeatureExcludeBitmap                      feature = 0x100
	compatFeatureSparseSuperBlockV2                 feature = 0x200
	incompatFeatureCompression                      feature = 0x1
	incompatFeatureDirectoryEntriesRecordFileType   feature = 0x2
	incompatFeatureRecoveryNeeded                   feature = 0x4
	incompatFeatureSeparateJournalDevice            feature = 0x8
	incompatFeatureMetaBlockGroups                  feature = 0x10
	incompatFeatureExtents                          feature = 0x40
	incompatFeature64Bit                            feature = 0x80
	incompatFeatureMultipleMountProtection          feature = 0x100
	incompatFeatureFlexBlockGroups                  feature = 0x200
	incompatFeatureExtendedAttributeInodes          feature = 0x400
	incompatFeatureDataInDirectoryEntries           feature = 0x1000
	incompatFeatureMetadataChecksumSeedInSuperblock feature = 0x2000
	incompatFeatureLargeDirectory                   feature = 0x4000
	incompatFeatureDataInInode                      feature = 0x8000
	incompatFeatureEncryptInodes                    feature = 0x10000
	roCompatFeatureSparseSuperblock                 feature = 0x1
	roCompatFeatureLargeFile                        feature = 0x2
	roCompatFeatureBtreeDirectory                   feature = 0x4
	roCompatFeatureHugeFile                         feature = 0x8
	roCompatFeatureGDTChecksum                      feature = 0x10
	roCompatFeatureLargeSubdirectoryCount           feature = 0x20
	roCompatFeatureLargeInodes                      feature = 0x40
	roCompatFeatureSnapshot                         feature = 0x80
	roCompatFeatureQuota                            feature = 0x100
	roCompatFeatureBigalloc                         feature = 0x200
	roCompatFeatureMetadataChecksums                feature = 0x400
	roCompatFeatureReplicas                         feature = 0x800
	roCompatFeatureReadOnly                         feature = 0x1000
	roCompatFeatureProjectQuotas                    feature = 0x2000
	// hash algorithms for htree directory entries
	hashLegacy          hashAlgorithm = 0x0
	hashHalfMD4         hashAlgorithm = 0x1
	hashTea             hashAlgorithm = 0x2
	hashLegacyUnsigned  hashAlgorithm = 0x3
	hashHalfMD4Unsigned hashAlgorithm = 0x4
	hashTeaUnsigned     hashAlgorithm = 0x5
	// default mount options
	mountPrintDebugInfo                 mountOption = 0x1
	mountNewFilesGidContainingDirectory mountOption = 0x2
	mountUserspaceExtendedAttributes    mountOption = 0x4
	mountPosixACLs                      mountOption = 0x8
	mount16BitUIDs                      mountOption = 0x10
	mountJournalDataAndMetadata         mountOption = 0x20
	mountFlushBeforeJournal             mountOption = 0x40
	mountUnorderingDataMetadata         mountOption = 0x60
	mountDisableWriteFlushes            mountOption = 0x100
	mountTrackMetadataBlocks            mountOption = 0x200
	mountDiscardDeviceSupport           mountOption = 0x400
	mountDisableDelayedAllocation       mountOption = 0x800
	// miscellaneous flags
	flagSignedDirectoryHash   = 0x0001
	flagUnsignedDirectoryHash = 0x0002
	flagTestDevCode           = 0x0004
	// encryption algorithms
	encryptionAlgorithmInvalid   encryptionAlgorithm = 1
	encryptionAlgorithm256AESXTS encryptionAlgorithm = 2
	encryptionAlgorithm256AESGCM encryptionAlgorithm = 3
	encryptionAlgorithm256AESCBC encryptionAlgorithm = 4
)

// journalBackup is a backup in the superblock of the journal's inode i_block[] array and size
type journalBackup struct {
	iBlocks   [15]uint32
	iSizeHigh uint64
	iSize     uint64
}

// mountOptions is a structure holding which default mount options are set
type mountOptions struct {
	printDebugInfo                 bool
	newFilesGidContainingDirectory bool
	userspaceExtendedAttributes    bool
	posixACLs                      bool
	use16BitUIDs                   bool
	journalDataAndMetadata         bool
	flushBeforeJournal             bool
	unorderingDataMetadata         bool
	disableWriteFlushes            bool
	trackMetadataBlocks            bool
	discardDeviceSupport           bool
	disableDelayedAllocation       bool
}

// Superblock is a structure holding the ext4 superblock
type superblock struct {
	inodeCount                   uint32
	blockCount                   uint64
	reservedBlocks               uint64
	freeBlocks                   uint64
	freeInodes                   uint32
	firstDataBlock               uint32
	blockSize                    uint64
	clusterSize                  uint64
	blocksPerGroup               uint32
	clustersPerGroup             uint32
	inodesPerGroup               uint32
	mountTime                    time.Time
	writeTime                    time.Time
	mountCount                   uint16
	mountsToFsck                 uint16
	filesystemState              filesystemState
	errorBehaviour               errorBehaviour
	minorRevision                uint16
	lastCheck                    time.Time
	checkInterval                uint32
	creatorOS                    osFlag
	revisionLevel                uint32
	reservedBlocksDefaultUID     uint16
	reservedBlocksDefaultGID     uint16
	firstNonReservedInode        uint32
	inodeSize                    uint16
	blockGroup                   uint16
	features                     featureFlags
	uuid                         [16]byte
	volumeLabel                  string
	lastMountedDirectory         string
	algorithmUsageBitmap         uint32
	preallocationBlocks          byte
	preallocationDirectoryBlocks byte
	reservedGDTBlocks            uint16
	journalSuperblockUUID        [16]byte
	journalInode                 uint32
	journalDeviceNumber          uint32
	orphanedInodesStart          uint32
	hashTreeSeed                 [4]uint32
	hashVersion                  hashAlgorithm
	groupDescriptorSize          uint16 // Size of group descriptors, in bytes, if the 64bit incompat feature flag is set
	defaultMountOptions          mountOptions
	firstMetablockGroup          uint32
	mkfsTime                     time.Time
	journalBackup                journalBackup
	// 64-bit mode features
	inodeMinBytes                uint16
	inodeReserveBytes            uint16
	miscFlags                    miscFlags
	raidStride                   uint16
	multiMountPreventionInterval uint16
	multiMountProtectionBlock    uint64
	raidStripeWidth              uint32
	logGroupsPerFlex             uint64
	checksumType                 byte
	totalKBWritten               uint64
	snapshotInodeNumber          uint32
	snapshotID                   uint32
	snapshotReservedBlocks       uint64
	snapshotStartInode           uint32
	errorCount                   uint32
	errorFirstTime               time.Time
	errorFirstInode              uint32
	errorFirstBlock              uint64
	errorFirstFunction           string
	errorFirstLine               uint32
	errorLastTime                time.Time
	errorLastInode               uint32
	errorLastLine                uint32
	errorLastBlock               uint64
	errorLastFunction            string
	mountOptions                 string
	userQuotaInode               uint32
	groupQuotaInode              uint32
	overheadBlocks               uint32
	backupSuperblockBlockGroups  [2]uint32
	encryptionAlgorithms         [4]encryptionAlgorithm
	encryptionSalt               [16]byte
	lostFoundInode               uint32
	projectQuotaInode            uint32
	checksumSeed                 uint32
}

func bytesToUUIDBytes(in []byte) []byte {
	// first 3 sections (4 bytes, 2 bytes, 2 bytes) are little-endian, last 2 section are big-endian
	b := make([]byte, 0, 16)
	b = append(b, in[0:16]...)
	tmpb := b[0:4]
	reverseSlice(tmpb)

	tmpb = b[4:6]
	reverseSlice(tmpb)

	tmpb = b[6:8]
	reverseSlice(tmpb)
	return b
}

func reverseSlice(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// FSInformationSectorFromBytes create an FSInformationSector struct from bytes
func superblockFromBytes(b []byte) (*superblock, error) {
	bLen := len(b)
	if bLen != int(SuperblockSize) {
		return nil, fmt.Errorf("cannot read superblock from %d bytes instead of expected %d", bLen, SuperblockSize)
	}

	// check the magic signature
	actualSignature := binary.LittleEndian.Uint16(b[0x38:0x3a])
	if actualSignature != superblockSignature {
		return nil, fmt.Errorf("erroneous signature at location 0x38 was %x instead of expected %x", actualSignature, superblockSignature)
	}

	sb := superblock{}

	// first read feature flags of various types
	compatFlags := feature(binary.LittleEndian.Uint32(b[0x5c:0x60]))
	incompatFlags := feature(binary.LittleEndian.Uint32(b[0x60:0x64]))
	roCompatFlags := feature(binary.LittleEndian.Uint32(b[0x64:0x68]))
	// track which ones are set
	sb.features = parseFeatureFlags(compatFlags, incompatFlags, roCompatFlags)

	sb.inodeCount = binary.LittleEndian.Uint32(b[0:4])

	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	var blockCount [8]byte
	var reservedBlocks [8]byte
	var freeBlocks [8]byte

	copy(blockCount[0:4], b[0x4:0x8])
	copy(reservedBlocks[0:4], b[0x8:0xc])
	copy(freeBlocks[0:4], b[0xc:0x10])

	if sb.features.fs64Bit {
		copy(blockCount[4:8], b[0x150:0x154])
		copy(reservedBlocks[4:8], b[0x154:0x158])
		copy(freeBlocks[4:8], b[0x158:0x15c])
	}
	sb.blockCount = binary.LittleEndian.Uint64(blockCount[:])
	sb.reservedBlocks = binary.LittleEndian.Uint64(reservedBlocks[:])
	sb.freeBlocks = binary.LittleEndian.Uint64(freeBlocks[:])

	sb.freeInodes = binary.LittleEndian.Uint32(b[0x10:0x14])
	sb.firstDataBlock = binary.LittleEndian.Uint32(b[0x14:0x18])
	sb.blockSize = uint64(1) << (10 + binary.LittleEndian.Uint32(b[0x18:0x1c]))
	sb.clusterSize = uint64(1) << (binary.LittleEndian.Uint32(b[0x1c:0x20]))
	sb.blocksPerGroup = binary.LittleEndian.Uint32(b[0x20:0x24])
	if sb.features.bigalloc {
		sb.clustersPerGroup = binary.LittleEndian.Uint32(b[0x24:0x28])
	}
	sb.inodesPerGroup = binary.LittleEndian.Uint32(b[0x28:0x2c])
	sb.mountTime = time.Unix(int64(binary.LittleEndian.Uint32(b[0x2c:0x30])), 0)
	sb.writeTime = time.Unix(int64(binary.LittleEndian.Uint32(b[0x30:0x34])), 0)
	sb.mountCount = binary.LittleEndian.Uint16(b[0x34:0x36])
	sb.mountsToFsck = binary.LittleEndian.Uint16(b[0x36:0x38])

	sb.filesystemState = filesystemState(binary.LittleEndian.Uint16(b[0x3a:0x3c]))
	sb.errorBehaviour = errorBehaviour(binary.LittleEndian.Uint16(b[0x3c:0x3e]))

	sb.minorRevision = binary.LittleEndian.Uint16(b[0x3e:0x40])
	sb.lastCheck = time.Unix(int64(binary.LittleEndian.Uint32(b[0x40:0x44])), 0)
	sb.checkInterval = binary.LittleEndian.Uint32(b[0x44:0x48])

	sb.creatorOS = osFlag(binary.LittleEndian.Uint32(b[0x48:0x4c]))
	sb.revisionLevel = binary.LittleEndian.Uint32(b[0x4c:0x50])
	sb.reservedBlocksDefaultUID = binary.LittleEndian.Uint16(b[0x50:0x52])
	sb.reservedBlocksDefaultGID = binary.LittleEndian.Uint16(b[0x52:0x54])

	sb.firstNonReservedInode = binary.LittleEndian.Uint32(b[0x54:0x58])
	sb.inodeSize = binary.LittleEndian.Uint16(b[0x58:0x5a])
	sb.blockGroup = binary.LittleEndian.Uint16(b[0x5a:0x5c])

	copy(sb.uuid[:], b[0x68:0x78])
	sb.volumeLabel = string(b[0x78:0x88])
	sb.lastMountedDirectory = string(b[0x88:0xc8])
	sb.algorithmUsageBitmap = binary.LittleEndian.Uint32(b[0xc8:0xcc])

	sb.preallocationBlocks = b[0xcc]
	sb.preallocationDirectoryBlocks = b[0xcd]
	sb.reservedGDTBlocks = binary.LittleEndian.Uint16(b[0xce:0xd0])

	copy(sb.journalSuperblockUUID[:], b[0xd0:0xe0])
	sb.journalInode = binary.LittleEndian.Uint32(b[0xe0:0xe4])
	sb.journalDeviceNumber = binary.LittleEndian.Uint32(b[0xe4:0xe8])
	sb.orphanedInodesStart = binary.LittleEndian.Uint32(b[0xe8:0xec])

	sb.hashTreeSeed[0] = binary.LittleEndian.Uint32(b[0xec:0xf0])
	sb.hashTreeSeed[1] = binary.LittleEndian.Uint32(b[0xf0:0xf4])
	sb.hashTreeSeed[2] = binary.LittleEndian.Uint32(b[0xf4:0xf8])
	sb.hashTreeSeed[3] = binary.LittleEndian.Uint32(b[0xf8:0xfc])

	sb.hashVersion = hashAlgorithm(b[0xfc])

	sb.groupDescriptorSize = binary.LittleEndian.Uint16(b[0xfe:0x100])

	sb.defaultMountOptions = parseMountOptions(mountOption(binary.LittleEndian.Uint32(b[0x100:0x104])))
	sb.firstMetablockGroup = binary.LittleEndian.Uint32(b[0x104:0x108])
	sb.mkfsTime = time.Unix(int64(binary.LittleEndian.Uint32(b[0x108:0x10c])), 0)

	journalBackupType := b[0xfd]
	if journalBackupType == 0 {
		var journalBackupArray [15]uint32
		startJournalBackup := 0x10c
		for i := 0; i < 15; i++ {
			start := startJournalBackup + 4*i
			end := startJournalBackup + 4*i + 4
			journalBackupArray[i] = binary.LittleEndian.Uint32(b[start:end])
		}
		var iSizeBytes [8]byte

		copy(iSizeBytes[0:4], b[startJournalBackup+4*16:startJournalBackup+4*17])
		copy(iSizeBytes[4:8], b[startJournalBackup+4*15:startJournalBackup+4*16])

		sb.journalBackup = journalBackup{
			iSize:   binary.LittleEndian.Uint64(iSizeBytes[:]),
			iBlocks: journalBackupArray,
		}
	}

	sb.inodeMinBytes = binary.LittleEndian.Uint16(b[0x15c:0x15e])
	sb.inodeReserveBytes = binary.LittleEndian.Uint16(b[0x15e:0x160])
	sb.miscFlags = parseMiscFlags(binary.LittleEndian.Uint32(b[0x160:0x164]))

	sb.raidStride = binary.LittleEndian.Uint16(b[0x164:0x166])
	sb.raidStripeWidth = binary.LittleEndian.Uint32(b[0x170:0x174])

	sb.multiMountPreventionInterval = binary.LittleEndian.Uint16(b[0x166:0x168])
	sb.multiMountProtectionBlock = binary.LittleEndian.Uint64(b[0x168:0x170])

	sb.logGroupsPerFlex = uint64(1) << (b[0x174])

	sb.checksumType = b[0x175] // only valid one is 1 or 0
	if sb.checksumType != crc32c && sb.checksumType != 0 {
		return nil, fmt.Errorf("cannot read superblock: invalid checksum type %d, only valid is %d | 0", sb.checksumType, crc32c)
	}

	// b[0x176:0x178] are reserved padding

	sb.totalKBWritten = binary.LittleEndian.Uint64(b[0x178:0x180])

	sb.snapshotInodeNumber = binary.LittleEndian.Uint32(b[0x180:0x184])
	sb.snapshotID = binary.LittleEndian.Uint32(b[0x184:0x188])
	sb.snapshotReservedBlocks = binary.LittleEndian.Uint64(b[0x188:0x190])
	sb.snapshotStartInode = binary.LittleEndian.Uint32(b[0x190:0x194])

	// errors
	sb.errorCount = binary.LittleEndian.Uint32(b[0x194:0x198])
	sb.errorFirstTime = time.Unix(int64(binary.LittleEndian.Uint32(b[0x198:0x19c])), 0)
	sb.errorFirstInode = binary.LittleEndian.Uint32(b[0x19c:0x1a0])
	sb.errorFirstBlock = binary.LittleEndian.Uint64(b[0x1a0:0x1a8])
	sb.errorFirstFunction = string(b[0x1a8:0x1c8])
	sb.errorFirstLine = binary.LittleEndian.Uint32(b[0x1c8:0x1cc])
	sb.errorLastTime = time.Unix(int64(binary.LittleEndian.Uint32(b[0x1cc:0x1d0])), 0)
	sb.errorLastInode = binary.LittleEndian.Uint32(b[0x1d0:0x1d4])
	sb.errorLastLine = binary.LittleEndian.Uint32(b[0x1d4:0x1d8])
	sb.errorLastBlock = binary.LittleEndian.Uint64(b[0x1d8:0x1e0])
	sb.errorLastFunction = string(b[0x1e0:0x200])

	sb.mountOptions = string(b[0x200:0x240])
	sb.userQuotaInode = binary.LittleEndian.Uint32(b[0x240:0x244])
	sb.groupQuotaInode = binary.LittleEndian.Uint32(b[0x244:0x248])
	// overheadBlocks *always* is 0
	sb.overheadBlocks = binary.LittleEndian.Uint32(b[0x248:0x24c])
	sb.backupSuperblockBlockGroups = [2]uint32{
		binary.LittleEndian.Uint32(b[0x24c:0x250]),
		binary.LittleEndian.Uint32(b[0x250:0x254]),
	}
	for i := range sb.encryptionAlgorithms {
		sb.encryptionAlgorithms[i] = encryptionAlgorithm(b[0x254+i])
	}
	for i := range sb.encryptionSalt {
		sb.encryptionSalt[i] = b[0x258+i]
	}

	sb.lostFoundInode = binary.LittleEndian.Uint32(b[0x268:0x26c])
	sb.projectQuotaInode = binary.LittleEndian.Uint32(b[0x26c:0x270])

	if sb.features.metadataChecksumSeedInSuperblock {
		sb.checksumSeed = binary.LittleEndian.Uint32(b[0x270:0x274])
	} else if sb.features.metadataChecksums || sb.features.extendedAttributeInodes {
		sb.checksumSeed = crc32c_update(^uint32(0), sb.uuid[:])
	}

	// b[0x274:0x3fc] are reserved for zero padding

	// checksum
	checksum := binary.LittleEndian.Uint32(b[0x3fc:0x400])

	// calculate the checksum and validate - we use crc32c
	if sb.features.metadataChecksums {
		actualChecksum := crc32c_update(^uint32(0), b[0:0x3fc])
		if actualChecksum != checksum {
			return nil, fmt.Errorf("invalid superblock checksum, actual was %x, on disk was %x", actualChecksum, checksum)
		}
	}

	return &sb, nil
}

func (sb *superblock) blockGroupCount() uint64 {
	// blocks_count = (ext4_blocks_count(es) -
	// le32_to_cpu(es->s_first_data_block) +
	// EXT4_BLOCKS_PER_GROUP(sb) - 1);
	// do_div(blocks_count, EXT4_BLOCKS_PER_GROUP(sb));

	blocksCount := sb.blockCount - uint64(sb.firstDataBlock) + uint64(sb.blocksPerGroup) - 1
	blocksCount = blocksCount / uint64(sb.blocksPerGroup)

	return blocksCount
}

func (sb *superblock) getGroupDescriptorSize() int {
	const groupDescriptorSize = 32
	const groupDescriptorSize64Bit = 64
	if sb.features.fs64Bit {
		return int(sb.groupDescriptorSize)
		// return groupDescriptorSize64Bit
	}
	return groupDescriptorSize
}

func parseMountOptions(flags mountOption) mountOptions {
	m := mountOptions{
		printDebugInfo:                 flags&mountPrintDebugInfo == mountPrintDebugInfo,
		newFilesGidContainingDirectory: flags&mountNewFilesGidContainingDirectory == mountNewFilesGidContainingDirectory,
		userspaceExtendedAttributes:    flags&mountUserspaceExtendedAttributes == mountUserspaceExtendedAttributes,
		posixACLs:                      flags&mountPosixACLs == mountPosixACLs,
		use16BitUIDs:                   flags&mount16BitUIDs == mount16BitUIDs,
		journalDataAndMetadata:         flags&mountJournalDataAndMetadata == mountJournalDataAndMetadata,
		flushBeforeJournal:             flags&mountFlushBeforeJournal == mountFlushBeforeJournal,
		unorderingDataMetadata:         flags&mountUnorderingDataMetadata == mountUnorderingDataMetadata,
		disableWriteFlushes:            flags&mountDisableWriteFlushes == mountDisableWriteFlushes,
		trackMetadataBlocks:            flags&mountTrackMetadataBlocks == mountTrackMetadataBlocks,
		discardDeviceSupport:           flags&mountDiscardDeviceSupport == mountDiscardDeviceSupport,
		disableDelayedAllocation:       flags&mountDisableDelayedAllocation == mountDisableDelayedAllocation,
	}
	return m
}

func (m *mountOptions) toInt() uint32 {
	var flags mountOption

	if m.printDebugInfo {
		flags = flags | mountPrintDebugInfo
	}
	if m.newFilesGidContainingDirectory {
		flags = flags | mountNewFilesGidContainingDirectory
	}
	if m.userspaceExtendedAttributes {
		flags = flags | mountUserspaceExtendedAttributes
	}
	if m.posixACLs {
		flags = flags | mountPosixACLs
	}
	if m.use16BitUIDs {
		flags = flags | mount16BitUIDs
	}
	if m.journalDataAndMetadata {
		flags = flags | mountJournalDataAndMetadata
	}
	if m.flushBeforeJournal {
		flags = flags | mountFlushBeforeJournal
	}
	if m.unorderingDataMetadata {
		flags = flags | mountUnorderingDataMetadata
	}
	if m.disableWriteFlushes {
		flags = flags | mountDisableWriteFlushes
	}
	if m.trackMetadataBlocks {
		flags = flags | mountTrackMetadataBlocks
	}
	if m.discardDeviceSupport {
		flags = flags | mountDiscardDeviceSupport
	}
	if m.disableDelayedAllocation {
		flags = flags | mountDisableDelayedAllocation
	}

	return uint32(flags)
}
