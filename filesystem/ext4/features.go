package ext4

// featureFlags is a structure holding which flags are set - compatible, incompatible and read-only compatible
type featureFlags struct {
	// compatible, incompatible, and compatibleReadOnly feature flags
	directoryPreAllocate             bool
	imagicInodes                     bool
	hasJournal                       bool
	extendedAttributes               bool
	reservedGDTBlocksForExpansion    bool
	directoryIndices                 bool
	lazyBlockGroup                   bool
	excludeInode                     bool
	excludeBitmap                    bool
	sparseSuperBlockV2               bool
	compression                      bool
	directoryEntriesRecordFileType   bool
	recoveryNeeded                   bool
	separateJournalDevice            bool
	metaBlockGroups                  bool
	extents                          bool
	fs64Bit                          bool
	multipleMountProtection          bool
	flexBlockGroups                  bool
	extendedAttributeInodes          bool
	dataInDirectoryEntries           bool
	metadataChecksumSeedInSuperblock bool
	largeDirectory                   bool
	dataInInode                      bool
	encryptInodes                    bool
	sparseSuperblock                 bool
	largeFile                        bool
	btreeDirectory                   bool
	hugeFile                         bool
	gdtChecksum                      bool
	largeSubdirectoryCount           bool
	largeInodes                      bool
	snapshot                         bool
	quota                            bool
	bigalloc                         bool
	metadataChecksums                bool
	replicas                         bool
	readOnly                         bool
	projectQuotas                    bool
}

func parseFeatureFlags(compatFlags feature, incompatFlags feature, roCompatFlags feature) featureFlags {
	f := featureFlags{
		directoryPreAllocate:             compatFlags&compatFeatureDirectoryPreAllocate == compatFeatureDirectoryPreAllocate,
		imagicInodes:                     compatFlags&compatFeatureImagicInodes == compatFeatureImagicInodes,
		hasJournal:                       compatFlags&compatFeatureHasJournal == compatFeatureHasJournal,
		extendedAttributes:               compatFlags&compatFeatureExtendedAttributes == compatFeatureExtendedAttributes,
		reservedGDTBlocksForExpansion:    compatFlags&compatFeatureReservedGDTBlocksForExpansion == compatFeatureReservedGDTBlocksForExpansion,
		directoryIndices:                 compatFlags&compatFeatureDirectoryIndices == compatFeatureDirectoryIndices,
		lazyBlockGroup:                   compatFlags&compatFeatureLazyBlockGroup == compatFeatureLazyBlockGroup,
		excludeInode:                     compatFlags&compatFeatureExcludeInode == compatFeatureExcludeInode,
		excludeBitmap:                    compatFlags&compatFeatureExcludeBitmap == compatFeatureExcludeBitmap,
		sparseSuperBlockV2:               compatFlags&compatFeatureSparseSuperBlockV2 == compatFeatureSparseSuperBlockV2,
		compression:                      incompatFlags&incompatFeatureCompression == incompatFeatureCompression,
		directoryEntriesRecordFileType:   incompatFlags&incompatFeatureDirectoryEntriesRecordFileType == incompatFeatureDirectoryEntriesRecordFileType,
		recoveryNeeded:                   incompatFlags&incompatFeatureRecoveryNeeded == incompatFeatureRecoveryNeeded,
		separateJournalDevice:            incompatFlags&incompatFeatureSeparateJournalDevice == incompatFeatureSeparateJournalDevice,
		metaBlockGroups:                  incompatFlags&incompatFeatureMetaBlockGroups == incompatFeatureMetaBlockGroups,
		extents:                          incompatFlags&incompatFeatureExtents == incompatFeatureExtents,
		fs64Bit:                          incompatFlags&incompatFeature64Bit == incompatFeature64Bit,
		multipleMountProtection:          incompatFlags&incompatFeatureMultipleMountProtection == incompatFeatureMultipleMountProtection,
		flexBlockGroups:                  incompatFlags&incompatFeatureFlexBlockGroups == incompatFeatureFlexBlockGroups,
		extendedAttributeInodes:          incompatFlags&incompatFeatureExtendedAttributeInodes == incompatFeatureExtendedAttributeInodes,
		dataInDirectoryEntries:           incompatFlags&incompatFeatureDataInDirectoryEntries == incompatFeatureDataInDirectoryEntries,
		metadataChecksumSeedInSuperblock: incompatFlags&incompatFeatureMetadataChecksumSeedInSuperblock == incompatFeatureMetadataChecksumSeedInSuperblock,
		largeDirectory:                   incompatFlags&incompatFeatureLargeDirectory == incompatFeatureLargeDirectory,
		dataInInode:                      incompatFlags&incompatFeatureDataInInode == incompatFeatureDataInInode,
		encryptInodes:                    incompatFlags&incompatFeatureEncryptInodes == incompatFeatureEncryptInodes,
		sparseSuperblock:                 roCompatFlags&roCompatFeatureSparseSuperblock == roCompatFeatureSparseSuperblock,
		largeFile:                        roCompatFlags&roCompatFeatureLargeFile == roCompatFeatureLargeFile,
		btreeDirectory:                   roCompatFlags&roCompatFeatureBtreeDirectory == roCompatFeatureBtreeDirectory,
		hugeFile:                         roCompatFlags&roCompatFeatureHugeFile == roCompatFeatureHugeFile,
		gdtChecksum:                      roCompatFlags&roCompatFeatureGDTChecksum == roCompatFeatureGDTChecksum,
		largeSubdirectoryCount:           roCompatFlags&roCompatFeatureLargeSubdirectoryCount == roCompatFeatureLargeSubdirectoryCount,
		largeInodes:                      roCompatFlags&roCompatFeatureLargeInodes == roCompatFeatureLargeInodes,
		snapshot:                         roCompatFlags&roCompatFeatureSnapshot == roCompatFeatureSnapshot,
		quota:                            roCompatFlags&roCompatFeatureQuota == roCompatFeatureQuota,
		bigalloc:                         roCompatFlags&roCompatFeatureBigalloc == roCompatFeatureBigalloc,
		metadataChecksums:                roCompatFlags&roCompatFeatureMetadataChecksums == roCompatFeatureMetadataChecksums,
		replicas:                         roCompatFlags&roCompatFeatureReplicas == roCompatFeatureReplicas,
		readOnly:                         roCompatFlags&roCompatFeatureReadOnly == roCompatFeatureReadOnly,
		projectQuotas:                    roCompatFlags&roCompatFeatureProjectQuotas == roCompatFeatureProjectQuotas,
	}

	return f
}

func (f *featureFlags) toInts() (uint32, uint32, uint32) {
	var (
		compatFlags   feature
		incompatFlags feature
		roCompatFlags feature
	)

	// compatible flags
	if f.directoryPreAllocate {
		compatFlags = compatFlags | compatFeatureDirectoryPreAllocate
	}
	if f.imagicInodes {
		compatFlags = compatFlags | compatFeatureImagicInodes
	}
	if f.hasJournal {
		compatFlags = compatFlags | compatFeatureHasJournal
	}
	if f.extendedAttributes {
		compatFlags = compatFlags | compatFeatureExtendedAttributes
	}
	if f.reservedGDTBlocksForExpansion {
		compatFlags = compatFlags | compatFeatureReservedGDTBlocksForExpansion
	}
	if f.directoryIndices {
		compatFlags = compatFlags | compatFeatureDirectoryIndices
	}
	if f.lazyBlockGroup {
		compatFlags = compatFlags | compatFeatureLazyBlockGroup
	}
	if f.excludeInode {
		compatFlags = compatFlags | compatFeatureExcludeInode
	}
	if f.excludeBitmap {
		compatFlags = compatFlags | compatFeatureExcludeBitmap
	}
	if f.sparseSuperBlockV2 {
		compatFlags = compatFlags | compatFeatureSparseSuperBlockV2
	}

	// incompatible flags
	if f.compression {
		incompatFlags = incompatFlags | incompatFeatureCompression
	}
	if f.directoryEntriesRecordFileType {
		incompatFlags = incompatFlags | incompatFeatureDirectoryEntriesRecordFileType
	}
	if f.recoveryNeeded {
		incompatFlags = incompatFlags | incompatFeatureRecoveryNeeded
	}
	if f.separateJournalDevice {
		incompatFlags = incompatFlags | incompatFeatureSeparateJournalDevice
	}
	if f.metaBlockGroups {
		incompatFlags = incompatFlags | incompatFeatureMetaBlockGroups
	}
	if f.extents {
		incompatFlags = incompatFlags | incompatFeatureExtents
	}
	if f.fs64Bit {
		incompatFlags = incompatFlags | incompatFeature64Bit
	}
	if f.multipleMountProtection {
		incompatFlags = incompatFlags | incompatFeatureMultipleMountProtection
	}
	if f.flexBlockGroups {
		incompatFlags = incompatFlags | incompatFeatureFlexBlockGroups
	}
	if f.extendedAttributeInodes {
		incompatFlags = incompatFlags | incompatFeatureExtendedAttributeInodes
	}
	if f.dataInDirectoryEntries {
		incompatFlags = incompatFlags | incompatFeatureDataInDirectoryEntries
	}
	if f.metadataChecksumSeedInSuperblock {
		incompatFlags = incompatFlags | incompatFeatureMetadataChecksumSeedInSuperblock
	}
	if f.largeDirectory {
		incompatFlags = incompatFlags | incompatFeatureLargeDirectory
	}
	if f.dataInInode {
		incompatFlags = incompatFlags | incompatFeatureDataInInode
	}
	if f.encryptInodes {
		incompatFlags = incompatFlags | incompatFeatureEncryptInodes
	}

	// read only compatible flags
	if f.sparseSuperblock {
		roCompatFlags = roCompatFlags | roCompatFeatureSparseSuperblock
	}
	if f.largeFile {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeFile
	}
	if f.btreeDirectory {
		roCompatFlags = roCompatFlags | roCompatFeatureBtreeDirectory
	}
	if f.hugeFile {
		roCompatFlags = roCompatFlags | roCompatFeatureHugeFile
	}
	if f.gdtChecksum {
		roCompatFlags = roCompatFlags | roCompatFeatureGDTChecksum
	}
	if f.largeSubdirectoryCount {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeSubdirectoryCount
	}
	if f.largeInodes {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeInodes
	}
	if f.snapshot {
		roCompatFlags = roCompatFlags | roCompatFeatureSnapshot
	}
	if f.quota {
		roCompatFlags = roCompatFlags | roCompatFeatureQuota
	}
	if f.bigalloc {
		roCompatFlags = roCompatFlags | roCompatFeatureBigalloc
	}
	if f.metadataChecksums {
		roCompatFlags = roCompatFlags | roCompatFeatureMetadataChecksums
	}
	if f.replicas {
		roCompatFlags = roCompatFlags | roCompatFeatureReplicas
	}
	if f.readOnly {
		roCompatFlags = roCompatFlags | roCompatFeatureReadOnly
	}
	if f.projectQuotas {
		roCompatFlags = roCompatFlags | roCompatFeatureProjectQuotas
	}

	return uint32(compatFlags), uint32(incompatFlags), uint32(roCompatFlags)
}

// default features
/*
	base_features = sparse_super,large_file,filetype,resize_inode,dir_index,ext_attr
	features = has_journal,extent,huge_file,flex_bg,uninit_bg,64bit,dir_nlink,extra_isize
*/
var defaultFeatureFlags = featureFlags{
	largeFile:          true,
	hugeFile:           true,
	sparseSuperblock:   true,
	flexBlockGroups:    true,
	hasJournal:         true,
	extents:            true,
	fs64Bit:            true,
	extendedAttributes: true,
}

type FeatureOpt func(*featureFlags)

func WithFeatureDirectoryPreAllocate(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.directoryPreAllocate = enable
	}
}
func WithFeatureImagicInodes(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.imagicInodes = enable
	}
}
func WithFeatureHasJournal(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.hasJournal = enable
	}
}
func WithFeatureExtendedAttributes(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.extendedAttributes = enable
	}
}
func WithFeatureReservedGDTBlocksForExpansion(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.reservedGDTBlocksForExpansion = enable
	}
}
func WithFeatureDirectoryIndices(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.directoryIndices = enable
	}
}
func WithFeatureLazyBlockGroup(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.lazyBlockGroup = enable
	}
}
func WithFeatureExcludeInode(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.excludeInode = enable
	}
}
func WithFeatureExcludeBitmap(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.excludeBitmap = enable
	}
}
func WithFeatureSparseSuperBlockV2(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.sparseSuperBlockV2 = enable
	}
}
func WithFeatureCompression(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.compression = enable
	}
}
func WithFeatureDirectoryEntriesRecordFileType(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.directoryEntriesRecordFileType = enable
	}
}
func WithFeatureRecoveryNeeded(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.recoveryNeeded = enable
	}
}
func WithFeatureSeparateJournalDevice(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.directoryPreAllocate = enable
	}
}
func WithFeatureMetaBlockGroups(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.metaBlockGroups = enable
	}
}
func WithFeatureExtents(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.extents = enable
	}
}
func WithFeatureFS64Bit(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.fs64Bit = enable
	}
}
func WithFeatureMultipleMountProtection(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.multipleMountProtection = enable
	}
}
func WithFeatureFlexBlockGroups(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.flexBlockGroups = enable
	}
}
func WithFeatureExtendedAttributeInodes(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.extendedAttributeInodes = enable
	}
}
func WithFeatureDataInDirectoryEntries(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.dataInDirectoryEntries = enable
	}
}
func WithFeatureMetadataChecksumSeedInSuperblock(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.metadataChecksumSeedInSuperblock = enable
	}
}
func WithFeatureLargeDirectory(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.largeDirectory = enable
	}
}
func WithFeatureDataInInode(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.dataInInode = enable
	}
}
func WithFeatureEncryptInodes(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.encryptInodes = enable
	}
}
func WithFeatureSparseSuperblock(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.sparseSuperblock = enable
	}
}
func WithFeatureLargeFile(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.largeFile = enable
	}
}
func WithFeatureBTreeDirectory(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.btreeDirectory = enable
	}
}
func WithFeatureHugeFile(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.hugeFile = enable
	}
}
func WithFeatureGDTChecksum(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.gdtChecksum = enable
	}
}
func WithFeatureLargeSubdirectoryCount(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.largeSubdirectoryCount = enable
	}
}
func WithFeatureLargeInodes(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.largeInodes = enable
	}
}
func WithFeatureSnapshot(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.snapshot = enable
	}
}
func WithFeatureQuota(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.quota = enable
	}
}
func WithFeatureBigalloc(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.bigalloc = enable
	}
}
func WithFeatureMetadataChecksums(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.metadataChecksums = enable
	}
}
func WithFeatureReplicas(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.replicas = enable
	}
}
func WithFeatureReadOnly(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.readOnly = enable
	}
}
func WithFeatureProjectQuotas(enable bool) FeatureOpt {
	return func(o *featureFlags) {
		o.projectQuotas = enable
	}
}
