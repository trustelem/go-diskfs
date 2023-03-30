package ext4

// miscFlags is a structure holding various miscellaneous flags
type miscFlags struct {
	signedDirectoryHash   bool
	unsignedDirectoryHash bool
	developmentTest       bool
}

func parseMiscFlags(flags uint32) miscFlags {
	m := miscFlags{
		signedDirectoryHash:   flags&flagSignedDirectoryHash == flagSignedDirectoryHash,
		unsignedDirectoryHash: flags&flagUnsignedDirectoryHash == flagUnsignedDirectoryHash,
		developmentTest:       flags&flagTestDevCode == flagTestDevCode,
	}
	return m
}

func (m *miscFlags) toInt() uint32 {
	var flags uint32

	if m.signedDirectoryHash {
		flags = flags | flagSignedDirectoryHash
	}
	if m.unsignedDirectoryHash {
		flags = flags | flagUnsignedDirectoryHash
	}
	if m.developmentTest {
		flags = flags | flagTestDevCode
	}
	return flags
}

var defaultMiscFlags = miscFlags{}
