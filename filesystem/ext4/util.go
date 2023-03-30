package ext4

import (
	"fmt"
)

const (
	// KB represents one KB
	KB int64 = 1024
	// MB represents one MB
	MB int64 = 1024 * KB
	// GB represents one GB
	GB int64 = 1024 * MB
	// TB represents one TB
	TB int64 = 1024 * GB
	// PB represents one TB
	PB int64 = 1024 * TB
	// XB represents one Exabyte
	XB int64 = 1024 * PB
	// these because they are larger than int64 or uint64 can handle
	// ZB represents one Zettabyte
	//ZB int64 = 1024 * XB
	// YB represents one Yottabyte
	//YB int64 = 1024 * ZB
	// Ext4MaxSize is maximum size of an ext4 filesystem in bytes
	//   it varies based on the block size and if we are 64-bit or 32-bit mode, but the absolute complete max
	//   is 64KB per block (128 sectors) in 64-bit mode
	//   for a max filesystem size of 1YB (yottabyte)
	//Ext4MaxSize int64 = YB
	// if we ever actually care, we will use math/big to do it
	//var xb, ZB, kb, YB big.Int
	//kb.SetUint64(1024)
	//xb.SetUint64(uint64(XB))
	//ZB.Mul(&xb, &kb)
	//YB.Mul(&ZB, &kb)

	// Ext4MinSize is minimum size for an ext4 filesystem
	//   it assumes a single block group with:
	//   blocksize = 2 sectors = 1KB
	//   1 block for boot code
	//   1 block for superblock
	//   1 block for block group descriptors
	//   1 block for bock and inode bitmaps and inode table
	//   1 block for data
	//   total = 5 blocks
	Ext4MinSize int64 = 5 * int64(SectorSize512)
)

// convert a string to a byte array, if all characters are valid ascii
func stringToASCIIBytes(s string) ([]byte, error) {
	length := len(s)
	b := make([]byte, length, length)
	// convert the name into 11 bytes
	r := []rune(s)
	// take the first 8 characters
	for i := 0; i < length; i++ {
		val := int(r[i])
		// we only can handle values less than max byte = 255
		if val > 255 {
			return nil, fmt.Errorf("Non-ASCII character in name: %s", s)
		}
		b[i] = byte(val)
	}
	return b, nil
}
