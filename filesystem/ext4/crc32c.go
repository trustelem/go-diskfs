package ext4

import (
	"encoding/binary"
	"hash/crc32"
)

const crc32seed uint32 = 0xFFFFFFFF

var crc32Tab = crc32.MakeTable(crc32.Castagnoli)

func crc32c_update(crc uint32, input []byte) uint32 {
	return ^crc32.Update(^crc, crc32Tab, input)
}

func crc32c_update_u32(crc uint32, n uint32) uint32 {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], n)
	return ^crc32.Update(^crc, crc32Tab, data[:])
}
