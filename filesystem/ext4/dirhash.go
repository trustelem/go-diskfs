package ext4

const (
	teaDelta       uint32 = 0x9E3779B9
	k1             uint32 = 0
	k2             uint32 = 013240474631
	k3             uint32 = 015666365641
	ext4HtreeEOF32 uint32 = ((1 << (32 - 1)) - 1)
	ext4HtreeEOF64 uint64 = ((1 << (64 - 1)) - 1)
)

type hashVersion uint8

const (
	HashVersionLegacy          = 0
	HashVersionHalfMD4         = 1
	HashVersionTEA             = 2
	HashVersionLegacyUnsigned  = 3
	HashVersionHalfMD4Unsigned = 4
	HashVersionTEAUnsigned     = 5
	HashVersionSIP             = 6
)

func TEATransform(buf [4]uint32, in []uint32) [4]uint32 {
	var sum uint32
	var b0, b1 = buf[0], buf[1]
	var a, b, c, d = in[0], in[1], in[2], in[3]
	var n int = 16

	for ; n > 0; n-- {
		sum += teaDelta
		b0 += ((b1 << 4) + a) ^ (b1 + sum) ^ ((b1 >> 5) + b)
		b1 += ((b0 << 4) + c) ^ (b0 + sum) ^ ((b0 >> 5) + d)
	}

	buf[0] += b0
	buf[1] += b1
	return buf
}

// rol32 - rotate a 32-bit value left
func rol32(word uint32, shift uint) uint32 {
	return (word << (shift & 31)) | (word >> ((-shift) & 31))
}

// F, G and H are basic MD4 functions: selection, majority, parity */
func f(x, y, z uint32) uint32 {
	return ((z) ^ ((x) & ((y) ^ (z))))
}
func g(x, y, z uint32) uint32 {
	return (((x) & (y)) + (((x) ^ (y)) & (z)))
}
func h(x, y, z uint32) uint32 {
	return ((x) ^ (y) ^ (z))
}

func round(f func(uint32, uint32, uint32) uint32, a, b, c, d, x uint32, s uint) uint32 {
	return rol32(a+f(b, c, d)+x, s)
}

// halfMD4Transform basic cut-down MD4 transform.  Returns only 32 bits of result.
func halfMD4Transform(buf [4]uint32, in []uint32) [4]uint32 {
	var a, b, c, d uint32 = buf[0], buf[1], buf[2], buf[3]

	/* Round 1 */
	a = round(f, a, b, c, d, in[0]+k1, 3)
	d = round(f, d, a, b, c, in[1]+k1, 7)
	c = round(f, c, d, a, b, in[2]+k1, 11)
	b = round(f, b, c, d, a, in[3]+k1, 19)
	a = round(f, a, b, c, d, in[4]+k1, 3)
	d = round(f, d, a, b, c, in[5]+k1, 7)
	c = round(f, c, d, a, b, in[6]+k1, 11)
	b = round(f, b, c, d, a, in[7]+k1, 19)

	/* Round 2 */
	a = round(g, a, b, c, d, in[1]+k2, 3)
	d = round(g, d, a, b, c, in[3]+k2, 5)
	c = round(g, c, d, a, b, in[5]+k2, 9)
	b = round(g, b, c, d, a, in[7]+k2, 13)
	a = round(g, a, b, c, d, in[0]+k2, 3)
	d = round(g, d, a, b, c, in[2]+k2, 5)
	c = round(g, c, d, a, b, in[4]+k2, 9)
	b = round(g, b, c, d, a, in[6]+k2, 13)

	/* Round 3 */
	a = round(h, a, b, c, d, in[3]+k3, 3)
	d = round(h, d, a, b, c, in[7]+k3, 9)
	c = round(h, c, d, a, b, in[2]+k3, 11)
	b = round(h, b, c, d, a, in[6]+k3, 15)
	a = round(h, a, b, c, d, in[1]+k3, 3)
	d = round(h, d, a, b, c, in[5]+k3, 9)
	c = round(h, c, d, a, b, in[0]+k3, 11)
	b = round(h, b, c, d, a, in[4]+k3, 15)

	buf[0] += a
	buf[1] += b
	buf[2] += c
	buf[3] += d

	return buf
}

// the old legacy hash
func dxHackHash(name string, signed bool) uint32 {
	var hash uint32
	var hash0, hash1 uint32 = 0x12a3fe2d, 0x37abe8f9
	b := []byte(name)

	for i := len(b); i > 0; i-- {
		// get the specific character
		var c int
		if signed {
			c = int(int8(b[i-1]))
		} else {
			c = int(uint8(b[i-1]))
		}
		// the value of the individual character depends on if it is signed or not
		hash = hash1 + (hash0 ^ uint32(c*7152373))

		if hash&0x80000000 != 0 {
			hash -= 0x7fffffff
		}
		hash1 = hash0
		hash0 = hash
	}
	return hash0 << 1
}

func str2hashbuf(msg string, num int, signed bool) []uint32 {
	var buf [8]uint32
	var pad, val uint32
	b := []byte(msg)
	size := len(b)

	pad = uint32(size) | (uint32(size) << 8)
	pad |= pad << 16

	val = pad
	if size > num*4 {
		size = num * 4
	}
	var j int
	for i := 0; i < size; i++ {
		var c int
		if signed {
			c = int(int8(b[i]))
		} else {
			c = int(uint8(b[i]))
		}
		val = uint32(c) + (val << 8)
		if (i % 4) == 3 {
			buf[j] = val
			val = pad
			num--
			j++
		}
	}
	num--
	if num >= 0 {
		buf[j] = val
		j++
	}
	for num--; num >= 0; num-- {
		buf[j] = pad
		j++
	}
	return buf[:]
}

func ext4fsDirhash(name string, version hashVersion, seed []uint32) (uint32, uint32) {
	var hash, minorHash uint32
	/* Initialize the default seed for the hash checksum functions */
	var buf = [4]uint32{0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476}

	// Check to see if the seed is all zero, and if so, use the default
	for i, val := range seed {
		if val != 0 {
			buf[i] = val
		}
	}

	switch version {
	case HashVersionLegacyUnsigned:
		hash = dxHackHash(name, false)
	case HashVersionLegacy:
		hash = dxHackHash(name, true)
	case HashVersionHalfMD4Unsigned:
		for i := 0; i < len(name); i += 32 {
			in := str2hashbuf(name[i:], 8, false)
			buf = halfMD4Transform(buf, in)
		}
		minorHash = buf[2]
		hash = buf[1]
	case HashVersionHalfMD4:
		for i := 0; i < len(name); i += 32 {
			in := str2hashbuf(name[i:], 8, true)
			buf = halfMD4Transform(buf, in)
		}
		minorHash = buf[2]
		hash = buf[1]
	case HashVersionTEAUnsigned:
		for i := 0; i < len(name); i += 16 {
			in := str2hashbuf(name[i:], 4, false)
			buf = TEATransform(buf, in)
		}
		hash = buf[0]
		minorHash = buf[1]
	case HashVersionTEA:
		for i := 0; i < len(name); i += 16 {
			in := str2hashbuf(name[i:], 4, true)
			buf = TEATransform(buf, in)
		}
		hash = buf[0]
		minorHash = buf[1]
	default:
		return 0, 0
	}
	hash = hash & ^uint32(1)
	if hash == (ext4HtreeEOF32 << 1) {
		hash = (ext4HtreeEOF32 - 1) << 1
	}
	return hash, minorHash
}
