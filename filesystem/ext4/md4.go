package ext4

import (
	"golang.org/x/crypto/md4"
)

// HalfMD4 given an input set of bytes, create the "half-md4 hash" used
// by ext4 to calculate keys in the hash tree directory format.
// It is a normal md4 hash, which is then "converted down"
func HalfMD4(in []byte) ([]byte, error) {
	h := md4.New()
	n, err := h.Write(in)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

const (
	K1 uint32 = 0
	K2 uint64 = 013240474631
	K3 uint64 = 015666365641
)

/* F, G and H are basic MD4 functions: selection, majority, parity */
// #define F(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
// #define G(x, y, z) (((x) & (y)) + (((x) ^ (y)) & (z)))
// #define H(x, y, z) ((x) ^ (y) ^ (z))

/*
 * The generic round function.  The application is so specific that
 * we don't bother protecting all the arguments with parens, as is generally
 * good macro practice, in favor of extra legibility.
 * Rotation is separate from addition to prevent recomputation
 */
//func ROUND(f, a, b, c, d, x, s)	{
//	(a += f(b, c, d) + x, a = rol32(a, s))
//}

/**
 * rol32 - rotate a 32-bit value left
 * @word: value to rotate
 * @shift: bits to roll
 */
//static inline __u32 rol32(__u32 word, unsigned int shift)
//{
//	return (word << shift) | (word >> ((-shift) & 31));
//}

/*
 * Basic cut-down MD4 transform.  Returns only 32 bits of result.
 */
// halfMD4Transform takes a full MD4 output and returns only 32 bites of result
// func halfMD4Transform(buf [4]uint32, in [8]uint32) uint32 {
// var a, b, c, d = buf[0], buf[1], buf[2], buf[3]
//
// /* Round 1 */
// ROUND(F, a, b, c, d, in[0]+K1, 3)
// ROUND(F, d, a, b, c, in[1]+K1, 7)
// ROUND(F, c, d, a, b, in[2]+K1, 11)
// ROUND(F, b, c, d, a, in[3]+K1, 19)
// ROUND(F, a, b, c, d, in[4]+K1, 3)
// ROUND(F, d, a, b, c, in[5]+K1, 7)
// ROUND(F, c, d, a, b, in[6]+K1, 11)
// ROUND(F, b, c, d, a, in[7]+K1, 19)
//
// /* Round 2 */
// ROUND(G, a, b, c, d, in[1]+K2, 3)
// ROUND(G, d, a, b, c, in[3]+K2, 5)
// ROUND(G, c, d, a, b, in[5]+K2, 9)
// ROUND(G, b, c, d, a, in[7]+K2, 13)
// ROUND(G, a, b, c, d, in[0]+K2, 3)
// ROUND(G, d, a, b, c, in[2]+K2, 5)
// ROUND(G, c, d, a, b, in[4]+K2, 9)
// ROUND(G, b, c, d, a, in[6]+K2, 13)
//
// /* Round 3 */
// ROUND(H, a, b, c, d, in[3]+K3, 3)
// ROUND(H, d, a, b, c, in[7]+K3, 9)
// ROUND(H, c, d, a, b, in[2]+K3, 11)
// ROUND(H, b, c, d, a, in[6]+K3, 15)
// ROUND(H, a, b, c, d, in[1]+K3, 3)
// ROUND(H, d, a, b, c, in[5]+K3, 9)
// ROUND(H, c, d, a, b, in[0]+K3, 11)
// ROUND(H, b, c, d, a, in[4]+K3, 15)
//
// buf[0] += a
// buf[1] += b
// buf[2] += c
// buf[3] += d
//
// return buf[1] /* "most hashed" word */
// }
//
