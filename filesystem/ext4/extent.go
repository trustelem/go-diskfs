package ext4

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
)

// extent a structure with information about a contiguous run of blocks
type extent struct {
	fileBlock     uint32
	startingBlock uint64
	count         uint16
}

type extentTreeHeader struct {
	ehMagic      uint16 // Magic number, 0xF30A.
	ehEntries    uint16 // Number of valid entries following the header
	ehMax        uint16 // Maximum number of entries that could follow the header.
	ehDepth      uint16 // Depth of this extent node in the extent tree
	ehGeneration uint32 // only used by lustre
}

func parseExtentTreeHeader(b []byte) (eh extentTreeHeader, err error) {
	eh.ehMagic = binary.LittleEndian.Uint16(b[0:2])
	if eh.ehMagic != extentHeaderSignature {
		return eh, fmt.Errorf("invalid extent tree header signature: %x", eh.ehMagic)
	}
	eh.ehEntries = binary.LittleEndian.Uint16(b[0x2:0x4])
	eh.ehMax = binary.LittleEndian.Uint16(b[0x4:0x6])
	eh.ehDepth = binary.LittleEndian.Uint16(b[0x6:0x8])
	return
}

type extentTreeInternalNode struct {
	eiBlock uint32 /* index covers file blocks from 'block' onward */
	eiLeaf  uint64
}

func parseExtentTreeInternalNodes(b []byte, count int) (eis []extentTreeInternalNode, err error) {
	if len(b) < count*extentTreeEntryLength {
		return nil, fmt.Errorf("invalid size %d to parse extent tree internal nodes, expected at least %d", len(b), count*extentTreeEntryLength)
	}
	eis = make([]extentTreeInternalNode, count)
	for i := 0; i < count; i++ {
		start := i * extentTreeEntryLength
		eis[i].eiBlock = binary.LittleEndian.Uint32(b[start : start+4])
		eiLeafLo := binary.LittleEndian.Uint32(b[start+4 : start+8])  // Lower 32-bits of the block number of the extent node that is the next level lower in the tree
		eiLeafHi := binary.LittleEndian.Uint16(b[start+8 : start+10]) // high 16 bits of previous field
		eis[i].eiLeaf = uint64(eiLeafLo) + uint64(eiLeafHi)<<32
	}
	return
}

// parseExtentTree takes bytes, parses them to find the actual extents or the next blocks down
func parseExtentTree(b []byte, fileBlock uint32, dataBlock uint64) (*extentTree, error) {
	// must have at least header and one entry
	minLength := extentTreeHeaderLength + extentTreeEntryLength
	if len(b) < minLength {
		return nil, fmt.Errorf("cannot parse extent tree from %d bytes, minimum required %d", len(b), minLength)
	}
	// check magic signature
	eh, err := parseExtentTreeHeader(b)
	if err != nil {
		return nil, err
	}

	e := extentTree{
		entries:     eh.ehEntries,
		max:         eh.ehMax,
		depth:       eh.ehDepth,
		fileBlock:   fileBlock,
		blockNumber: dataBlock,
	}

	// we have parsed the header, now read either the leaf entries or the intermediate nodes
	switch e.depth {
	case 0:
		// read the leaves
		e.extents = make([]extent, 0, 4)
		for i := uint16(0); i < e.entries; i++ {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			var diskBlock [8]byte
			copy(diskBlock[0:4], b[start+8:start+12])
			copy(diskBlock[4:6], b[start+6:start+8])
			e.extents = append(e.extents, extent{
				fileBlock:     binary.LittleEndian.Uint32(b[start : start+4]),
				count:         binary.LittleEndian.Uint16(b[start+4 : start+6]),
				startingBlock: binary.LittleEndian.Uint64(diskBlock[:]),
			})
		}
	default:
		e.children, err = parseExtentTreeInternalNodes(b[extentTreeHeaderLength:], int(e.entries))
		if err != nil {
			return nil, err
		}
	}

	return &e, nil
}

func (e *extentTree) print(w io.Writer, ctx string) {
	fmt.Fprintf(w, "[%s] extent tree, depth=%d max=%d\n", ctx, e.depth, e.max)
	if e.depth == 0 {
		for _, e := range e.extents {
			fmt.Fprintf(w, "[%s] extent leaf node, fileBlock=%d startingBlock=%d count=%d\n", ctx, e.fileBlock, e.startingBlock, e.count)
		}
	} else {
		for _, e := range e.children {
			fmt.Fprintf(w, "[%s] extent internal node, eiBlock=%d eiLeaf=%d\n", ctx, e.eiBlock, e.eiLeaf)
		}
	}

}

func flattenExtentTree(fs *FileSystem, t *extentTree) ([]extent, error) {
	if t.depth == 0 {
		return t.extents, nil
	}
	var extents []extent
	for _, c := range t.children {
		byteStart := uint64(fs.start) + uint64(c.eiLeaf)*fs.superblock.blockSize
		buffer := make([]byte, fs.superblock.blockSize)
		read, err := fs.file.ReadAt(buffer, int64(byteStart))
		if err != nil {
			return nil, fmt.Errorf("extent child %d %d read error %v\n", c.eiBlock, c.eiLeaf, err)
		}
		et, err := parseExtentTree(buffer[:read], 0, 0)
		if err != nil {
			return nil, fmt.Errorf("extent child %d %d parse error %v\n", c.eiBlock, c.eiLeaf, err)
		}
		if et.depth != t.depth-1 {
			return nil, fmt.Errorf("wrong extent child data %v\n%s\n", et, hex.Dump(buffer[:read]))
		}
		childExtents, err := flattenExtentTree(fs, et)
		if err != nil {
			return nil, err
		}
		extents = append(extents, childExtents...)

	}
	return extents, nil
}
