package mariv2

import (
	"fmt"
	"math/bits"
)


//============================================= Mari Utilities


// Print children
//	Debugging function for printing nodes in the ordered array mapped trie.
func (mariInst *Mari) PrintChildren() error {
	_, rootOffset, readRootOffErr := mariInst.loadMetaRootOffset()
	if readRootOffErr != nil { return readRootOffErr }

	currRoot, readRootErr := mariInst.readINodeFromMemMap(rootOffset)
	if readRootErr != nil { return readRootErr }
	
	totalCount, readChildrenErr := mariInst.printChildrenRecursive(currRoot, 0, 0)
	if readChildrenErr != nil { return readChildrenErr }

	fmt.Println("total count of elements:", totalCount)
	return nil
}

// calculateHammingWeight
//	Determines the total number of 1s in the binary representation of a number. 0s are ignored.
func calculateHammingWeight(bitmap uint32) int {
	return bits.OnesCount32(bitmap)
}

// extendTable
//	Utility function for dynamically expanding the child node array if a bit is set and a value needs to be inserted into the array.
func extendTable(orig []*INode, bitmap [8]uint32, pos int, newNode *INode) []*INode {
	tableSize := populationCount(bitmap)
	newTable := make([]*INode, tableSize)

	copy(newTable[:pos], orig[:pos])
	newTable[pos] = newNode
	copy(newTable[pos + 1:], orig[pos:])
	
	return newTable
}

// getIndexForLevel
//	Determines the local level for a key.
func getIndexForLevel(key []byte, level int) byte {
	return key[level]
}

// getPosition
//	Calculates the position in the child node array based on the sparse index and the current bitmap of internal node.
//	The sparse index is calculated using the hash and bitchunk size.
//	A mask is calculated by performing a bitwise left shift operation, which shifts the binary representation of the value 1 the number of positions associated with the sparse index value and then subtracts 1.
//	This creates a binary number with all 1s to the right sparse index positions.
//	The mask is then applied the bitmap and the resulting isolated bits are the 1s right of the sparse index. 
//	The hamming weight, or total bits right of the sparse index, is then calculated.
func getPosition(bitMap [8]uint32, index byte, level int) int {
	subBitmapIndex := index >> 5
	indexInSubBitmap := index & 0x1F
	precedingSubBitmapsCount := 0
	
	if subBitmapIndex - 1 > 0 {
		switch subBitmapIndex - 1 {
			case 6:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[6])
				fallthrough
			case 5:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[5])
				fallthrough
			case 4:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[4])
				fallthrough
			case 3:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[3])
				fallthrough
			case 2:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[2])
				fallthrough
			case 1:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[1])
				fallthrough
			case 0:
				precedingSubBitmapsCount += calculateHammingWeight(bitMap[0])
		}
	}

	subBitmap := bitMap[subBitmapIndex]
	mask := uint32((1 << uint32(indexInSubBitmap)) - 1)
	isolatedBits := subBitmap & mask
	
	return precedingSubBitmapsCount + calculateHammingWeight(isolatedBits)
}

// isBitSet
//	Determines whether or not a bit is set in a bitmap by taking the bitmap and applying a mask with a 1 at the position in the bitmap to check.
//	A logical and operation is applied and if the value is not equal to 0, then the bit is set.
func isBitSet(bitmap [8]uint32, index byte) bool {
	subBitmapIndex := index >> 5
	indexInSubBitmap := index & 0x1F 

	subBitmap := bitmap[subBitmapIndex]
	return (subBitmap & (1 << indexInSubBitmap)) != 0
}

// populationCount
//	Determine the total population for the combination of all 8 32 bit bitmaps making up the 256 bit bitmap.
func populationCount(bitmap [8]uint32) int {
	popCount := 0

	popCount += calculateHammingWeight(bitmap[7])
	popCount += calculateHammingWeight(bitmap[6])
	popCount += calculateHammingWeight(bitmap[5])
	popCount += calculateHammingWeight(bitmap[4])
	popCount += calculateHammingWeight(bitmap[3])
	popCount += calculateHammingWeight(bitmap[2])
	popCount += calculateHammingWeight(bitmap[1])
	popCount += calculateHammingWeight(bitmap[0])

	return popCount
}

// setBit
//	Performs a logical xor operation on the current bitmap and the a 32 bit value where the value is all 0s except for at the position of the incoming index.
//	Essentially flips the bit if incoming is 1 and bitmap is 0 at that position, or 0 to 1. 
//	If 0 and 0 or 1 and 1, bitmap is not changed.
func setBit(bitmap [8]uint32, index byte) [8]uint32 {
	subBitmapIndex := index >> 5
	indexInSubBitmap := index & 0x1F 

	bitmap[subBitmapIndex] = bitmap[subBitmapIndex] ^ (1 << indexInSubBitmap)
	return bitmap
}

// shrinkTable
//	Inverse of the extendTable utility function.
//	It dynamically shrinks a table by removing an element at a given position.
func shrinkTable(orig []*INode, bitmap [8]uint32, pos int) []*INode {
	tableSize := populationCount(bitmap)
	newTable := make([]*INode, tableSize)

	copy(newTable[:pos], orig[:pos])
	copy(newTable[pos:], orig[pos + 1:])
	
	return newTable
}

// printChildrenRecursive
//	Recursively print nodes in the mariInst as we traverse down levels.
func (mariInst *Mari) printChildrenRecursive(node *INode, totalCount, level int) (int, error) {
	if node == nil { return 0, nil }

	for idx := range node.children {
		childPtr := node.children[idx]
		child, desErr := mariInst.readINodeFromMemMap(childPtr.startOffset)
		if desErr != nil { return 0, desErr }

		if child != nil {
			if len(child.leaf.key) > 0 { totalCount += 1 }

			fmt.Printf("Level: %d, Index: %d, Key: %s, Value: %s, Version:%d\n", level + 1, idx, child.leaf.key, child.leaf.value, child.leaf.version)
			newtotalCount, printErr := mariInst.printChildrenRecursive(child, totalCount, level + 1)
			if printErr != nil { return 0, printErr }

			totalCount = newtotalCount
		}
	}

	return totalCount, nil
}