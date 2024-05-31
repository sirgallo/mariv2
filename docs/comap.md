# comap

### concurrent ordered map


## overview

A a non-blocking implementation of an `Array Mapped Trie (AMT)` that utilizes atomic `Compare-and-Swap (CAS)` operations. By nature, the `AMT` is in ordered form, where all elements are sorted from left to right, least to greatest.


## design

The design takes the basic algorithm for `AMT`, and adds in `CAS` to insert/delete new values. A thread will modify an element at the point in time it loads it, and if the compare and swap operation fails, the update is discarded and the operation will start back at the root of the trie and traverse the path through to reattempt to add/delete the element.


## background

This persistent, concurrent `AMT` is inspired by the `Ideal Hash Trees` whitepaper, written by Phil Bagwell in 2000 [1](https://lampwww.epfl.ch/papers/idealhashtrees.pdf). In it, he describes the sorted order `AMT`, which is a modified form of a `Hash Array Mapped Trie (HAMT)`. A `HAMT` is a memory efficient data structure that can be used to implement maps (associative arrays) and sets. `HAMTs`, when implemented with path copying and garbage collection, become persistent as well, which means that any function that utilizes them becomes pure (essentially, the data structure becomes immutable). The sorted order `AMT`, instead of hashing the key before operations, treats the key itself as a hash. This maintains the same time complexity for operations as a more general array mapped trie or character trie, but uses the exact same efficient memory allocation as a `HAMT`. This makes it a good candidate for data storage/data indexing and could be a general replacement for the more common B+/B- trees seen in many database storage engines. Sorted Order `AMTs` supports all the expected sorted order functions such as ranges, ordered iteration, etc.


## algorithms

### put

Pseudo-code:
```
  1.) calculate the index of the key
    I.) if the bit is not set in the bitmap
      1.) create a new leaf node with the key and value
      2.) set the bit in the bitmap to 1
      3.) calculate the position in the dense array where the node will reside based on the current size
      4.) extend the table and add the leaf node
    II.) if the bit is set
      1.) caculate the position of the child node
        a.) if the node is a leaf node, and the key is the same as the incoming, update the value
        b.) if it is a leaf node, and the keys are not the same, create a new internal node (which acts as a branch), and then recursively add the new leaf node and the existing leaf node at this key to the internal node
        c.) if the node is an internal node, recursively move into that branch and repeat 2
```

### get

Pseudo-code:
```
  1.) calculate the index of the key
    I.) if the bit is not set, return null since the value does not exist
    II.) if the bit is set
      a.) if the node at the index in the dense array is a leaf node, and the keys match, return the value
      b.) otherwise, recurse down a level and repeat 2
```

### delete

Pseudo-code:
```
  1.) calculate the index of the key
    I.) if the bit is not set, return false, since we are not deleting anything
    II.) if the bit is set
      a.) calculate the index in the children, and if it is a leaf node and the key is equal to incoming, shrink the table and remove the element, and set the bitmap value at the index to 0
      b.) otherwise, recurse down a level since we are at an internal node
```

## NOTES

### compact paths

One optimization made to the data structure is the use of compact paths. When a key-value pair is inserted into the sorted order `AMT`, instead of creating a node for the each byte in the key, the key is terminated when it finds that there are no more children beyond the level it is at, and on later inserts the key-value pair can be shifted down the branch if necessary. This makes for a more compact, flat data structure, where most key-value pairs will exist on the same level within the trie, similar to a B+Tree. This optimization saves valuable wasted space for empty nodes where there are no key value pairs and is a significant factor in the overall performance of `mari`.


### array mapping

Using the key, we can determine:

1. The index in the sparse index
2. The index in the actual dense array where the node is stored

#### sparse index

Each node contains a sparse index for the mapped nodes in a 256bit bitmap, made up of 8 uint32 bitmaps. 

To calculate the index:
```go
func getIndexForLevel(key []byte, level int) byte {
	return key[level]
}
```

Treat each byte in the key as an index, resulting in a total of 256 combinations at each step.

#### dense index

To limit table size and create dynamically sized tables to limit memory usage (instead of fixed size child node arrays), we can take the calculated sparse index for the key and, for all non-zero bits to the right of it, caclulate the population count ([Hamming Weight](https://en.wikipedia.org/wiki/Hamming_weight))

In go, we can utilize the `math/bits` package to calculate the hamming weight efficiently:
```go
func calculateHammingWeight(bitmap uint32) int {
	return bits.OnesCount32(bitmap)
}
```

calculating hamming weight naively:
```
hammingWeight(uint32 bits): 
  weight = 0
  for bits != 0:
    if bits & 1 == 1:
      weight++
    bits >>= 1
  
  return weight
```

to get population count for all 8 uint32 bitmaps:
```go
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
```

to calculate position:
```go
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
```

`isolatedBits` is all of the non-zero bits right of the index, which can be calculated by is applying a mask to the bitMap at that particular node. The mask is calculated from all of from the start of the sparse index right.


### path copying

`mari` implements full path copying. As an operation traverses down the path to the key, on inserts/deletes it will make a copy of the current node and modify the copy instead of modifying the node in place. This makes Mari [persistent](https://en.wikipedia.org/wiki/Persistent_data_structure). The modified node causes all parent nodes to point to it by cascading the changes up the path back to the root of the trie. This is done by passing a copy of the node being looked at, and then performing compare and swap back up the path. If the compare and swap operation fails, the copy is discarded and the operation retries back at the root.


### table resizing

#### extend

When a position in the new table is calculated for an inserted element, the original table needs to be resized, and a new row at that particular location will be added, maintaining the sorted nature from the sparse index. This is done using go array slices, and copying elements from the original to the new table.

```go
func extendTable(orig []*MariINode, bitmap [8]uint32, pos int, newNode *MariINode) []*MariINode {
	tableSize := populationCount(bitmap)
	newTable := make([]*MariINode, tableSize)

	copy(newTable[:pos], orig[:pos])
	newTable[pos] = newNode
	copy(newTable[pos + 1:], orig[pos:])
	
	return newTable
}
```

#### shrink

Similarly to extending, shrinking a table will remove a row at a particular index and then copy elements from the original table over to the new table.

```go
func shrinkTable(orig []*MariINode, bitmap [8]uint32, pos int) []*MariINode {
	tableSize := populationCount(bitmap)
	newTable := make([]*MariINode, tableSize)

	copy(newTable[:pos], orig[:pos])
	copy(newTable[pos:], orig[pos + 1:])
	
	return newTable
}
```


## refs

[1] [Ideal Hash Trees, Phil Bagwell](https://lampwww.epfl.ch/papers/idealhashtrees.pdf)