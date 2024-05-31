package mariv2

import (
	"bytes"
	"unsafe"
)


//============================================= Mari Operations


// putRecursive
//	Attempts to traverse through the trie, locating the node at a given level to modify for the key-value pair.
//	It first hashes the key, determines the sparse index in the bitmap to modify, and creates a copy of the current node to be modified.
//	If the bit in the bitmap of the node is not set, a new leaf node is created, the bitmap of the copy is modified to reflect the position of the new leaf node, and the child node array is extended to include the new leaf node.
//	Then, an atomic compare and swap operation is performed where the operation attempts to replace the current node with the modified copy.
//	If the operation succeeds the response is returned by moving back up the tree. If it fails, the copy is discarded and the operation returns to the root to be reattempted.
//	If the current bit is set in the bitmap, the operation checks if the node at the location in the child node array is a leaf node or an internal node.
//	If it is a leaf node and the key is the same as the incoming key, the copy is modified with the new value and we attempt to compare and swap the current child leaf node with the new copy.
//	If the leaf node does not contain the same key, the operation creates a new internal node, and inserts the new leaf node for the incoming key and value as well as the existing child node into the new internal node.
//	Attempts to compare and swap the current leaf node with the new internal node containing the existing child node and the new leaf node for the incoming key and value.
//	If the node is an internal node, the operation traverses down the tree to the internal node and the above steps are repeated until the key-value pair is inserted.
func (mariInst *Mari) putRecursive(node *unsafe.Pointer, key, value []byte, level int) (bool, error) {
	var putErr error

	currNode := loadINodeFromPointer(node)
	nodeCopy := mariInst.copyINode(currNode)
	nodeCopy.leaf.version = nodeCopy.version

	putNewINode := func(node *MariINode, currIdx byte, uKey, uVal []byte) (*MariINode, error) {
		node.bitmap = setBit(node.bitmap, currIdx)
		pos := getPosition(node.bitmap, currIdx, level)

		newINode := mariInst.newInternalNode(node.version)
		iNodePtr := storeINodeAsPointer(newINode)
		_, putINodeErr := mariInst.putRecursive(iNodePtr, uKey, uVal, level + 1)
		if putINodeErr != nil { return nil, putINodeErr }

		updatedINode:= loadINodeFromPointer(iNodePtr)
		node.children = extendTable(node.children, node.bitmap, pos, updatedINode)

		return node, nil
	}

	if len(key) == level {
		switch {
			case bytes.Equal(nodeCopy.leaf.key, key):
				if ! bytes.Equal(nodeCopy.leaf.value, value) { nodeCopy.leaf = mariInst.newLeafNode(key, value, nodeCopy.version) }
			default:
				currentLeaf := nodeCopy.leaf
				nodeCopy.leaf = mariInst.newLeafNode(key, value, nodeCopy.version)

				if len(currentLeaf.key) > len(key) {
					idx := getIndexForLevel(currentLeaf.key, level)

					if ! isBitSet(nodeCopy.bitmap, idx) { 
						nodeCopy, putErr = putNewINode(nodeCopy, idx, currentLeaf.key, currentLeaf.value)
						if putErr != nil { return false, putErr }
					}
				}
		}
	} else {
		index := getIndexForLevel(key, level)
		
		switch {
			case ! isBitSet(nodeCopy.bitmap, index):
				if level > 0 {
					popCount := populationCount(nodeCopy.bitmap)
					currentLeaf := nodeCopy.leaf

					switch {
						case bytes.Equal(currentLeaf.key, key):
							if ! bytes.Equal(currentLeaf.value, value) { nodeCopy.leaf = mariInst.newLeafNode(key, value, nodeCopy.version) }
						case len(currentLeaf.key) == 0 && popCount == 0:
							nodeCopy.leaf = mariInst.newLeafNode(key, value, nodeCopy.version)
						case len(currentLeaf.key) == 0 && popCount > 0:
							nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
							if putErr != nil { return false, putErr }
						default:
							switch {
								case len(key) > len(currentLeaf.key) && len(currentLeaf.key) > 0:
									nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
									if putErr != nil { return false, putErr }
								case len(currentLeaf.key) > len(key):
									nodeCopy.leaf = mariInst.newLeafNode(key, value, nodeCopy.version)
									newIdx := getIndexForLevel(currentLeaf.key, level)
									
									if ! isBitSet(nodeCopy.bitmap, newIdx) {
										nodeCopy, putErr = putNewINode(nodeCopy, newIdx, currentLeaf.key, currentLeaf.value)
										if putErr != nil { return false, putErr }
									}
								default:
									nodeCopy.leaf = mariInst.newLeafNode(nil, nil, nodeCopy.version)

									nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
									if putErr != nil { return false, putErr }
		
									newIdx := getIndexForLevel(currentLeaf.key, level)

									if ! isBitSet(nodeCopy.bitmap, newIdx) {
										nodeCopy, putErr = putNewINode(nodeCopy, newIdx, currentLeaf.key, currentLeaf.value)
										if putErr != nil { return false, putErr }
									} else {
										newPos := getPosition(nodeCopy.bitmap, newIdx, level)
										
										childOffset := nodeCopy.children[newPos]
										childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.version)
										if getChildErr != nil { return false, getChildErr }
							
										childNode.version = nodeCopy.version
										childPtr := storeINodeAsPointer(childNode)
										_, putErr = mariInst.putRecursive(childPtr, currentLeaf.key, currentLeaf.value, level + 1)
										if putErr != nil { return false, putErr }

										updatedCNode := loadINodeFromPointer(childPtr)
										nodeCopy.children[newPos] = updatedCNode
									}
							}
					}
				} else {
					nodeCopy, putErr = putNewINode(nodeCopy, index, key, value)
					if putErr != nil { return false, putErr }
				}
			default:
				pos := getPosition(nodeCopy.bitmap, index, level)

				childOffset := nodeCopy.children[pos]
				childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.version)
				if getChildErr != nil { return false, getChildErr }
	
				childNode.version = nodeCopy.version
				childPtr := storeINodeAsPointer(childNode)
	
				_, putErr = mariInst.putRecursive(childPtr, key, value, level + 1)
				if putErr != nil { return false, putErr }
	
				nodeCopy.children[pos] = loadINodeFromPointer(childPtr)
		}
	}

	return mariInst.compareAndSwap(node, currNode, nodeCopy), nil
}

// getRecursive
//	Attempts to recursively retrieve a value for a given key within the ordered array mapped trie.
//	For each node traversed to at each level the operation travels to, the sparse index is calculated for the hashed key.
//	If the bit is not set in the bitmap, return nil since the key has not been inserted yet into the trie.
//	Otherwise, determine the position in the child node array for the sparse index.
//	If the child node is a leaf node and the key to be searched for is the same as the key of the child node, the value has been found.
//	Since the trie utilizes path copying, any threads modifying the trie are modifying copies so it the get operation returns the value at the point in time of the get operation.
//	If the node is node a leaf node, but instead an internal node, recurse down the path to the next level to the child node in the position of the child node array and repeat the above.
func (mariInst *Mari) getRecursive(node *unsafe.Pointer, key []byte, level int, transform MariOpTransform) (*KeyValuePair, error) {
	currNode := loadINodeFromPointer(node)
	
	getKeyVal := func() *KeyValuePair {
		return &KeyValuePair{
			Version: currNode.leaf.version,
			Key: currNode.leaf.key,
			Value: currNode.leaf.value,
		}
	}

	if len(key) == level {
		if bytes.Equal(key, currNode.leaf.key) { return transform(getKeyVal()), nil }
		return nil, nil
	} else {
		if bytes.Equal(key, currNode.leaf.key) { return transform(getKeyVal()), nil }
		
		index := getIndexForLevel(key, level)
		
		switch {
			case ! isBitSet(currNode.bitmap, index):
				return nil, nil
			default:
				pos := getPosition(currNode.bitmap, index, level)
				childOffset := currNode.children[pos]

				childNode, getChildErr := mariInst.getChildNode(childOffset, currNode.version)
				if getChildErr != nil { return nil, getChildErr }

				childPtr := storeINodeAsPointer(childNode)
				return mariInst.getRecursive(childPtr, key, level + 1, transform)
		}
	}
}

// deleteRecursive
//	Attempts to recursively move down the path of the trie to the key-value pair to be deleted.
//	The byte index for the key is calculated, the sparse index in the bitmap is determined for the given level, and a copy of the current node is created to be modifed.
//	If the bit in the bitmap is not set, the key doesn't exist so truthy is returned since there is nothing to delete and the operation completes.
//	If the bit is set, the child node for the position within the child node array is found.
//	If the child node is a leaf node and the key of the child node is equal to the key of the key to delete, the copy is modified to update the bitmap and shrink the table and remove the given node.
//	A compare and swap operation is performed, and if successful traverse back up the trie and complete, otherwise the operation is returned to the root to retry.
//	If the child node is an internal node, the operation recurses down the trie to the next level.
//	On return, if the internal node is empty, the copy modified so the bitmap is updated and table is shrunk.
//	A compare and swap operation is performed on the current node with the new copy.
func (mariInst *Mari) deleteRecursive(node *unsafe.Pointer, key []byte, level int) (bool, error) {
	currNode := loadINodeFromPointer(node)
	nodeCopy := mariInst.copyINode(currNode)

	deleteKeyVal := func() bool {
		nodeCopy.leaf = mariInst.newLeafNode(nil, nil, nodeCopy.version)
		return mariInst.compareAndSwap(node, currNode, nodeCopy)
	}

	if len(key) == level {
		switch {
			case bytes.Equal(nodeCopy.leaf.key, key):
				return deleteKeyVal(), nil
			default:
				return true, nil
		}
	} else {
		index := getIndexForLevel(key, level)

		switch {
			case bytes.Equal(nodeCopy.leaf.key, key):
				return deleteKeyVal(), nil
			case ! isBitSet(nodeCopy.bitmap, index):
				return true, nil
			default:
				pos := getPosition(nodeCopy.bitmap, index, level)
				childOffset := nodeCopy.children[pos]
		
				childNode, getChildErr := mariInst.getChildNode(childOffset, nodeCopy.version)
				if getChildErr != nil { return false, getChildErr }
		
				childNode.version = nodeCopy.version
				childPtr := storeINodeAsPointer(childNode)

				_, delErr := mariInst.deleteRecursive(childPtr, key, level + 1)
				if delErr != nil { return false, delErr }

				updatedChildNode := loadINodeFromPointer(childPtr)
				nodeCopy.children[pos] = updatedChildNode

				if updatedChildNode.leaf.version == nodeCopy.version {
					childNodePopCount := populationCount(updatedChildNode.bitmap)
					
					if childNodePopCount == 0 {
						nodeCopy.bitmap = setBit(nodeCopy.bitmap, index)
						nodeCopy.children = shrinkTable(nodeCopy.children, nodeCopy.bitmap, pos)
					}
				}

				return mariInst.compareAndSwap(node, currNode, nodeCopy), nil
		}
	}
}