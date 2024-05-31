package mariv2

import (
	"errors"
	"sync/atomic"
	"unsafe"
)


//============================================= MariNode Operations


// copyINode
//	Creates a copy of an existing internal node.
//	This is used for path copying, so on operations that modify the trie, a copy is created instead of modifying the existing node.
//	The data structure is essentially immutable. 
//	If an operation succeeds, the copy replaces the existing node, otherwise the copy is discarded.
func (mariInst *Mari) copyINode(node *MariINode) *MariINode {
	nodeCopy := mariInst.nodePool.getINode()
	
	nodeCopy.version = node.version
	nodeCopy.bitmap = node.bitmap
	nodeCopy.leaf = node.leaf
	nodeCopy.children = make([]*MariINode, len(node.children))

	copy(nodeCopy.children, node.children)
	return nodeCopy
}

// determineEndOffsetINode
//	Determine the end offset of a serialized MariINode.
//	This will be the start offset through the children index, plus (number of children * 8 bytes).
func (node *MariINode) determineEndOffsetINode() uint64 {
	nodeEndOffset := node.startOffset

	encodedChildrenLength := func() int {
		var totalChildren int 
		for _, subBitmap := range node.bitmap {
			totalChildren += calculateHammingWeight(subBitmap)
		}
			
		return totalChildren * NodeChildPtrSize
	}()

	if encodedChildrenLength != 0 {
		nodeEndOffset += uint64(NodeChildrenIdx + encodedChildrenLength)
	} else { nodeEndOffset += NodeChildrenIdx }

	return nodeEndOffset - 1
}

// determineEndOffsetLNode
//	Determine the end offset of a serialized MariLNode.
//	This will be the start offset through the key index, plus the length of the key and the length of the value.
func (node *MariLNode) determineEndOffsetLNode() uint64 {
	nodeEndOffset := node.startOffset
	if node.key != nil {
		nodeEndOffset += uint64(NodeKeyIdx + int(node.keyLength) + len(node.value))
	} else { nodeEndOffset += uint64(NodeKeyIdx) }
	
	return nodeEndOffset - 1
}

// getChildNode
//	Get the child node of an internal node.
//	If the version is the same, set child as that node since it exists in the path.
//	Otherwise, read the node from the memory map.
func (mariInst *Mari) getChildNode(childOffset *MariINode, version uint64) (*MariINode, error) {
	var childNode *MariINode
	var desErr error

	if childOffset.version == version && childOffset.startOffset == 0 {
		childNode = childOffset
	} else {
		childNode, desErr = mariInst.readINodeFromMemMap(childOffset.startOffset)
		if desErr != nil { return nil, desErr }
	}

	return childNode, nil
}

// getSerializedNodeSize
//	Get the length of the node based on the length of its serialized representation.
func getSerializedNodeSize(data []byte) uint64 {
	return uint64(len(data))
}

// initRoot
//	Initialize the version 0 root where operations will begin traversing.
func (mariInst *Mari) initRoot() (uint64, error) {
	root := mariInst.nodePool.getINode()
	root.startOffset = uint64(InitRootOffset)

	endOffset, writeNodeErr := mariInst.writeINodeToMemMap(root)
	if writeNodeErr != nil { return 0, writeNodeErr }
	return endOffset, nil
}

// loadNodeFromPointer
//	Load Mari node from an unsafe pointer.
func loadINodeFromPointer(ptr *unsafe.Pointer) *MariINode {
	return (*MariINode)(atomic.LoadPointer(ptr))
}

// newInternalNode
//	Creates a new internal node in the ordered array mapped trie, which is essentially a branch node that contains pointers to child nodes.
func (mariInst *Mari) newInternalNode(version uint64) *MariINode {
	iNode := mariInst.nodePool.getINode()
	iNode.version = version
	return iNode
}

// newLeafNode
//	Creates a new leaf node when path copying Mari, which stores a key value pair.
//	It will also include the version of Mari.
func (mariInst *Mari) newLeafNode(key, value []byte, version uint64) *MariLNode {
	lNode := mariInst.nodePool.getLNode()
	lNode.version = version
	lNode.keyLength = uint16(len(key))
	lNode.key = key
	lNode.value = value

	return lNode
}

// readINodeFromMemMap
//	Reads an internal node in Mari from the serialized memory map.
func (mariInst *Mari) readINodeFromMemMap(startOffset uint64) (node *MariINode, err error) {
	defer func() {
		r := recover()
		if r != nil {
			node = nil
			err = errors.New("error reading node from mem map")
		}
	}()
	
	var readErr error 
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	
	mMap := mariInst.data.Load().(MMap)
	sEndOffset := mMap[endOffsetIdx:endOffsetIdx + OffsetSize]

	endOffset, readErr := deserializeUint64(sEndOffset)
	if readErr != nil { return nil, readErr }

	sNode := mMap[startOffset:endOffset + 1]
	node, readErr = deserializeINode(sNode)
	if readErr != nil { return nil, readErr }

	leaf, readErr := mariInst.readLNodeFromMemMap(node.leaf.startOffset)
	if readErr != nil { return nil, readErr }

	node.leaf = leaf
	return node, nil
}

// readLNodeFromMemMap
//	Reads a leaf node in Mari from the serialized memory map.
func (mariInst *Mari) readLNodeFromMemMap(startOffset uint64) (node *MariLNode, err error) {
	defer func() {
		r := recover()
		if r != nil {
			node = nil
			err = errors.New("error reading node from mem map")
		}
	}()
	
	var readErr error 
	endOffsetIdx := startOffset + NodeEndOffsetIdx
	mMap := mariInst.data.Load().(MMap)
	sEndOffset := mMap[endOffsetIdx:endOffsetIdx + OffsetSize]

	endOffset, readErr := deserializeUint64(sEndOffset)
	if readErr != nil { return nil, readErr }

	sNode := mMap[startOffset:endOffset + 1]
	node, readErr = deserializeLNode(sNode)
	if readErr != nil { return nil, readErr }
	return node, nil
}

// storeNodeAsPointer
//	Store a MariINode as an unsafe pointer.
func storeINodeAsPointer(node *MariINode) *unsafe.Pointer {
	ptr := unsafe.Pointer(node)
	return &ptr
}

// writeINodeToMemMap
//	Serializes and writes an internal node instance to the memory map.
func (mariInst *Mari) writeINodeToMemMap(node *MariINode) (offset uint64, err error) {
	defer func() {
		r := recover()
		if r != nil {
			offset = 0
			err = errors.New("error writing new path to mmap")
		}
	}()

	var writeErr error
	sNode, writeErr := node.serializeINode(false)
	if writeErr != nil { return 0, writeErr	}

	mMap := mariInst.data.Load().(MMap)
	copy(mMap[node.startOffset:node.leaf.startOffset], sNode)

	writeErr = mariInst.flushRegionToDisk(node.startOffset, node.endOffset)
	if writeErr != nil { return 0, writeErr } 
	
	lEndOffset, writeErr := mariInst.writeLNodeToMemMap(node.leaf)
	if writeErr != nil { return 0, writeErr }
	return lEndOffset, nil
}

// writeLNodeToMemMap
//	Serializes and writes a MariNode instance to the memory map.
func (mariInst *Mari) writeLNodeToMemMap(node *MariLNode) (offset uint64, err error) {
	defer func() {
		r := recover()
		if r != nil {
			offset = 0
			err = errors.New("error writing new path to mmap")
		}
	}()

	var writeErr error
	sNode, writeErr := node.serializeLNode()
	if writeErr != nil { return 0, writeErr	}

	endOffset := node.determineEndOffsetLNode()
	mMap := mariInst.data.Load().(MMap)
	copy(mMap[node.startOffset:endOffset + 1], sNode)

	writeErr = mariInst.flushRegionToDisk(node.startOffset, endOffset)
	if writeErr != nil { return 0, writeErr } 
	return endOffset + 1, nil
}

// writeNodesToMemMap
//	Write a list of serialized nodes to the memory map. If the mem map is too small for the incoming nodes, dynamically resize.
func (mariInst *Mari) writeNodesToMemMap(snodes []byte, offset uint64) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil {
			ok = false
			err = errors.New("error writing new path to mmap")
		}
	}()

	lenSNodes := uint64(len(snodes))
	endOffset := offset + lenSNodes

	mMap := mariInst.data.Load().(MMap)
	copy(mMap[offset:endOffset], snodes)
	return true, nil
}