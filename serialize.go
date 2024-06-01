package mariv2

import (
	"encoding/binary"
	"errors"
)


//============================================= Mari Serialization


// serializeMetaData
//	Serialize the metadata at the first 0-23 bytes of the memory map. version is 8 bytes and Root Offset is 8 bytes.
func (meta *MetaData) serializeMetaData() []byte {
	versionBytes := make([]byte, OffsetSize64)
	binary.LittleEndian.PutUint64(versionBytes, meta.version)

	rootOffsetBytes := make([]byte, OffsetSize64)
	binary.LittleEndian.PutUint64(rootOffsetBytes, meta.rootOffset)

	nextStartOffsetBytes := make([]byte, OffsetSize64)
	binary.LittleEndian.PutUint64(nextStartOffsetBytes, meta.nextStartOffset)

	offsets := append(rootOffsetBytes, nextStartOffsetBytes...)
	return append(versionBytes, offsets...)
}

// deserializeINode
//	Deserialize the byte representation of an internal in the memory mapped file.
func deserializeINode(snode []byte) (*INode, error) {
	var deserializeErr error

	version, deserializeErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if deserializeErr != nil { return nil, deserializeErr }

	startOffset, deserializeErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if deserializeErr != nil { return nil, deserializeErr	}

	endOffset, deserializeErr := deserializeUint16(snode[NodeEndOffsetIdx:NodeBitmapIdx])
	if deserializeErr != nil { return nil, deserializeErr }

	var bitmaps [8]uint32
	for i := range make([]int, 8) {
		bitmap, deserializeErr := deserializeUint32(snode[NodeBitmapIdx + (4 * i):NodeBitmapIdx + (4 * i) + 4])
		if deserializeErr != nil { return nil, deserializeErr }
		bitmaps[i] = bitmap
	}

	leafOffset, deserializeErr := deserializeUint64(snode[NodeLeafOffsetIdx:NodeChildrenIdx])
	if deserializeErr != nil { return nil, deserializeErr }

	var totalChildren int 
	for _, subBitmap := range bitmaps {
		totalChildren += calculateHammingWeight(subBitmap)
	}

	var children []*INode
	currOffset := NodeChildrenIdx
	for range make([]int, totalChildren) {
		offset, deserializeErr := deserializeUint64(snode[currOffset:currOffset + OffsetSize64])
		if deserializeErr != nil { return nil, deserializeErr }

		nodePtr := &INode{ startOffset: offset }
		children = append(children, nodePtr)
		currOffset += NodeChildPtrSize
	}

	return &INode{
		version: version,
		startOffset: startOffset,
		endOffset: endOffset,
		bitmap: bitmaps,
		leaf: &LNode{ startOffset: leafOffset },
		children: children,
	}, nil
}

// deserializeLNode
//	Deserialize the byte representation of a leaf node in the memory mapped file.
func deserializeLNode(snode []byte) (*LNode, error) {
	var deserializeErr error

	version, deserializeErr := deserializeUint64(snode[NodeVersionIdx:NodeStartOffsetIdx])
	if deserializeErr != nil { return nil, deserializeErr }

	startOffset, deserializeErr := deserializeUint64(snode[NodeStartOffsetIdx:NodeEndOffsetIdx])
	if deserializeErr != nil { return nil, deserializeErr }

	endOffset, deserializeErr := deserializeUint16(snode[NodeEndOffsetIdx:NodeKeyLength])
	if deserializeErr != nil { return nil, deserializeErr }

	keyLength := uint8(snode[NodeKeyLength])
	return &LNode{
		version: version,
		startOffset: startOffset,
		endOffset: endOffset,
		keyLength: keyLength,
		key: snode[NodeKeyIdx:NodeKeyIdx + keyLength],
		value: snode[NodeKeyIdx + keyLength:],
	}, nil
}

// serializePathToMemMap
//	Serializes a path copy by starting at the root, getting the latest available offset in the memory map, and recursively serializing.
func (mariInst *Mari) serializePathToMemMap(root *INode, nextOffsetInMMap uint64) ([]byte, error) {
	serializedPath, serializeErr := mariInst.serializeRecursive(root, 0, nextOffsetInMMap)
	if serializeErr != nil { return nil, serializeErr }

	return serializedPath, nil
}

// serializeRecursive
//	Traverses the path copy down to the end of the path.
//	If the node is a leaf, serialize it and return. If the node is a internal node, serialize each of the children recursively if
//	the version matches the version of the root. If it is an older version, just serialize the existing offset in the memory map.
func (mariInst *Mari) serializeRecursive(node *INode, level int, offset uint64) ([]byte, error) {
	var serializeErr error
	node.startOffset = offset
	sNode, serializeErr := node.serializeINode(true)
	if serializeErr != nil { return nil, serializeErr }

	serializedKeyVal, serializeErr := node.leaf.serializeLNode()
	if serializeErr != nil { return nil, serializeErr }

	var childrenOnPaths []byte
	nextStartOffset := node.leaf.getEndOffsetLNode() + 1
	for _, child := range node.children {
		if child.version != node.version {
			sNode = append(sNode, serializeUint64(child.startOffset)...)
		} else {
			sNode = append(sNode, serializeUint64(nextStartOffset)...)
			childrenOnPath, serializeErr := mariInst.serializeRecursive(child, level + 1, nextStartOffset)
			if serializeErr != nil { return nil, serializeErr }

			nextStartOffset += getSerializedNodeSize(childrenOnPath)
			childrenOnPaths = append(childrenOnPaths, childrenOnPath...)
		}
	}

	sNode = append(sNode, serializedKeyVal...)
	if len(childrenOnPaths) > 0 { sNode = append(sNode, childrenOnPaths...) }

	mariInst.pool.putLNode(node.leaf)
	mariInst.pool.putINode(node)
	return sNode, nil
}

// serializeLNode
//	Serialize a leaf node in the mariInst. Append the key and value together since both are already byte slices.
func (node *LNode) serializeLNode() ([]byte, error) {
	var sLNode []byte

	node.endOffset = node.determineEndOffsetLNode()
	sVersion := serializeUint64(node.version)
	sStartOffset := serializeUint64(node.startOffset)
	sEndOffset := serializeUint16(node.endOffset)
	sKeyLength := byte(node.keyLength)

	sLNode = append(sLNode, sVersion...)
	sLNode = append(sLNode, sStartOffset...)
	sLNode = append(sLNode, sEndOffset...)
	sLNode = append(sLNode, sKeyLength)
	sLNode = append(sLNode, node.key...)
	sLNode = append(sLNode, node.value...)

	return sLNode, nil
}

// serializeINode
//	Serialize an internal node in the mariInst. This involves scanning the children nodes and serializing the offset in the memory map for each one.
func (node *INode) serializeINode(serializePath bool) ([]byte, error) {
	var sINode []byte

	node.endOffset = node.determineEndOffsetINode()
	node.leaf.startOffset = node.getEndOffsetINode() + 1
	
	sVersion := serializeUint64(node.version)
	sStartOffset := serializeUint64(node.startOffset)
	sEndOffset := serializeUint16(node.endOffset)
	sLeafOffset := serializeUint64(node.leaf.startOffset)
	
	var sBitmap []byte
	for _, subBitmap := range node.bitmap {
		sSubBitmap := serializeUint32(subBitmap)
		sBitmap = append(sBitmap, sSubBitmap...)
	}

	sINode = append(sINode, sVersion...)
	sINode = append(sINode, sStartOffset...)
	sINode = append(sINode, sEndOffset...)
	sINode = append(sINode, sBitmap...)
	sINode = append(sINode, sLeafOffset...)

	if ! serializePath { 
		for _, cnode := range node.children {
			snode := serializeUint64(cnode.startOffset)
			sINode = append(sINode, snode...)
		}
	}

	return sINode, nil
}


//============================================= Helper Functions for Serialize/Deserialize primitives


func serializeUint64(in uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, in)
	return buf
}

func deserializeUint64(data []byte) (uint64, error) {
	if len(data) != 8 { return uint64(0), errors.New("invalid data length for byte slice to uint64") }
	return binary.LittleEndian.Uint64(data), nil
}

func serializeUint32(in uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, in)
	return buf
}

func deserializeUint32(data []byte) (uint32, error) {
	if len(data) != 4 { return uint32(0), errors.New("invalid data length for byte slice to uint32") }
	return binary.LittleEndian.Uint32(data), nil
}

func serializeUint16(in uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, in)
	return buf
}

func deserializeUint16(data []byte) (uint16, error) {
	if len(data) != 2 { return uint16(0), errors.New("invalid data length for byte slice to uint16") }
	return binary.LittleEndian.Uint16(data), nil
}