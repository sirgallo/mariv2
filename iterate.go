package mariv2

import (
	"bytes"
	"unsafe"
)

//============================================= Mari Iterate


// iterateRecursive
//	Essentially create a cursor that begins at the specified start key.
//	Recursively builds an accumulator of key value pairs until it reaches the max size.
func (mariInst *Mari) iterateRecursive(
	node *unsafe.Pointer, minVersion uint64, 
	startKey []byte, totalResults, level int, 
	acc []*KeyValuePair, transform MariOpTransform,
) ([]*KeyValuePair, error) {
	genKeyValPair := func(node *MariINode) *KeyValuePair {
		kvPair := &KeyValuePair {
			// Version: node.leaf.version,
			Key: node.leaf.key,
			Value: node.leaf.value,
		}

		return kvPair
	}

	currNode := loadINodeFromPointer(node)

	var startKeyPos int
	if level > 0 {
		switch {
			case totalResults == len(acc):
				return acc, nil
			case len(startKey) == level:
				if currNode.leaf.version >= minVersion { acc = append(acc, transform(genKeyValPair(currNode))) }
				startKeyPos = 0
			case startKey != nil && len(startKey) > level:
				if bytes.Compare(currNode.leaf.key, startKey) == 1 || bytes.Equal(currNode.leaf.key, startKey) {
					if currNode.leaf.version >= minVersion { acc = append(acc, transform(genKeyValPair(currNode))) }
				}

				startKeyIndex := getIndexForLevel(startKey, level)
				startKeyPos = getPosition(currNode.bitmap, startKeyIndex, level)
			default:
				if currNode.leaf.version >= minVersion && len(currNode.leaf.key) > 0 { 
					acc = append(acc, transform(genKeyValPair(currNode)))
				} 

				startKeyPos = 0
		}
	} else {
		startKeyIdx := getIndexForLevel(startKey, level)
		startKeyPos = getPosition(currNode.bitmap, startKeyIdx, level)
	}

	if len(currNode.children) > 0 {
		var iterErr error
		var childNode *MariINode
		var childPtr *unsafe.Pointer
		
		currPos := startKeyPos
		for totalResults > len(acc) && currPos < len(currNode.children) {
			childOffset := currNode.children[currPos]

			childNode, iterErr = mariInst.getChildNode(childOffset, currNode.version)
			if iterErr != nil { return nil, iterErr}
			childPtr = storeINodeAsPointer(childNode)

			switch {
				case currPos == startKeyPos && startKey != nil:
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, startKey, totalResults, level + 1, acc, transform)
					if iterErr != nil { return nil, iterErr }
				default:
					acc, iterErr = mariInst.iterateRecursive(childPtr, minVersion, nil, totalResults, level + 1, acc, transform)
					if iterErr != nil { return nil, iterErr }
			}

			currPos++
		}
	}

	return acc, nil
}