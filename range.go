package mariv2

import (
	"bytes"
	"unsafe"
)


//============================================= Mari Range


// rangeRecursive
//	Limit the indexes to check in the range at level 0, and then recursively traverse the paths between the start and end index.
//	On the start key path, continue to use the start index to check the level to see which index forward should be recursively checked.
//	The opposite is done for the end key path.
func (mariInst *Mari) rangeRecursive(node *unsafe.Pointer, minVersion uint64, startKey, endKey []byte, level int, transform Transform) ([]*KeyValuePair, error) {
	genKeyValPair := func(node *INode) *KeyValuePair { return &KeyValuePair { Key: node.leaf.key, Value: node.leaf.value } }
	currNode := loadINodeFromPointer(node)

	var sortedKvPairs []*KeyValuePair
	var startKeyPos, endKeyPos int
	if level > 0 {
		switch {
			case startKey != nil && len(startKey) > level:
				if currNode.leaf.version >= minVersion && bytes.Compare(currNode.leaf.key, startKey) == 1 {
					sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode)))
				} else { return sortedKvPairs, nil }

				startKeyIndex := getIndexForLevel(startKey, level)
				startKeyPos = getPosition(currNode.bitmap, startKeyIndex, level)
				endKeyPos = len(currNode.children)
			case endKey != nil && len(endKey) > level:
				if currNode.leaf.version >= minVersion && bytes.Compare(currNode.leaf.key, endKey) == -1 {
					sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode)))
				} else { return sortedKvPairs, nil }

				startKeyPos = 0
				endKeyIndex := getIndexForLevel(endKey, level)
				endKeyPos = getPosition(currNode.bitmap, endKeyIndex, level)
			default:
				if currNode.leaf.version >= minVersion && len(currNode.leaf.key) > 0 { sortedKvPairs = append(sortedKvPairs, transform(genKeyValPair(currNode))) }
				startKeyPos = 0
				endKeyPos = len(currNode.children)
		}
	} else {
		switch {
			case startKey == nil && endKey == nil:
				startKeyPos = 0
				endKeyPos = len(currNode.children)
			default:
				startKeyIndex := getIndexForLevel(startKey, 0)
				startKeyPos = getPosition(currNode.bitmap, startKeyIndex, 0)
				endKeyIndex := getIndexForLevel(endKey, 0)
				endKeyPos = getPosition(currNode.bitmap, endKeyIndex, 0)
		}
	}

	if len(currNode.children) > 0 {
		var rangeErr error
		var childNode *INode
		var childPtr *unsafe.Pointer
		var kvPairs []*KeyValuePair

		switch {
			case startKeyPos == endKeyPos:
				childNode, rangeErr = mariInst.getChildNode(currNode.children[startKeyPos], currNode.version)
				if rangeErr != nil { return nil, rangeErr}
				childPtr = storeINodeAsPointer(childNode)

				kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, startKey, endKey, level + 1, transform)
				if rangeErr != nil { return nil, rangeErr }
				if len(kvPairs) > 0 { sortedKvPairs = append(sortedKvPairs, kvPairs...) }
			default:
				for idx, childOffset := range currNode.children[startKeyPos:endKeyPos] {		
					childNode, rangeErr = mariInst.getChildNode(childOffset, currNode.version)
					if rangeErr != nil { return nil, rangeErr }
					
					childPtr = storeINodeAsPointer(childNode)
					switch {
						case idx == 0 && startKey != nil:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, startKey, nil, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
						case idx == endKeyPos && endKey != nil:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, nil, endKey, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
						default:
							kvPairs, rangeErr = mariInst.rangeRecursive(childPtr, minVersion, nil, nil, level + 1, transform)
							if rangeErr != nil { return nil, rangeErr }
					}
		
					if len(kvPairs) > 0 { sortedKvPairs = append(sortedKvPairs, kvPairs...) }
				}
		}
	}

	return sortedKvPairs, nil
}