package mariv2

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"
)


//============================================= Mari Compact


// newCompaction
//	Instatiate the compaction strategy on compaction signal.
//	Creates a new temporary memory mapped file where the version to be snapshotted will be written to.
func (mariInst *Mari) newCompaction(compactedVersion uint64) (*MariCompaction, error) {
	var compactErr error
	tempFileName := mariInst.file.Name() + "temp"

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	tempFile, compactErr := os.OpenFile(tempFileName, flag, 0600)
	if compactErr != nil { return nil, compactErr }

	compact := &MariCompaction{ 
		tempFile: tempFile,
		compactedVersion: compactedVersion,
	}

	compact.tempData.Store(MMap{})
	compactErr = compact.resizeTempFile(0)
	if compactErr != nil { return nil, compactErr }
	return compact, nil
}

// signalCompact
//	When the maximum version is reached, signal the compaction go routine
func (mariInst *Mari) signalCompact() {
	select { 
		case mariInst.signalCompactChan <- true:
		default:
	}
}

// compactHandler
//	Run in a separate go routine.
//	On signal, sets the resizing flag and acquires the write lock.
//	The current root is loaded and then the elements are recursively written to the new file.
//	On completion, the original memory mapped file is removed and the new file is swapped in.
func (mariInst *Mari) compactHandler() {
	for range mariInst.signalCompactChan {
		cErr := func() error {
			for ! atomic.CompareAndSwapUint32(&mariInst.isResizing, 0, 1) { runtime.Gosched() }
			defer atomic.StoreUint32(&mariInst.isResizing, 0)

			mariInst.rwResizeLock.Lock()
			defer mariInst.rwResizeLock.Unlock()

			var compactErr error
			_, rootOffset, compactErr := mariInst.loadMetaRootOffset()
			if compactErr != nil { return compactErr }
		
			currRoot, compactErr := mariInst.readINodeFromMemMap(rootOffset)
			if compactErr != nil { return compactErr }
		
			compact, compactErr := mariInst.newCompaction(currRoot.version)
			if compactErr != nil { return compactErr }
		
			currRootPtr := storeINodeAsPointer(currRoot)
			endOff, compactErr := mariInst.serializeCurrentVersionToNewFile(compact, currRootPtr, 0, 0, InitRootOffset)
			if compactErr != nil { 
				os.Remove(compact.tempFile.Name())
				return compactErr 
			}
		
			newMeta := &MariMetaData{
				version: 0,
				rootOffset: uint64(InitRootOffset),
				nextStartOffset: endOff,
			}
		
			serializedMeta := newMeta.serializeMetaData()
			_, compactErr = compact.writeMetaToTempMemMap(serializedMeta)
			if compactErr != nil { 
				os.Remove(compact.tempFile.Name())
				return compactErr 
			}
			
			compactErr = mariInst.swapTempFileWithMari(compact)
			if compactErr != nil { 
				os.Remove(compact.tempFile.Name())
				return compactErr 
			}

			return nil
		}()

		if cErr != nil { fmt.Println("error on compaction process:", cErr) }
	}
}

// serializeCurrentVersionToNewFile
//	Recursively builds the new copy of the current version to the new file.
//	All previous unused paths are discarded.
//	At each level, the nodes are directly written to the memory map as to avoid loading the entire structure into memory.
func (mariInst *Mari) serializeCurrentVersionToNewFile(compact *MariCompaction, node *unsafe.Pointer, level int, version, offset uint64) (uint64, error) {
	currNode := loadINodeFromPointer(node)
	
	currNode.version = version
	currNode.startOffset = offset
	currNode.leaf.version = version

	var serializeErr error
	sNode, serializeErr := currNode.serializeINode(true)
	if serializeErr != nil { return 0, serializeErr }

	serializedKeyVal, serializeErr := currNode.leaf.serializeLNode()
	if serializeErr != nil { return 0, serializeErr }

	nextStartOffset := currNode.leaf.getEndOffsetLNode() + 1

	if len(currNode.children) > 0 {
		var childNode *MariINode
		var childPtr *unsafe.Pointer
		var updatedOffset uint64

		for _, child := range currNode.children {
			sNode = append(sNode, serializeUint64(nextStartOffset)...)
	
			childNode, serializeErr = mariInst.readINodeFromMemMap(child.startOffset)
			if serializeErr != nil { return 0, serializeErr }
	
			childPtr = storeINodeAsPointer(childNode)
			updatedOffset, serializeErr = mariInst.serializeCurrentVersionToNewFile(compact, childPtr, level + 1, version, nextStartOffset)
			if serializeErr != nil { return 0, serializeErr }
	
			nextStartOffset = updatedOffset
		}
	}

	serializeErr = compact.resizeTempFile(currNode.leaf.getEndOffsetLNode() + 1)
	if serializeErr != nil { return 0, serializeErr }

	sNode = append(sNode, serializedKeyVal...)

	temp := compact.tempData.Load().(MMap)
	copy(temp[currNode.startOffset:currNode.leaf.getEndOffsetLNode() + 1], sNode)
	return nextStartOffset, nil
}

// swapTempFileWithMari
//	Close the current mari memory mapped file and swap the new compacted copy.
//	Rebuild the version index on compaction
func (mariInst *Mari) swapTempFileWithMari(compact *MariCompaction) error {
	currFileName := mariInst.file.Name()
	tempFileName := compact.tempFile.Name()
	swapFileName := mariInst.file.Name() + "swap"

	var swapErr error
	swapErr = mariInst.Close()
	if swapErr != nil { return swapErr }

	swapErr = compact.tempFile.Sync()
	if swapErr != nil { return swapErr }

	swapErr = compact.munmapTemp()
	if swapErr != nil { return swapErr }

	swapErr = compact.tempFile.Close()
	if swapErr != nil { return swapErr }
	
	os.Rename(currFileName, swapFileName)
	os.Rename(tempFileName, currFileName)

	os.Remove(swapFileName)

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	mariInst.file, swapErr = os.OpenFile(currFileName, flag, 0600)
	if swapErr != nil { return swapErr }

	swapErr = mariInst.mmap()
	if swapErr != nil { return swapErr }
	return nil
}


// mMapTemp
//	Mmap helper for the temporary memory mapped file.
func (compact *MariCompaction) mMapTemp() error {
	temp, tempErr := Map(compact.tempFile, RDWR, 0)
	if tempErr != nil { return tempErr }

	compact.tempData.Store(temp)
	return nil
}

// munmapTemp
//	Unmap helper for the tempory memory mapped file
func (compact *MariCompaction) munmapTemp() error {
	temp := compact.tempData.Load().(MMap)
	unmapErr := temp.Unmap()
	if unmapErr != nil { return unmapErr }

	compact.tempData.Store(MMap{})
	return nil
}

// resizeTempFile
//	As the new copy is being built, the file will need to be resized as more elements are appended.
//	Follow a similar strategy to the resizeFile method for the mari memory map.
func (compact *MariCompaction) resizeTempFile(offset uint64) error {
	temp := compact.tempData.Load().(MMap)
	if offset > 0 && int(offset) < len(temp) { return nil }
	
	allocateSize := func() int64 {
		switch {
			case len(temp) == 0:
				return int64(DefaultPageSize) * 16 * 1000 // 64MB
			case len(temp) >= MaxResize:
				return int64(len(temp) + MaxResize)
			default:
				return int64(len(temp) * 2)
		}
	}()

	var resizeErr error
	if len(temp) > 0 {
		resizeErr = compact.tempFile.Sync()
		if resizeErr != nil { return resizeErr }
		
		resizeErr = compact.munmapTemp()
		if resizeErr != nil { return resizeErr }
	}

	resizeErr = compact.tempFile.Truncate(allocateSize)
	if resizeErr != nil { return resizeErr }

	resizeErr = compact.mMapTemp()
	if resizeErr != nil { return resizeErr }
	return nil
}

// writeMetaToTempMemMap
//	Copy the serialized metadata into the memory map.
func (compact *MariCompaction) writeMetaToTempMemMap(sMeta []byte) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ok = false
			err = errors.New("error writing metadata to mmap")
		}
	}()

	temp := compact.tempData.Load().(MMap)
	copy(temp[MetaVersionIdx:MetaEndSerializedOffset + OffsetSize64], sMeta)

	flushErr := compact.tempFile.Sync()
	if flushErr != nil { return false, flushErr }
	return true, nil
}