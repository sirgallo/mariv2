package mariv2

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)


//============================================= Mari IO Utils


// compareAndSwap
//	Performs CAS operation.
func (mariInst *Mari) compareAndSwap(node *unsafe.Pointer, currNode, nodeCopy *INode) bool {
	if atomic.CompareAndSwapPointer(node, unsafe.Pointer(currNode), unsafe.Pointer(nodeCopy)) {
		return true
	} else {
		mariInst.pool.iPool.Put(nodeCopy.leaf)
		mariInst.pool.lPool.Put(nodeCopy)
		return false
	}
}

// determineIfResize
//	Helper function that signals go routine for resizing if the condition to resize is met.
func (mariInst *Mari) determineIfResize(offset uint64) bool {
	mMap := mariInst.data.Load().(MMap)
	switch {
		case offset > 0 && int(offset) < len(mMap):
			return false
		case len(mMap) == 0 || ! atomic.CompareAndSwapUint32(&mariInst.isResizing, 0, 1):
			return true
		default:
			mariInst.signalResizeChan <- true
			return true
	}
}

// flushRegionToDisk
//	Flushes a region of the memory map to disk instead of flushing the entire map. 
//	When a startoffset is provided, if it is not aligned with the start of the last page, the offset needs to be normalized.
func (mariInst *Mari) flushRegionToDisk(startOffset, endOffset uint64) error {
	startOffsetOfPage := startOffset & ^(uint64(DefaultPageSize) - 1)
	mMap := mariInst.data.Load().(MMap)
	if len(mMap) == 0 { return nil }

	flushErr := mMap[startOffsetOfPage:endOffset].Flush()
	if flushErr != nil { return flushErr }

	return nil
}

// handleFlush
//	This is "optimistic" flushing. 
//	A separate go routine is spawned and signalled to flush changes to the mmap to disk.
func (mariInst *Mari) handleFlush() {
	for range mariInst.signalFlushChan {
		func() {
			for atomic.LoadUint32(&mariInst.isResizing) == 1 { runtime.Gosched() }
			
			mariInst.rwResizeLock.RLock()
			defer mariInst.rwResizeLock.RUnlock()

			mariInst.file.Sync()
		}()
	}
}

// handleResize
//	A separate go routine is spawned to handle resizing the memory map.
//	When the mmap reaches its size limit, the go routine is signalled.
func (mariInst *Mari) handleResize() {
	for range mariInst.signalResizeChan { mariInst.resizeMmap() }
}

// mmap
//	Helper to memory map the mariInst File in to buffer.
func (mariInst *Mari) mmap() error {
	mMap, mmapErr := Map(mariInst.file, RDWR, 0)
	if mmapErr != nil { return mmapErr }

	mariInst.data.Store(mMap)
	return nil
}

// munmap
//	Unmaps the memory map from RAM.
func (mariInst *Mari) munmap() error {
	mMap := mariInst.data.Load().(MMap)
	unmapErr := mMap.Unmap()
	if unmapErr != nil { return unmapErr }

	mariInst.data.Store(MMap{})
	return nil
}

// resizeMmap
//	Dynamically resizes the underlying memory mapped file.
//	When a file is first created, default size is 64MB and doubles the mem map on each resize until 1GB.
func (mariInst *Mari) resizeMmap() (bool, error) {
	var resizeErr error
	mariInst.rwResizeLock.Lock()
	
	defer mariInst.rwResizeLock.Unlock()
	defer atomic.StoreUint32(&mariInst.isResizing, 0)

	mMap := mariInst.data.Load().(MMap)
	allocateSize := func() int64 {
		switch {
			case len(mMap) == 0:
				return int64(DefaultPageSize) * 16 * 1000 // 64MB
			case len(mMap) >= MaxResize:
				return int64(len(mMap) + MaxResize)
			default:
				return int64(len(mMap) * 2)
		}
	}()

	if len(mMap) > 0 {
		resizeErr = mariInst.file.Sync()
		if resizeErr != nil { return false, resizeErr }
		
		resizeErr = mariInst.munmap()
		if resizeErr != nil { return false, resizeErr }
	}

	resizeErr = mariInst.file.Truncate(allocateSize)
	if resizeErr != nil { return false, resizeErr }

	resizeErr = mariInst.mmap()
	if resizeErr != nil { return false, resizeErr }

	return true, nil
}

// signalFlush
//	Called by all writes to "optimistically" handle flushing changes to the mmap to disk.
func (mariInst *Mari) signalFlush() {
	select {
		case mariInst.signalFlushChan <- true:
		default:
	}
}

// exclusiveWriteMmap
//	Takes a path copy and writes the nodes to the memory map, then updates the metadata.
func (mariInst *Mari) exclusiveWriteMmap(path *INode) (bool, error) {
	if atomic.LoadUint32(&mariInst.isResizing) == 1 { return false, nil }

	var writeErr error
	versionPtr, version, writeErr := mariInst.loadMetaVersion()
	if writeErr != nil { return false, nil }

	rootOffsetPtr, prevRootOffset, writeErr := mariInst.loadMetaRootOffset()
	if writeErr != nil { return false, nil }

	endOffsetPtr, endOffset, writeErr := mariInst.loadMetaEndSerialized()
	if writeErr != nil { return false, nil }

	newVersion := path.version
	newOffsetInMMap := endOffset
	
	serializedPath, writeErr := mariInst.serializePathToMemMap(path, newOffsetInMMap)
	if writeErr != nil { return false, writeErr }

	updatedMeta := &MetaData{
		version: newVersion,
		rootOffset: newOffsetInMMap,
		nextStartOffset: newOffsetInMMap + uint64(len(serializedPath)),
	}

	isResize := mariInst.determineIfResize(updatedMeta.nextStartOffset)
	if isResize { return false, nil }

	if ! mariInst.appendOnly && mariInst.compactTrigger(updatedMeta) {
		mariInst.signalCompact()
		return false, nil
	}
	
	if atomic.LoadUint32(&mariInst.isResizing) == 0 {
		if version == updatedMeta.version - 1 && atomic.CompareAndSwapUint64(versionPtr, version, updatedMeta.version) {
			mariInst.storeMetaPointer(endOffsetPtr, updatedMeta.nextStartOffset)
			
			_, writeErr = mariInst.writeNodesToMemMap(serializedPath, newOffsetInMMap)
			if writeErr != nil {
				mariInst.storeMetaPointer(endOffsetPtr, endOffset)
				mariInst.storeMetaPointer(versionPtr, version)
				mariInst.storeMetaPointer(rootOffsetPtr, prevRootOffset)

				return false, writeErr
			}
			
			mariInst.storeMetaPointer(rootOffsetPtr, updatedMeta.rootOffset)
			mariInst.signalFlush()

			return true, nil
		}
	}

	return false, nil
}