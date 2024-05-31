package mariv2

import (
	"errors"
	"sync/atomic"
	"unsafe"
)


//============================================= Mari Metadata


// initMeta
//	Initialize and serialize the metadata in a new Mari.
//	Version starts at 0 and increments, and root offset starts at 24.
func (mariInst *Mari) initMeta(nextStart uint64) error {
	newMeta := &MariMetaData{
		version: 0,
		rootOffset: uint64(InitRootOffset),
		nextStartOffset: nextStart,
	}

	serializedMeta := newMeta.serializeMetaData()
	_, flushErr := mariInst.writeMetaToMemMap(serializedMeta)
	if flushErr != nil { return flushErr }
	return nil
}

// loadMetaRootOffsetPointer
//	Get the uint64 pointer from the memory map.
func (mariInst *Mari) loadMetaRootOffset() (ptr *uint64, rOff uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			rOff = 0
			err = errors.New("error getting root offset from mmap")
		}
	}()

	mMap := mariInst.data.Load().(MMap)
	rootOffsetPtr := (*uint64)(unsafe.Pointer(&mMap[MetaRootOffsetIdx]))
	rootOffset := atomic.LoadUint64(rootOffsetPtr)
	return rootOffsetPtr, rootOffset, nil
}

// loadMetaEndMmapPointer
//	Get the uint64 pointer from the memory map.
func (mariInst *Mari) loadMetaEndSerialized() (ptr *uint64, sOff uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			sOff = 0
			err = errors.New("error getting end of serialized data from mmap")
		}
	}()

	mMap := mariInst.data.Load().(MMap)
	endSerializedPtr := (*uint64)(unsafe.Pointer(&mMap[MetaEndSerializedOffset]))
	endSerialized := atomic.LoadUint64(endSerializedPtr)
	return endSerializedPtr, endSerialized, nil
}

// loadMetaVersionPointer
//	Get the uint64 pointer from the memory map.
func (mariInst *Mari) loadMetaVersion() (ptr *uint64, v uint64, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ptr = nil
			v = 0
			err = errors.New("error getting version from mmap")
		}
	}()

	mMap := mariInst.data.Load().(MMap)
	versionPtr := (*uint64)(unsafe.Pointer(&mMap[MetaVersionIdx]))
	version := atomic.LoadUint64(versionPtr)
	return versionPtr, version, nil
}

// storeMetaPointer
//	Store the pointer associated with the particular metadata (root offset, end serialized, version) back in the memory map.
func (mariInst *Mari) storeMetaPointer(ptr *uint64, val uint64) (err error) {
	defer func() {
		r := recover()
		if r != nil { 
			err = errors.New("error storing meta value in mmap")
		}
	}()

	atomic.StoreUint64(ptr, val)
	return nil
}

// writeMetaToMemMap
//	Copy the serialized metadata into the memory map.
func (mariInst *Mari) writeMetaToMemMap(sMeta []byte) (ok bool, err error) {
	defer func() {
		r := recover()
		if r != nil { 
			ok = false
			err = errors.New("error writing metadata to mmap")
		}
	}()

	mMap := mariInst.data.Load().(MMap)
	copy(mMap[MetaVersionIdx:MetaEndSerializedOffset + OffsetSize64], sMeta)

	flushErr := mariInst.flushRegionToDisk(MetaVersionIdx, MetaEndSerializedOffset + OffsetSize64)
	if flushErr != nil { return false, flushErr }
	return true, nil
}