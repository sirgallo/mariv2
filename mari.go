package mariv2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

//============================================= Mari

// Open initializes Mari
//	This will create the memory mapped file or read it in if it already exists.
//	Then, the meta data is initialized and written to the first 0-23 bytes in the memory map.
//	An initial root MariINode will also be written to the memory map as well.
func Open(opts MariOpts) (*Mari, error) {
	fileWithFilePath := filepath.Join(opts.Filepath, opts.FileName)

	mariInst := &Mari{
		filepath: opts.Filepath,
		opened: true,
		signalCompactChan: make(chan bool),
		signalFlushChan: make(chan bool),
		signalResizeChan: make(chan bool),
	}

	if opts.NodePoolSize != nil {
		nodePoolSize := *opts.NodePoolSize
		mariInst.nodePool = newMariNodePool(nodePoolSize)
	} else { mariInst.nodePool = newMariNodePool(DefaultNodePoolSize) }

	if opts.AppendOnly != nil {
		mariInst.appendOnly = *opts.AppendOnly
	} else { mariInst.appendOnly = false }

	if opts.CompactTrigger != nil {	
		mariInst.compactTrigger = *opts.CompactTrigger
	} else { 
		mariInst.compactTrigger = func(metaData *MariMetaData) bool {
			return metaData.version - 1 >= MaxCompactVersion
		} 
	}

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	
	var openErr error
	mariInst.file, openErr = os.OpenFile(fileWithFilePath, flag, 0600)
	if openErr != nil { return nil, openErr	}

	mariInst.filepath = opts.Filepath
	
	atomic.StoreUint32(&mariInst.isResizing, 0)
	mariInst.data.Store(MMap{})

	openErr = mariInst.initializeFile()
	if openErr != nil { return nil, openErr	}

	go mariInst.compactHandler()
	go mariInst.handleFlush()
	go mariInst.handleResize()

	return mariInst, nil
}

// Close
//	Close Mari, unmapping the file from memory and closing the file.
func (mariInst *Mari) Close() error {
	var closeErr error
	if ! mariInst.opened { return nil }
	mariInst.opened = false

	closeErr = mariInst.file.Sync()
	if closeErr != nil { return closeErr }

	closeErr = mariInst.munmap()
	if closeErr != nil { return closeErr }

	if mariInst.file != nil {
		closeErr = mariInst.file.Close()
		if closeErr != nil { return closeErr }
	}

	return nil
}

// FileSize
//	Determine the memory mapped file size.
func (mariInst *Mari) FileSize() (int, error) {
	stat, statErr := mariInst.file.Stat()
	if statErr != nil { return 0, statErr }

	size := int(stat.Size())
	return size, nil
}

// Remove
//	Close Mari and remove the source file.
func (mariInst *Mari) Remove() error {
	var removeErr error
	
	removeErr = mariInst.Close()
	if removeErr != nil { return removeErr }

	removeErr = os.Remove(mariInst.file.Name())
	if removeErr != nil { return removeErr }

	return nil
}

// initializeFile
//	Initialize the memory mapped file to persist the hamt.
//	If file size is 0, initiliaze the file size to 64MB and set the initial metadata and root values into the map.
//	Otherwise, just map the already initialized file into the memory map.
func (mariInst *Mari) initializeFile() error {
	var initErr error

	fSize, initErr := mariInst.FileSize()
	if initErr != nil { return initErr }

	switch {
		case fSize == 0:
			_, initErr = mariInst.resizeMmap()
			if initErr != nil { return initErr }
			endOffset, initErr := mariInst.initRoot()
			if initErr != nil { return initErr }
			initErr = mariInst.initMeta(endOffset)
			if initErr != nil { return initErr }
		default:
			initErr = mariInst.mmap()
			if initErr != nil { return initErr }
	}

	return nil
}