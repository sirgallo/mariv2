package mariv2

import (
	"bytes"
	"errors"
	"runtime"
	"sync/atomic"
	"unsafe"
)


//============================================= Mari Transaction


// newTx
//	Creates a new transaction.
//	The current root is operated on for "Optimistic Concurrency Control".
//	If isWrite is false, then write operations in the read only transaction will fail.
func newTx(mariInst *Mari, rootPtr *unsafe.Pointer, isWrite bool) *Tx {
	return &Tx{ store: mariInst, root: rootPtr, isWrite: isWrite }
}

// ReadTx
//	Handles all read related operations.
//	It gets the latest version of the ordered array mapped trie and starts from that offset in the mem-map.
//	Get is concurrent since it will perform the operation on an existing path, so new paths can be written at the same time with new versions.
func (mariInst *Mari) ReadTx(txOps func(tx *Tx) error) error {
	var readTxErr error
	for atomic.LoadUint32(&mariInst.isResizing) == 1 { runtime.Gosched() }
	
	mariInst.rwResizeLock.RLock()
	defer mariInst.rwResizeLock.RUnlock()

	var rootOffset uint64
	_, rootOffset, readTxErr = mariInst.loadMetaRootOffset()
	if readTxErr != nil { return readTxErr }

	var currRoot *INode
	currRoot, readTxErr = mariInst.readINodeFromMemMap(rootOffset)
	if readTxErr != nil { return readTxErr }

	rootPtr := storeINodeAsPointer(currRoot)
	transaction := newTx(mariInst, rootPtr, false)
	readTxErr = txOps(transaction)
	if readTxErr != nil { return readTxErr }

	return nil
}

// UpdateTx
//	Handles all read-write related operations.
//	If the operation fails, the copied and modified path is discarded and the operation retries back at the root until completed.
//	The operation begins at the latest known version of root, reads from the metadata in the memory map.
//	The version of the copy is incremented and if the metadata is the same after the path copying has occured, the path is serialized and appended to the memory-map.
//	The metadata is also being updated to reflect the new version and the new root offset.
func (mariInst *Mari) UpdateTx(txOps func(tx *Tx) error) error {
	var updateTxErr error
	var currRoot, updatedRootCopy *INode
	var rootOffset, version uint64
	var versionPtr *uint64

	for {
		for atomic.LoadUint32(&mariInst.isResizing) == 1 { runtime.Gosched() }
		mariInst.rwResizeLock.RLock()

		versionPtr, version, updateTxErr = mariInst.loadMetaVersion()
		if updateTxErr != nil { return updateTxErr }

		if version == atomic.LoadUint64(versionPtr) {
			_, rootOffset, updateTxErr = mariInst.loadMetaRootOffset()
			if updateTxErr != nil { return updateTxErr }
	
			currRoot, updateTxErr = mariInst.readINodeFromMemMap(rootOffset)
			if updateTxErr != nil {
				mariInst.rwResizeLock.RUnlock()
				return updateTxErr
			}
	
			currRoot.version = currRoot.version + 1
			rootPtr := storeINodeAsPointer(currRoot)
			
			transaction := newTx(mariInst, rootPtr, true)
			updateTxErr = txOps(transaction)
			if updateTxErr != nil { return updateTxErr }

			updatedRootCopy = loadINodeFromPointer(rootPtr)
			ok, updateTxErr := mariInst.exclusiveWriteMmap(updatedRootCopy)
			if updateTxErr != nil {
				mariInst.rwResizeLock.RUnlock()
				return updateTxErr
			}

			if ok {
				mariInst.rwResizeLock.RUnlock() 
				return nil
			}
		}

		mariInst.rwResizeLock.RUnlock()
		runtime.Gosched()
	}
}

// Put 
//	Inserts or updates key-value pair into the ordered array mapped trie.
//	The operation begins at the root of the trie and traverses through the tree until the correct location is found, copying the entire path.
func (tx *Tx) Put(key, value []byte) error {
	if ! tx.isWrite { return errors.New("attempting to perform a write in a read only transaction, use tx.UpdateTx") }

	_, putErr := tx.store.putRecursive(tx.root, key, value, 0)
	if putErr != nil { return putErr }
	return nil
}

// Get
//	Attempts to retrieve the value for a key within the ordered array mapped trie.
//	The operation begins at the root of the trie and traverses down the path to the key.
func (tx *Tx) Get(key []byte, transform *Transform) (*KeyValuePair, error) {
	var newTransform Transform
	if transform != nil {
		newTransform = *transform
	} else { newTransform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	return tx.store.getRecursive(tx.root, key, 0, newTransform)
}

// Delete 
//	Attempts to delete a key-value pair within the ordered array mapped trie.
//	It starts at the root of the trie and recurses down the path to the key to be deleted.
//	The operation creates an entire, in-memory copy of the path down to the key.
func (tx *Tx) Delete(key []byte) error {
	if ! tx.isWrite { return errors.New("attempting to perform a write in a read only transaction, use tx.UpdateTx") }

	_, delErr := tx.store.deleteRecursive(tx.root, key, 0)
	if delErr != nil { return delErr }
	return nil
}

// Iterate
//	Creates an ordered iterator starting at the given start key up to the range specified by total results.
//	Since the array mapped trie is sorted, the iterate function starts at the startKey and recursively builds the result set up the specified end.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
// 	If nil is passed for the transformer, then the kv pair will be returned as is.
func (tx *Tx) Iterate(startKey []byte, totalResults int, opts *RangeOpts) ([]*KeyValuePair, error) {
	var minV uint64 
	var transform Transform
	if opts != nil && opts.MinVersion != nil {
		minV = *opts.MinVersion
	} else { minV = 0 }

	if opts != nil && opts.Transform != nil {
		transform = *opts.Transform
	} else { transform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	kvPairs, iterErr := tx.store.iterateRecursive(tx.root, minV, startKey, totalResults, 0, []*KeyValuePair{}, transform)
	if iterErr != nil { return nil, iterErr }
	return kvPairs, nil
}

// Range
//	Since the array mapped trie is sorted by nature, the range operation begins at the root of the trie.
//	It checks the root bitmap and determines which indexes to check in the range.
//	It then recursively checks each index, traversing the paths and building the sorted results.
//	A minimum version can be provided which will limit results to the min version forward.
//	If nil is passed for the minimum version, the earliest version in the structure will be used.
// 	If nil is passed for the transformer, then the kv pair will be returned as is.
func (tx *Tx) Range(startKey, endKey []byte, opts *RangeOpts) ([]*KeyValuePair, error) {
	if bytes.Compare(startKey, endKey) == 1 { return nil, errors.New("start key is larger than end key") }

	var minV uint64 
	var transform Transform
	if opts != nil && opts.MinVersion != nil {
		minV = *opts.MinVersion
	} else { minV = 0 }

	if opts != nil && opts.Transform != nil {
		transform = *opts.Transform
	} else { transform = func(kvPair *KeyValuePair) *KeyValuePair { return kvPair } }

	kvPairs, rangeErr := tx.store.rangeRecursive(tx.root, minV, startKey, endKey, 0, transform)
	if rangeErr != nil { return nil, rangeErr }
	return kvPairs, nil
}