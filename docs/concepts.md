# concepts

### a concurrent ordered array mapped trie with path copying and copy on write


## issue

One issue when working with memory mapped files is ensuring data integrity on concurrent operations, especially when trying to implement an immutable datastructure like a concurrent ordered array mapped trie (`coamt`). Because of this, most `coamts` are implemented as in-memory only, which means that while the data structure itself is immutable, the data is volatile and the data structure will not persist on system restarts. This implementation looks to create a persistent version of a `coamt` so that the data structure is serialized and stored to disk, eliminating the need to rebuild the entire structure if a system restarts.


## proposal

`mari` would extend the current `coamt` path copying technique with a form of versioning to ensure data integrity and allow multiple write operations to succeed, using retries. This is inspired by the `atomic compare-and-swap` operations used in the `coamt`, where many concurrent operations can attempt to update pointers to new nodes but if a concurrent operation is already modifying the same location, the operation is discarded and retried back at the root of the data structure. The memory mapped file for `mari` will be treated as an append only data structure, where data can only be appended to the buffer but cannot be modified or removed, making the memory mapped file a view of all operations that have ever occured on the data structure. Instead of performing an atomic compare and swap operation on the original node, replacing the original with the copy, the operation will create a copy of each node in the path with a new version of the node starting at the root, all the way down the structure until the updated value. This is essentially taking the path copying concept from the in-memory `coamt` and updates it to a form of `Copy-on-Write`. Every node will contain both the offset from the beginning of the memory mapped file, as well as the current version of the node, increasing from `0`.

At the start of the memory mapped file, the first 24 bytes will be allocated by default to hold metadata regarding the current version of `mari`, as well as the offset for the location of the root of the trie. When a path copy is created, the entire copy from the new node up to the root, with new version numbers, will be returned to the start of the operation. This path is not an entire copy of the trie, but just a copy of the path down to the new node, and new versions of nodes can point to nodes of different versions in the memory mapped file if they were not in the path. The version only truly matters for the root node and metadata, since the root node is the entry point into the data structure for all operations. The copy then will perform a check against the metadata page. If its version number is higher than the version number currently in the metadata page, it is assumed that the operation can be safely applied to the memory mapped file since no other threads modified the structure at the same time. Atomic updates will be applied on the version, as well as on the pageId and offset pointing to the location of the new root for the trie. The copy will then be serialized and appended to the end of the memory mapped file and all operations will then start traversing the trie at the new root location. If the operation finds that after performing path copying the metadata page holds a value equal to its own or higher, the copy is discarded, garbage collection cleans up the copied nodes, and the operation is retried from the new root node of the data structure.


## design

`mari` has a few points to touch upon:

  1. Lock-free reads and writes to the memory map using atomic operations and versioning
  2. An append only memory map where modified paths are appended to the end of the serialized chamt
  3. Non-blocking flushing, or "optimistic" flushing
  4. Dynamic memory map resizing


### lock free multi reader/writer

Reads and Writes to and from the memory map use a lock free approach. As mentioned in the proposal, the first 24 bytes of the memory map are reserved for metadata, which includes:
```
0-7: version
8-15: current root offset
16-23: the offset of the end of the serialized data
```

A retry mechanism is in place where when a thread attempts to modify or read the memory map, the latest version is first read from the metadata block at the beginning of the memory map. This version is used in two ways:

`Writes`

When a process attempts to write to the data structure, it begins at the version found in the metadata, increments the version by 1, and builds a complete path copy down to the modified node, updating the version of each node that it passes as it traverses the trie. When the path has been copied, it then performs a version check to ensure if the version of the path is 1 more than the version that it attempted to update from, and also attempts to compare and swap the location in the memory map with the new version. If it is successful, the path is serialized, written to the memory map, and the metadata for the rootoffset and end of the serialized data is updated to reflect this and all new operations will point to the new root. Otherwise, if the operation fails, it begins back at the beginning, reading from the new root.

`Reads`

Reads check the latest version in the metadata and traverse the path down to the node where the key-value will be. No retries are required and writes with later versions can continue to append to the map while reads occur.

### append only mem map

To ensure data integrity, in combination with versioning, the serialized data in the memory map is treated as an append only data structure. All write operations (put and delete) will never modify existing data, but will instead serialize the path copy and append the copy to the end of the serialized data. When appended, the process will also update the metadata to point to the location in memory of the new root. This makes the entire data structure truly immutable.

### optimistic flushing

"Optimistic" flushing is basically non-blocking flushing. A separate go routine takes care of flushing data and every write to the memory map attempts persisting the new updates to the memory map to disk. If a flush operation is already occuring, the write operation will continue and not block other attempts to write to the memory map. If a flush operation is not running, then the routine is signalled and flush begins. This approach attempts to find a middle ground between data integrity and throughput, where in situations where there is extremely high concurrency, changes to the memory map are batched and many writes will be flushed at once. It is "optimistic" because every write attempts to flush latest changes to disk, but if unable will not block.

The flush go routine, which uses `os.File.Sync()`:
```go
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
```

and the non-blocking signal to flush:
```go
func (mariInst *Mari) signalFlush() {
	select {
		case mariInst.signalFlushChan <- true:
		default:
	}
}
```

### dynamic mem map resizing

On initialization, the memory mapped file is resized to a `64MB` size. Once this size has been exhausted, the size is doubled each time the size limit is hit until `1GB`, where the file is then resized in `1GB` blocks every resize operation. The resize operation also incorporates a combination of atomic flags and a read/write lock to ensure that other processes trying to read/write to the memory map cannot interact with it until the resize operation completes. First, the operation resizing performs a `compare-and-swap` operation on the atomic flag. When set, it aquires the write lock and begins the resize process. All other threads first check if the flag is set, and then wait until the flag is unset, and then attempt to aquire a read lock. If the process can successfully aquire the read lock, it continues its operation. Both read and write operations aquire the read lock. This ensures that all reads and writes will complete their process before the resize operation can aquire the write lock. The resize operation is run in a separate go routine and signalled by the first process trying to modify the memory to find that the length of the memory map will be unable to fit the new serialized path copy.

The go routine to perform resizing:
```go
func (mariInst *Mari) handleResize() {
	for range mariInst.signalResizeChan { mariInst.resizeMmap() }
}
```

The function to determine when to resize:
```go
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
```