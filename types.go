package mariv2

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)


// MMap is the byte array representation of the memory mapped file in memory.
type MMap []byte

// MariOpts initialize the Mari
type InitOpts struct {
	// Filepath: the path to the memory mapped file
	Filepath string
	// FileName: the name of the file for the mari instance
	FileName string
	// NodePoolSize: the total number of pre-allocated nodes to create in the node pool
	NodePoolSize *int64
	// CompactionTrigger: the custom compaction trigger function
	CompactTrigger *CompactionTrigger
	// AppendOnly: optionally pass true to stop the compaction process from occuring
	AppendOnly *bool
}

// MariMetaData contains information related to where the root is located in the mem map and the version.
type MetaData struct {
	// Version: a tag for Copy-on-Write indicating the version of Mari
	version uint64
	// RootOffset: the offset of the latest version root node in Mari
	rootOffset uint64
	// NextStartOffset: the offset where the last node in the mmap is located
	nextStartOffset uint64
}

// MariNode represents a singular node within the hash array mapped trie data structure.
type INode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	startOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	endOffset uint16
	// Bitmap: a 256 bit sparse index that indicates the location of each hashed key within the array of child nodes. Only stored in internal nodes
	bitmap [8]uint32
	// LeafOffset: the offset of the leaf node associated with the current byte chunk
	leaf *LNode
	// Children: an array of child nodes, which are MariINodes. Location in the array is determined by the sparse index
	children []*INode
}

// MariNode represents a singular node within the hash array mapped trie data structure.
type LNode struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	version uint64
	// StartOffset: the offset from the beginning of the serialized node is located
	startOffset uint64
	// EndOffset: the offset from the end of the serialized node is located
	endOffset uint16
	// KeyLength: the length of the key in a Leaf Node. Keys can be variable size
	keyLength uint8
	// Key: The key associated with a value. Keys are in byte array representation. Keys are only stored within leaf nodes
	key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	value []byte
}

// KeyValuePair
type KeyValuePair struct {
	// Version: a tag for Copy-on-Write indicating the version of the node
	// Version uint64
	// Key: The key associated with a value. Keys are in byte array representation. Keys are only stored within leaf nodes
	Key []byte
	// Value: The value associated with a key, in byte array representation. Values are only stored within leaf nodes
	Value []byte
}

// Mari contains the memory mapped buffer for Mari, as well as all metadata for operations to occur
type Mari struct {
	// filepath: path to the Mari file
	filepath string
	// file: the Mari file
	file *os.File
	// opened: flag indicating if the file has been opened
	opened bool
	// data: the memory mapped file as a byte slice
	data atomic.Value
	// isResizing: atomic flag to determine if the mem map is being resized or not
	isResizing uint32
	// signalResize: send a signal to the resize go routine with the offset for resizing
	signalResizeChan chan bool
	// signalFlush: send a signal to flush to disk on writes to avoid contention
	signalFlushChan chan bool
	// signalCompactChan: send a signal to compact the database
	signalCompactChan chan bool
	// ReadResizeLock: A Read-Write mutex for locking reads on resize operations
	rwResizeLock sync.RWMutex
	// NodePool: the sync.Pool for recycling nodes so nodes are not constantly allocated/deallocated
	pool *Pool
	// compactAtVersion: the max version the root can be before being compacted
	compactTrigger CompactionTrigger
	// appendOnly: a flag to determine whether or not to perform the compaction process. By default will be false
	appendOnly bool
}

// MariNodePool contains pre-allocated MariINodes/MariLNodes to improve performance so go garbage collection doesn't handle allocating/deallocating nodes on every op
type Pool struct {
	// maxSize: the max size for the node pool
	maxSize int64
	// size: the current number of allocated nodes in the node pool
	size int64
	// iNodePool: the node pool that contains pre-allocated internal nodes
	iPool *sync.Pool
	// lNodePool: the node pool that contains pre-allocated leaf nodes
	lPool *sync.Pool
}

// MariTx represents a transaction on the store
type Tx struct {
	// store: the mari instance to perform the transaction on
	store *Mari
	// root: the root of the trie on which to operate on
	root *unsafe.Pointer
	// isWrite: determines whether the transaction is read only or read-write
	isWrite bool
}

// MariaCompactionStrategy is the function signature for custom compaction trigger
type CompactionTrigger = func(metaData *MetaData) bool

// MariCompaction represents the compaction strategy for removing unused versions
type Compaction struct {
	// tempFile: the temporary file for compacting the db
	tempFile *os.File
	// tempData: the temporary memory mapped file as byte slice
	tempData atomic.Value
	// compactedVersion: the version to compact at
	compactedVersion uint64
}

// MariOpTransform is the function signature for transform functions, which modify results
type Transform = func(kvPair *KeyValuePair) *KeyValuePair

// MariRangeOpts contains options for iteration and range functions
type RangeOpts struct {
	// MinVersion: the min version to return when performing the scan
	MinVersion *uint64
	// Transform: the transform function
	Transform *Transform
}

// DefaultPageSize is the default page size set by the underlying OS. Usually will be 4KiB
var DefaultPageSize = os.Getpagesize()

// DefaultNodePoolSize is the max number of nodes in the node pool, and the pre-allocated node pool size
const DefaultNodePoolSize = int64(1000000)
//	MaxCompactVersion is the maximum default version to increment to before the compaction process
const MaxCompactVersion = uint64(1000000)

const (
	// Index of Mari Version in serialized metadata
	MetaVersionIdx = 0
	// Index of Root Offset in serialized metadata
	MetaRootOffsetIdx = 8
	// Index of Node Version in serialized node
	MetaEndSerializedOffset = 16
	// The current node version index in serialized node
	NodeVersionIdx = 0
	// Index of StartOffset in serialized node
	NodeStartOffsetIdx = 8
	// Index of EndOffset in serialized node
	NodeEndOffsetIdx = 16
	// Index of Bitmap in serialized node
	NodeBitmapIdx = 18
	// Index of IsLeaf in serialized node
	NodeLeafOffsetIdx = 50
	// Index of Children in serialized internal node
	NodeChildrenIdx = 58
	// Index of Key Length in serialized node
	NodeKeyLength = 18
	// Index of Key in serialized leaf node node
	NodeKeyIdx = 19
	// OffsetSize for uint64 in serialized node
	OffsetSize64 = 8
	// Bitmap size in bytes since bitmap sis uint32
	OffsetSize32 = 4
	OffsetSize16 = 2
	// Size of child pointers, where the pointers are uint64 offsets in the memory map
	NodeChildPtrSize = 8
	// Offset for the first version of root on Mari initialization
	InitRootOffset = 24
	// 1 GB MaxResize
	MaxResize = 1000000000
)

const (
	// RDONLY: maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
	RDONLY = 0
	// RDWR: maps the memory as read-write. Writes to the MMap object will update the underlying file.
	RDWR = 1 << iota
	// COPY: maps the memory as copy-on-write. Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
	COPY
	// EXEC: marks the mapped memory as executable.
	EXEC
)

const (
	// If the ANON flag is set, the mapped memory will not be backed by a file.
	ANON = 1 << iota
)

// 1 << iota // this creates powers of 2

/*
	Offsets explained:

	Meta:
		0 Version - 8 bytes
		8 RootOffset - 8 bytes
		16 EndMmapOffset - 8 bytes

	[0-7, 8-15, 16-23, 24-27, 28, 29-92, 93+]
	Node (Leaf):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 2 bytes
		18 KeyLength - 1 bytes, size of the key
		19 Key - variable length


	Node (Internal):
		0 Version - 8 bytes
		8 StartOffset - 8 bytes
		16 EndOffset - 2 bytes
		18 8 Bitmaps - 32 bytes
		50 LeafOffset - 8 bytes
		58 Children -->
			every child will then be 8 bytes, up to 256 * 8 = 2048 bytes
*/