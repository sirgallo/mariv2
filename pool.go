package mariv2

import (
	"sync"
	"sync/atomic"
)


//============================================= Mari Node Pool


// NewMariNodePool
//	Creates a new node pool for recycling nodes instead of letting garbage collection handle them.
//	Should help performance when there are a large number of go routines attempting to allocate/deallocate nodes.
func newMariNodePool(maxSize int64) *MariNodePool {
	size := int64(0)
	np := &MariNodePool{ maxSize: maxSize, size: size }

	iNodePool := &sync.Pool { 
		New: func() interface {} { return np.resetINode(&MariINode{}) },
	}

	lNodePool := &sync.Pool {
		New: func() interface {} { return np.resetLNode(&MariLNode{}) },
	}

	np.iNodePool = iNodePool
	np.lNodePool = lNodePool
	np.initializePools()
	return np
}

// getINode
//	Attempt to get a pre-allocated internal node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (np *MariNodePool) getINode() *MariINode {
	node := np.iNodePool.Get().(*MariINode)
	if atomic.LoadInt64(&np.size) > 0 { atomic.AddInt64(&np.size, -1) }

	return node
}

// getLNode
//	Attempt to get a pre-allocated leaf node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (np *MariNodePool) getLNode() *MariLNode {
	node := np.lNodePool.Get().(*MariLNode)
	if atomic.LoadInt64(&np.size) > 0 { atomic.AddInt64(&np.size, -1) }

	return node
}

// initializePool
//	When Mari is opened, initialize the pool with the max size of nodes.
func (np *MariNodePool) initializePools() {
	for range make([]int, np.maxSize / 2) {
		np.iNodePool.Put(np.resetINode(&MariINode{}))
		atomic.AddInt64(&np.size, 1)
	}

	for range make([]int, np.maxSize / 2) {
		np.lNodePool.Put(np.resetLNode(&MariLNode{}))
		atomic.AddInt64(&np.size, 1)
	}
}

// putINode
//	Attempt to put an internal node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (np *MariNodePool) putINode(node *MariINode) {
	if atomic.LoadInt64(&np.size) < np.maxSize { 
		np.iNodePool.Put(np.resetINode(node))
		atomic.AddInt64(&np.size, 1)
	}
}

// putLNode
//	Attempt to put a leaf node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (np *MariNodePool) putLNode(node *MariLNode) {
	if atomic.LoadInt64(&np.size) < np.maxSize { 
		np.lNodePool.Put(np.resetLNode(node))
		atomic.AddInt64(&np.size, 1)
	}
}

// resetINode
//	When an internal node is put back in the pool, reset the values.
func (np *MariNodePool) resetINode(node *MariINode) *MariINode{
	node.version = 0
	node.startOffset = 0
	node.endOffset = 0
	node.bitmap = [8]uint32{0, 0, 0, 0, 0, 0, 0, 0}
	node.children = make([]*MariINode, 0)
	node.leaf = &MariLNode{ 
		version: 0, 
		startOffset: 0, 
		endOffset: 0,
		keyLength: 0, 
		key: nil, 
		value: nil, 
	}
	return node
}

// resetLNode
//	When a leaf node is put back in the pool, reset the values.
func (np *MariNodePool) resetLNode(node *MariLNode) *MariLNode{
	node.version = 0
	node.startOffset = 0
	node.endOffset = 0
	node.keyLength = 0
	node.key = nil
	node.value = nil

	return node
}