package mariv2

import (
	"sync"
	"sync/atomic"
)


//============================================= Mari Node Pool


// NewMariNodePool
//	Creates a new node pool for recycling nodes instead of letting garbage collection handle them.
//	Should help performance when there are a large number of go routines attempting to allocate/deallocate nodes.
func newPool(maxSize int64) *Pool {
	size := int64(0)
	np := &Pool{ maxSize: maxSize, size: size }

	iPool := &sync.Pool { 
		New: func() interface {} { return np.resetINode(&INode{}) },
	}

	lPool := &sync.Pool {
		New: func() interface {} { return np.resetLNode(&LNode{}) },
	}

	np.iPool = iPool
	np.lPool = lPool
	np.initializePools()
	return np
}

// getINode
//	Attempt to get a pre-allocated internal node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (p *Pool) getINode() *INode {
	node := p.iPool.Get().(*INode)
	if atomic.LoadInt64(&p.size) > 0 { atomic.AddInt64(&p.size, -1) }

	return node
}

// getLNode
//	Attempt to get a pre-allocated leaf node from the node pool and decrement the total allocated nodes.
//	If the pool is empty, a new node is allocated
func (p *Pool) getLNode() *LNode {
	node := p.lPool.Get().(*LNode)
	if atomic.LoadInt64(&p.size) > 0 { atomic.AddInt64(&p.size, -1) }

	return node
}

// initializePool
//	When Mari is opened, initialize the pool with the max size of nodes.
func (p *Pool) initializePools() {
	for range make([]int, p.maxSize / 2) {
		p.iPool.Put(p.resetINode(&INode{}))
		atomic.AddInt64(&p.size, 1)
	}

	for range make([]int, p.maxSize / 2) {
		p.lPool.Put(p.resetLNode(&LNode{}))
		atomic.AddInt64(&p.size, 1)
	}
}

// putINode
//	Attempt to put an internal node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (p *Pool) putINode(node *INode) {
	if atomic.LoadInt64(&p.size) < p.maxSize { 
		p.iPool.Put(p.resetINode(node))
		atomic.AddInt64(&p.size, 1)
	}
}

// putLNode
//	Attempt to put a leaf node back into the pool once a path has been copied + serialized.
//	If the pool is at max capacity, drop the node and let the garbage collector take care of it.
func (p *Pool) putLNode(node *LNode) {
	if atomic.LoadInt64(&p.size) < p.maxSize { 
		p.lPool.Put(p.resetLNode(node))
		atomic.AddInt64(&p.size, 1)
	}
}

// resetINode
//	When an internal node is put back in the pool, reset the values.
func (p *Pool) resetINode(node *INode) *INode {
	node.version = 0
	node.startOffset = 0
	node.endOffset = 0
	node.bitmap = [8]uint32{0, 0, 0, 0, 0, 0, 0, 0}
	node.children = make([]*INode, 0)
	node.leaf = &LNode{ 
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
func (p *Pool) resetLNode(node *LNode) *LNode {
	node.version = 0
	node.startOffset = 0
	node.endOffset = 0
	node.keyLength = 0
	node.key = nil
	node.value = nil

	return node
}