package cache

import (
	"container/heap"
	"sync"
	"time"
	"tiny-lsm-go/pkg/block"
)

// BlockCacheKey represents a cache key for a block
type BlockCacheKey struct {
	SSTID   uint64 // SST file ID
	BlockID uint64 // Block index within the SST
}

// cacheEntry represents a cached item
type cacheEntry struct {
	key        BlockCacheKey
	block      *block.Block
	timestamps []int64 // 记录最近访问的时间戳列表

	// For heap implementation
	index int // index of the item in the heap
}

// BlockCache implements a LFU cache for blocks
type BlockCache struct {
	capacity int
	maxFreq  int // 时间戳列表的最大长度
	cache    map[BlockCacheKey]*cacheEntry
	mutex    sync.RWMutex

	// For frequency based eviction
	freqHeap *entryHeap
}

// entryHeap implements heap.Interface and holds cache entries
type entryHeap []*cacheEntry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	// We want Pop to give us the entry with minimum frequency
	// If frequencies are equal, prefer the least recently used
	if len(h[i].timestamps) == len(h[j].timestamps) {
		// 如果都为空，按索引排序
		if len(h[i].timestamps) == 0 {
			return h[i].index < h[j].index
		}
		// 优先驱逐最近访问时间更久远的
		return h[i].timestamps[len(h[i].timestamps)-1] < h[j].timestamps[len(h[j].timestamps)-1]
	}
	return len(h[i].timestamps) < len(h[j].timestamps)
}

func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *entryHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*cacheEntry)
	entry.index = n
	*h = append(*h, entry)
}

func (h *entryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil   // avoid memory leak
	entry.index = -1 // for safety
	*h = old[0 : n-1]
	return entry
}

// NewBlockCache creates a new block cache with the specified capacity
func NewBlockCache(capacity int) *BlockCache {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	bc := &BlockCache{
		capacity: capacity,
		maxFreq:  32, // 默认最大频率限制
		cache:    make(map[BlockCacheKey]*cacheEntry),
		freqHeap: &entryHeap{},
	}
	heap.Init(bc.freqHeap)
	return bc
}

// Get retrieves a block from the cache
func (bc *BlockCache) Get(sstID, blockID uint64) *block.Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if entry, found := bc.cache[key]; found {
		// 记录访问时间戳
		entry.timestamps = append(entry.timestamps, time.Now().UnixNano())

		// 如果超过最大长度，移除最旧的时间戳
		if len(entry.timestamps) > bc.maxFreq {
			entry.timestamps = entry.timestamps[1:]
		}

		heap.Fix(bc.freqHeap, entry.index)
		return entry.block
	}

	return nil
}

// Put adds a block to the cache
func (bc *BlockCache) Put(sstID, blockID uint64, block *block.Block) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}

	// If already exists, update and record timestamp
	if entry, found := bc.cache[key]; found {
		entry.block = block
		entry.timestamps = append(entry.timestamps, time.Now().UnixNano())

		// 如果超过最大长度，移除最旧的时间戳
		if len(entry.timestamps) > bc.maxFreq {
			entry.timestamps = entry.timestamps[1:]
		}

		heap.Fix(bc.freqHeap, entry.index)
		return
	}

	// Evict least frequently used if over capacity
	if len(bc.cache) >= bc.capacity {
		bc.evict()
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		block:      block,
		timestamps: []int64{time.Now().UnixNano()},
	}
	bc.cache[key] = entry

	// Add to heap
	heap.Push(bc.freqHeap, entry)
}

// Remove removes a specific block from the cache
func (bc *BlockCache) Remove(sstID, blockID uint64) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if entry, found := bc.cache[key]; found {
		// Remove from heap
		heap.Remove(bc.freqHeap, entry.index)
		delete(bc.cache, key)
	}
}

// Clear removes all entries from the cache
func (bc *BlockCache) Clear() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.cache = make(map[BlockCacheKey]*cacheEntry)
	bc.freqHeap = &entryHeap{}
	heap.Init(bc.freqHeap)
}

// Size returns the current number of cached blocks
func (bc *BlockCache) Size() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return len(bc.cache)
}

// Capacity returns the maximum capacity of the cache
func (bc *BlockCache) Capacity() int {
	return bc.capacity
}

// Stats returns cache statistics
func (bc *BlockCache) Stats() BlockCacheStats {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	return BlockCacheStats{
		Size:     len(bc.cache),
		Capacity: bc.capacity,
	}
}

// evict removes the least frequently used entry
func (bc *BlockCache) evict() {
	if bc.freqHeap.Len() == 0 {
		return
	}

	// Get the entry with minimum frequency
	entry := heap.Pop(bc.freqHeap).(*cacheEntry)

	// Remove from map
	delete(bc.cache, entry.key)
}

// BlockCacheStats represents cache statistics
type BlockCacheStats struct {
	Size     int
	Capacity int
}
