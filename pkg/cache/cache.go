package cache

import (
	"container/list"
	"sync"
	"tiny-lsm-go/pkg/block"
)

// BlockCacheKey represents a cache key for a block
type BlockCacheKey struct {
	SSTID   uint64 // SST file ID
	BlockID uint64 // Block index within the SST
}

// BlockCache implements a LRU cache for blocks
type BlockCache struct {
	capacity int
	cache    map[BlockCacheKey]*list.Element
	lruList  *list.List
	mutex    sync.RWMutex
}

// cacheEntry represents a cached item
type cacheEntry struct {
	key   BlockCacheKey
	block *block.Block
}

// NewBlockCache creates a new block cache with the specified capacity
func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		capacity: capacity,
		cache:    make(map[BlockCacheKey]*list.Element),
		lruList:  list.New(),
	}
}

// Get retrieves a block from the cache
func (bc *BlockCache) Get(sstID, blockID uint64) *block.Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if elem, found := bc.cache[key]; found {
		// Move to front (most recently used)
		bc.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).block
	}
	
	return nil
}

// Put adds a block to the cache
func (bc *BlockCache) Put(sstID, blockID uint64, block *block.Block) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	
	// If already exists, update and move to front
	if elem, found := bc.cache[key]; found {
		bc.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).block = block
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, block: block}
	elem := bc.lruList.PushFront(entry)
	bc.cache[key] = elem

	// Evict least recently used if over capacity
	if bc.lruList.Len() > bc.capacity {
		oldest := bc.lruList.Back()
		if oldest != nil {
			bc.lruList.Remove(oldest)
			oldEntry := oldest.Value.(*cacheEntry)
			delete(bc.cache, oldEntry.key)
		}
	}
}

// Remove removes a specific block from the cache
func (bc *BlockCache) Remove(sstID, blockID uint64) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	key := BlockCacheKey{SSTID: sstID, BlockID: blockID}
	if elem, found := bc.cache[key]; found {
		bc.lruList.Remove(elem)
		delete(bc.cache, key)
	}
}

// Clear removes all entries from the cache
func (bc *BlockCache) Clear() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	bc.cache = make(map[BlockCacheKey]*list.Element)
	bc.lruList = list.New()
}

// Size returns the current number of cached blocks
func (bc *BlockCache) Size() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return bc.lruList.Len()
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
		Size:     bc.lruList.Len(),
		Capacity: bc.capacity,
	}
}

// BlockCacheStats represents cache statistics
type BlockCacheStats struct {
	Size     int
	Capacity int
}
