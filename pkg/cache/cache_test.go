package cache

import (
	"sync"
	"testing"

	"tiny-lsm-go/pkg/block"

	"github.com/stretchr/testify/assert"
)

func TestBlockCacheBasicOperations(t *testing.T) {
	cache := NewBlockCache(3) // capacity of 3

	// Create test blocks
	block1 := &block.Block{}
	block2 := &block.Block{}
	block3 := &block.Block{}

	// Test Put and Get
	cache.Put(1, 1, block1)
	cache.Put(1, 2, block2)
	cache.Put(2, 1, block3)

	// Verify retrieval
	retrieved := cache.Get(1, 1)
	assert.Equal(t, block1, retrieved)

	retrieved = cache.Get(1, 2)
	assert.Equal(t, block2, retrieved)

	retrieved = cache.Get(2, 1)
	assert.Equal(t, block3, retrieved)

	// Test non-existent key
	retrieved = cache.Get(999, 999)
	assert.Nil(t, retrieved)
}

func TestBlockCacheEviction(t *testing.T) {
	cache := NewBlockCache(2) // small capacity to test eviction

	block1 := &block.Block{}
	block2 := &block.Block{}
	block3 := &block.Block{}

	// Fill cache to capacity
	cache.Put(1, 1, block1)
	cache.Put(1, 2, block2)

	// Verify both blocks are cached
	assert.Equal(t, block1, cache.Get(1, 1))
	assert.Equal(t, block2, cache.Get(1, 2))

	// Add third block - should evict the least recently used
	cache.Put(1, 3, block3)

	// block1 should be evicted (least recently used)
	assert.Nil(t, cache.Get(1, 1))
	assert.Equal(t, block2, cache.Get(1, 2))
	assert.Equal(t, block3, cache.Get(1, 3))
}

func TestBlockCacheLRUOrdering(t *testing.T) {
	cache := NewBlockCache(3)

	block1 := &block.Block{}
	block2 := &block.Block{}
	block3 := &block.Block{}
	block4 := &block.Block{}

	// Fill cache
	cache.Put(1, 1, block1)
	cache.Put(1, 2, block2)
	cache.Put(1, 3, block3)

	// Access block1 and block2 to make them more recently used
	cache.Get(1, 1)
	cache.Get(1, 2)

	// Add block4 - should evict block3 (least recently used)
	cache.Put(1, 4, block4)

	assert.Equal(t, block1, cache.Get(1, 1))
	assert.Equal(t, block2, cache.Get(1, 2))
	assert.Nil(t, cache.Get(1, 3)) // evicted
	assert.Equal(t, block4, cache.Get(1, 4))
}

func TestBlockCacheStats(t *testing.T) {
	cache := NewBlockCache(2)
	block1 := &block.Block{}

	// Initially empty
	stats := cache.Stats()
	assert.Equal(t, 0, stats.Size)
	assert.Equal(t, 2, stats.Capacity)

	// Add one block
	cache.Put(1, 1, block1)
	stats = cache.Stats()
	assert.Equal(t, 1, stats.Size)
	assert.Equal(t, 2, stats.Capacity)

	// Verify get operation works
	result := cache.Get(1, 1)
	assert.Equal(t, block1, result)

	// Stats should remain consistent
	stats = cache.Stats()
	assert.Equal(t, 1, stats.Size)
	assert.Equal(t, 2, stats.Capacity)
}

func TestBlockCacheSize(t *testing.T) {
	cache := NewBlockCache(3)

	assert.Equal(t, 0, cache.Size())

	block1 := &block.Block{}
	block2 := &block.Block{}

	cache.Put(1, 1, block1)
	assert.Equal(t, 1, cache.Size())

	cache.Put(1, 2, block2)
	assert.Equal(t, 2, cache.Size())

	// Adding same key shouldn't change size
	cache.Put(1, 1, block1)
	assert.Equal(t, 2, cache.Size())
}

func TestBlockCacheClear(t *testing.T) {
	cache := NewBlockCache(3)

	block1 := &block.Block{}
	block2 := &block.Block{}

	cache.Put(1, 1, block1)
	cache.Put(1, 2, block2)

	assert.Equal(t, 2, cache.Size())

	cache.Clear()

	assert.Equal(t, 0, cache.Size())
	assert.Nil(t, cache.Get(1, 1))
	assert.Nil(t, cache.Get(1, 2))

	// Stats should reflect empty cache
	stats := cache.Stats()
	assert.Equal(t, 0, stats.Size)
	assert.Equal(t, 3, stats.Capacity)
}

func TestBlockCacheUpdateExisting(t *testing.T) {
	cache := NewBlockCache(2)

	block1 := &block.Block{}
	block2 := &block.Block{}

	// Put initial block
	cache.Put(1, 1, block1)
	assert.Equal(t, block1, cache.Get(1, 1))

	// Update with new block
	cache.Put(1, 1, block2)
	assert.Equal(t, block2, cache.Get(1, 1))

	// Size should remain the same
	assert.Equal(t, 1, cache.Size())
}

func TestBlockCacheConcurrency(t *testing.T) {
	cache := NewBlockCache(100)
	numRoutines := 10
	numOperations := 100

	var wg sync.WaitGroup

	// Concurrent puts
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				block := &block.Block{}
				cache.Put(uint64(routineID), uint64(j), block)
			}
		}(i)
	}

	// Concurrent gets
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				cache.Get(uint64(routineID), uint64(j))
			}
		}(i)
	}

	wg.Wait()

	// Verify cache is still functional
	testBlock := &block.Block{}
	cache.Put(999, 999, testBlock)
	assert.Equal(t, testBlock, cache.Get(999, 999))
}

func TestBlockCacheZeroCapacity(t *testing.T) {
	cache := NewBlockCache(0)
	block1 := &block.Block{}

	// Should not crash with zero capacity
	cache.Put(1, 1, block1)
	assert.Nil(t, cache.Get(1, 1))
	assert.Equal(t, 0, cache.Size())
}

func TestBlockCacheCapacityOne(t *testing.T) {
	cache := NewBlockCache(1)

	block1 := &block.Block{}
	block2 := &block.Block{}

	// Add first block
	cache.Put(1, 1, block1)
	assert.Equal(t, block1, cache.Get(1, 1))
	assert.Equal(t, 1, cache.Size())

	// Add second block - should evict first
	cache.Put(1, 2, block2)
	assert.Nil(t, cache.Get(1, 1))
	assert.Equal(t, block2, cache.Get(1, 2))
	assert.Equal(t, 1, cache.Size())
}

func TestBlockCacheMultipleSSTs(t *testing.T) {
	cache := NewBlockCache(20) // Increased capacity to accommodate all blocks

	// Add blocks from different SSTs
	for sstID := uint64(1); sstID <= 5; sstID++ {
		for blockID := uint64(1); blockID <= 3; blockID++ {
			block := &block.Block{}
			cache.Put(sstID, blockID, block)
		}
	}

	assert.Equal(t, 15, cache.Size())

	// Verify all blocks are retrievable
	for sstID := uint64(1); sstID <= 5; sstID++ {
		for blockID := uint64(1); blockID <= 3; blockID++ {
			assert.NotNil(t, cache.Get(sstID, blockID))
		}
	}
}

func BenchmarkBlockCachePut(b *testing.B) {
	cache := NewBlockCache(1000)
	blocks := make([]*block.Block, b.N)
	for i := 0; i < b.N; i++ {
		blocks[i] = &block.Block{}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(1, uint64(i), blocks[i])
	}
}

func BenchmarkBlockCacheGet(b *testing.B) {
	cache := NewBlockCache(1000)

	// Pre-populate cache
	numBlocks := 500
	for i := 0; i < numBlocks; i++ {
		block := &block.Block{}
		cache.Put(1, uint64(i), block)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(1, uint64(i%numBlocks))
	}
}

func BenchmarkBlockCacheMixed(b *testing.B) {
	cache := NewBlockCache(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			block := &block.Block{}
			cache.Put(1, uint64(i%50), block)
		} else {
			cache.Get(1, uint64(i%50))
		}
	}
}
