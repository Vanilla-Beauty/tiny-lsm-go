package sst

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"tiny-lsm-go/pkg/cache"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTBuilderBasic(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "test.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	// Add some test data
	err := builder.Add("key1", "value1", 1)
	require.NoError(t, err)

	err = builder.Add("key2", "value2", 2)
	require.NoError(t, err)

	err = builder.Add("key3", "value3", 3)
	require.NoError(t, err)

	// Build SST
	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	require.NotNil(t, sst)
	defer sst.Close()

	// Verify basic properties
	assert.Equal(t, uint64(1), sst.GetSSTID())
	assert.Equal(t, "key1", sst.GetFirstKey())
	assert.Equal(t, "key3", sst.GetLastKey())
	assert.Greater(t, sst.Size(), int64(0))

	// Test file existence
	_, err = os.Stat(sstPath)
	assert.NoError(t, err, "SST file should exist")
}

func TestSSTBuilderEmpty(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "empty.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	// Try to build without adding any data
	_, err := builder.Build(1, sstPath, blockCache)
	assert.Error(t, err, "Should error when building empty SST")
}

func TestSSTBuilderLargeData(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "large.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Use smaller block size to force multiple blocks
	builder := NewSSTBuilder(256, true)

	numEntries := 100
	expectedData := make(map[string]string)

	// Add enough data to create multiple blocks
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d_%s", i, "large_data_to_fill_block")
		expectedData[key] = value

		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	// Build SST
	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Verify multiple blocks were created
	assert.Greater(t, sst.GetNumBlocks(), 1, "Should have multiple blocks")

	// Verify first and last keys
	assert.Equal(t, "key_0000", sst.GetFirstKey())
	assert.Equal(t, "key_0099", sst.GetLastKey())
}

func TestSSTBuilderWithDeletes(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "deletes.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	// Add normal entries and delete entries
	builder.Add("key1", "value1", 1)
	builder.Add("key2", "", 2) // deleted
	builder.Add("key3", "value3", 3)
	builder.Add("key4", "", 4) // deleted

	// Build SST
	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Should include deleted entries
	assert.Equal(t, "key1", sst.GetFirstKey())
	assert.Equal(t, "key4", sst.GetLastKey())
}

func TestSSTOpenAndRead(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "test.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST with test data - use sorted keys to ensure predictable order
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "dark_red",
		"date":   "brown",
	}

	// Sort keys to ensure consistent order
	sortedKeys := make([]string, 0, len(testData))
	for key := range testData {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	{
		builder := NewSSTBuilder(cfg.GetBlockSize(), true)
		txnID := uint64(1)
		for _, key := range sortedKeys {
			value := testData[key]
			err := builder.Add(key, value, txnID)
			require.NoError(t, err)
			txnID++
		}

		sst, err := builder.Build(1, sstPath, blockCache)
		require.NoError(t, err)
		sst.Close()
	}

	// Reopen SST
	fileManager := utils.NewFileManager(tempDir)
	sst, err := OpenSST(1, sstPath, blockCache, fileManager)
	require.NoError(t, err)
	defer sst.Close()

	// Verify metadata
	assert.Equal(t, uint64(1), sst.GetSSTID())
	assert.Equal(t, "apple", sst.GetFirstKey())
	assert.Equal(t, "date", sst.GetLastKey())

	// Test reading data by finding appropriate blocks
	for key, expectedValue := range testData {
		blockIdx := sst.FindBlockIdx(key)
		require.NotEqual(t, -1, blockIdx, "Should find block for key %s", key)

		block, err := sst.ReadBlock(blockIdx)
		require.NoError(t, err)

		value, found := block.GetValue(key, 0)
		assert.True(t, found, "Key %s should be found", key)
		assert.Equal(t, expectedValue, value, "Value mismatch for key %s", key)
	}
}

func TestSSTFindBlock(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "find_block.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST with data that will span multiple blocks
	builder := NewSSTBuilder(128, true) // Small block size

	keys := make([]string, 0)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key_%02d_%s", i, "padding_to_make_large_entries")
		value := fmt.Sprintf("value_%02d", i)
		keys = append(keys, key)

		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Test finding blocks for various keys
	assert.Greater(t, sst.GetNumBlocks(), 1, "Should have multiple blocks")

	// Test finding existing keys
	for _, key := range keys {
		blockIdx := sst.FindBlockIdx(key)
		assert.NotEqual(t, -1, blockIdx, "Should find block for existing key %s", key)
		assert.Less(t, blockIdx, sst.GetNumBlocks(), "Block index should be valid")
	}

	// Test finding non-existing keys
	nonExistentKey := "zzz_not_exist"
	blockIdx := sst.FindBlockIdx(nonExistentKey)
	assert.Equal(t, -1, blockIdx, "Should not find block for non-existent key")

	// Test finding key before first key
	beforeFirstKey := "aaa"
	blockIdx = sst.FindBlockIdx(beforeFirstKey)
	assert.Equal(t, 0, blockIdx, "Key before first should map to first block")
}

func TestSSTBlockMetadata(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "metadata.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(512, true)

	// Add data that will create multiple blocks
	numEntries := 20
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%03d_large_entry_to_fill_blocks", i)
		value := fmt.Sprintf("value_%03d", i)

		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Verify we have multiple blocks
	assert.Greater(t, sst.GetNumBlocks(), 1)

	// Verify block metadata
	for i := 0; i < sst.GetNumBlocks(); i++ {
		block, err := sst.ReadBlock(i)
		require.NoError(t, err)
		assert.Greater(t, block.GetNumEntries(), 0, "Block %d should have entries", i)
	}
}

func TestSSTBloomFilter(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "bloom.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST with bloom filter
	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	testKeys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range testKeys {
		value := fmt.Sprintf("value_%s", key)
		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Test bloom filter (if implemented in SST)
	// Note: This assumes SST has MayContain method
	for _, key := range testKeys {
		// Key should possibly be in bloom filter
		blockIdx := sst.FindBlockIdx(key)
		assert.NotEqual(t, -1, blockIdx, "Key %s should be found", key)
	}

	// Test non-existent keys
	nonExistentKeys := []string{"grape", "kiwi", "mango"}
	for _, key := range nonExistentKeys {
		blockIdx := sst.FindBlockIdx(key)
		assert.Equal(t, -1, blockIdx, "Non-existent key %s should not be found", key)
	}
}

func TestSSTConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "concurrent.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST
	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Test concurrent reads
	const numGoroutines = 10
	const readsPerGoroutine = 20

	results := make(chan bool, numGoroutines*readsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for r := 0; r < readsPerGoroutine; r++ {
				keyIdx := (goroutineID*readsPerGoroutine + r) % numEntries
				key := fmt.Sprintf("key_%04d", keyIdx)
				expectedValue := fmt.Sprintf("value_%04d", keyIdx)

				blockIdx := sst.FindBlockIdx(key)
				if blockIdx == -1 {
					results <- false
					continue
				}

				block, err := sst.ReadBlock(blockIdx)
				if err != nil {
					results <- false
					continue
				}

				value, found := block.GetValue(key, 0)
				results <- found && value == expectedValue
			}
		}(g)
	}

	// Collect results
	successCount := 0
	for i := 0; i < numGoroutines*readsPerGoroutine; i++ {
		if <-results {
			successCount++
		}
	}

	assert.Equal(t, numGoroutines*readsPerGoroutine, successCount, "All concurrent reads should succeed")
}

func TestSSTMVCC(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "mvcc.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	// Add multiple versions of the same key
	builder.Add("key1", "value1_v1", 1)
	builder.Add("key1", "value1_v2", 2)
	builder.Add("key1", "value1_v3", 3)
	builder.Add("key2", "value2_v1", 1)
	builder.Add("key2", "value2_v2", 4)

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Test different transaction views
	blockIdx := sst.FindBlockIdx("key1")
	require.NotEqual(t, -1, blockIdx)

	block, err := sst.ReadBlock(blockIdx)
	require.NoError(t, err)

	// txnID=0 should see latest version
	value, found := block.GetValue("key1", 0)
	assert.True(t, found)
	assert.Equal(t, "value1_v3", value)

	// txnID=2 should see version 2
	value, found = block.GetValue("key1", 2)
	assert.True(t, found)
	assert.Equal(t, "value1_v2", value)

	// txnID=1 should see version 1
	value, found = block.GetValue("key1", 1)
	assert.True(t, found)
	assert.Equal(t, "value1_v1", value)
}

func TestSSTSize(t *testing.T) {
	tempDir := t.TempDir()
	sstPath := filepath.Join(tempDir, "size.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	builder := NewSSTBuilder(cfg.GetBlockSize(), true)

	// Add some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		err := builder.Add(key, value, uint64(i+1))
		require.NoError(t, err)
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	require.NoError(t, err)
	defer sst.Close()

	// Verify size is reasonable
	size := sst.Size()
	assert.Greater(t, size, int64(0))

	// Size should match file size on disk
	fileInfo, err := os.Stat(sstPath)
	require.NoError(t, err)
	assert.Equal(t, size, fileInfo.Size())
}

func TestSSTErrorHandling(t *testing.T) {
	tempDir := t.TempDir()

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())
	fileManager := utils.NewFileManager(tempDir)

	// Test opening non-existent file
	nonExistentPath := filepath.Join(tempDir, "nonexistent.sst")
	_, err := OpenSST(1, nonExistentPath, blockCache, fileManager)
	assert.Error(t, err, "Should error opening non-existent file")

	// Test invalid SST file
	invalidPath := filepath.Join(tempDir, "invalid.sst")
	err = os.WriteFile(invalidPath, []byte("invalid data"), 0644)
	require.NoError(t, err)

	_, err = OpenSST(1, invalidPath, blockCache, fileManager)
	assert.Error(t, err, "Should error opening invalid SST file")
}

func BenchmarkSSTBuilder(b *testing.B) {
	tempDir := b.TempDir()
	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sstPath := filepath.Join(tempDir, fmt.Sprintf("bench_%d.sst", i))
		builder := NewSSTBuilder(cfg.GetBlockSize(), true)

		// Add entries
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key_%04d", j)
			value := fmt.Sprintf("value_%04d", j)
			builder.Add(key, value, uint64(j+1))
		}

		sst, err := builder.Build(uint64(i+1), sstPath, blockCache)
		if err != nil {
			b.Fatal(err)
		}
		sst.Close()
	}
}

func BenchmarkSSTRead(b *testing.B) {
	tempDir := b.TempDir()
	sstPath := filepath.Join(tempDir, "bench_read.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST with test data
	builder := NewSSTBuilder(cfg.GetBlockSize(), true)
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		builder.Add(key, value, uint64(i+1))
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	if err != nil {
		b.Fatal(err)
	}
	defer sst.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % numEntries
		key := fmt.Sprintf("key_%04d", keyIdx)

		blockIdx := sst.FindBlockIdx(key)
		if blockIdx != -1 {
			block, err := sst.ReadBlock(blockIdx)
			if err != nil {
				b.Fatal(err)
			}
			block.GetValue(key, 0)
		}
	}
}

func BenchmarkSSTFindBlock(b *testing.B) {
	tempDir := b.TempDir()
	sstPath := filepath.Join(tempDir, "bench_find.sst")

	cfg := config.DefaultConfig()
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Create SST with many entries
	builder := NewSSTBuilder(1024, true)
	numEntries := 5000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := fmt.Sprintf("value_%06d", i)
		builder.Add(key, value, uint64(i+1))
	}

	sst, err := builder.Build(1, sstPath, blockCache)
	if err != nil {
		b.Fatal(err)
	}
	defer sst.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyIdx := i % numEntries
		key := fmt.Sprintf("key_%06d", keyIdx)
		sst.FindBlockIdx(key)
	}
}
