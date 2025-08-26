package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test that all default values are reasonable
	assert.Greater(t, cfg.GetBlockSize(), 0)
	assert.Greater(t, cfg.GetBlockCacheCapacity(), 0)
	assert.Greater(t, cfg.GetPerMemSizeLimit(), int64(0))
	assert.Greater(t, cfg.GetTotalMemSizeLimit(), int64(0))
	assert.Greater(t, cfg.GetWALBufferSize(), int64(0))
	assert.Greater(t, cfg.GetWALFileSizeLimit(), int64(0))
	assert.Greater(t, cfg.GetWALCleanInterval(), 0)

	// Test default compaction settings
	assert.True(t, cfg.Compaction.EnableAutoCompaction)
	assert.Greater(t, cfg.Compaction.TriggerRatio, 1)
	assert.Greater(t, cfg.Compaction.MaxThreads, 0)

	// Test validation passes
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigValidation(t *testing.T) {
	// Test valid config
	cfg := DefaultConfig()
	err := cfg.Validate()
	assert.NoError(t, err)

	// Test invalid block size
	cfg.LSM.Core.BlockSize = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block size")

	// Reset and test invalid cache capacity
	cfg = DefaultConfig()
	cfg.LSM.Cache.BlockCacheCapacity = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cache capacity")

	// Reset and test invalid memtable size
	cfg = DefaultConfig()
	cfg.LSM.Core.PerMemSizeLimit = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "per-memory size")

	// Reset and test invalid total memory size
	cfg = DefaultConfig()
	cfg.LSM.Core.TotalMemSizeLimit = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "total memory size")
}

func TestConfigWALValidation(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid WAL buffer size
	cfg.WAL.BufferSize = 0
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL buffer size")

	// Reset and test invalid WAL file size
	cfg = DefaultConfig()
	cfg.WAL.FileSizeLimit = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL file size")

	// Reset and test invalid clean interval
	cfg = DefaultConfig()
	cfg.WAL.CleanInterval = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "WAL clean interval")
}

func TestConfigCompactionValidation(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid trigger ratio
	cfg.Compaction.TriggerRatio = 1
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "trigger ratio")

	// Reset and test invalid max threads
	cfg = DefaultConfig()
	cfg.Compaction.MaxThreads = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max threads")

	// Reset and test valid compaction settings
	cfg = DefaultConfig()
	assert.True(t, cfg.Compaction.EnableAutoCompaction)
	assert.Greater(t, cfg.Compaction.TriggerRatio, 1)
	assert.Greater(t, cfg.Compaction.MaxThreads, 0)
}

func TestConfigBloomFilterValidation(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid bloom filter expected size
	cfg.BloomFilter.ExpectedSize = 0
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bloom filter expected size")

	// Reset and test invalid error rate
	cfg = DefaultConfig()
	cfg.BloomFilter.ExpectedErrorRate = 0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bloom filter expected error rate")

	cfg.BloomFilter.ExpectedErrorRate = 1.0
	err = cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bloom filter expected error rate")
}

func TestConfigFromTOMLString(t *testing.T) {
	tomlContent := `
[lsm.core]
LSM_BLOCK_SIZE = 16384
LSM_PER_MEM_SIZE_LIMIT = 8388608
LSM_TOL_MEM_SIZE_LIMIT = 67108864
LSM_SST_LEVEL_RATIO = 8

[lsm.cache]
LSM_BLOCK_CACHE_CAPACITY = 2048
LSM_BLOCK_CACHE_K = 16

[wal]
WAL_BUFFER_SIZE = 2097152
WAL_FILE_SIZE_LIMIT = 134217728
WAL_CLEAN_INTERVAL = 600

[compaction]
ENABLE_AUTO_COMPACTION = false
COMPACTION_TRIGGER_RATIO = 4
MAX_COMPACTION_THREADS = 4

[bloom_filter]
BLOOM_FILTER_EXPECTED_SIZE = 131072
BLOOM_FILTER_EXPECTED_ERROR_RATE = 0.05
`

	cfg, err := LoadFromString(tomlContent)
	require.NoError(t, err)

	// Verify loaded values
	assert.Equal(t, 16384, cfg.GetBlockSize())
	assert.Equal(t, 2048, cfg.GetBlockCacheCapacity())
	assert.Equal(t, int64(8388608), cfg.GetPerMemSizeLimit())
	assert.Equal(t, int64(67108864), cfg.GetTotalMemSizeLimit())
	assert.Equal(t, 8, cfg.GetSSTLevelRatio())
	assert.Equal(t, int64(2097152), cfg.GetWALBufferSize())
	assert.Equal(t, int64(134217728), cfg.GetWALFileSizeLimit())
	assert.Equal(t, 600, cfg.GetWALCleanInterval())

	// Verify compaction settings
	assert.False(t, cfg.Compaction.EnableAutoCompaction)
	assert.Equal(t, 4, cfg.Compaction.TriggerRatio)
	assert.Equal(t, 4, cfg.Compaction.MaxThreads)

	// Verify bloom filter settings
	assert.Equal(t, 131072, cfg.BloomFilter.ExpectedSize)
	assert.Equal(t, 0.05, cfg.BloomFilter.ExpectedErrorRate)

	// Verify validation passes
	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigFromTOMLFile(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.toml")

	// Create test config file with TOML format
	configContent := `
[lsm.core]
LSM_BLOCK_SIZE = 8192
LSM_PER_MEM_SIZE_LIMIT = 4194304
LSM_TOL_MEM_SIZE_LIMIT = 33554432

[lsm.cache]
LSM_BLOCK_CACHE_CAPACITY = 512

[wal]
WAL_BUFFER_SIZE = 1048576
WAL_FILE_SIZE_LIMIT = 33554432
WAL_CLEAN_INTERVAL = 300

[compaction]
ENABLE_AUTO_COMPACTION = true
COMPACTION_TRIGGER_RATIO = 3
MAX_COMPACTION_THREADS = 2
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Load config from file
	cfg, err := LoadFromFile(configFile)
	require.NoError(t, err)

	// Verify loaded values
	assert.Equal(t, 8192, cfg.GetBlockSize())
	assert.Equal(t, 512, cfg.GetBlockCacheCapacity())
	assert.Equal(t, int64(4194304), cfg.GetPerMemSizeLimit())
	assert.Equal(t, int64(33554432), cfg.GetTotalMemSizeLimit())

	// Verify validation passes
	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigFromInvalidFile(t *testing.T) {
	// Test non-existent file
	_, err := LoadFromFile("nonexistent.toml")
	assert.Error(t, err)

	// Test invalid TOML
	tempDir := t.TempDir()
	invalidFile := filepath.Join(tempDir, "invalid.toml")
	err = os.WriteFile(invalidFile, []byte("invalid toml content [[["), 0644)
	require.NoError(t, err)

	_, err = LoadFromFile(invalidFile)
	assert.Error(t, err)
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.LSM.Core.BlockSize = 16384
	original.Compaction.EnableAutoCompaction = false

	// Create clone
	clone := original.Clone()

	// Verify values match
	assert.Equal(t, original.GetBlockSize(), clone.GetBlockSize())
	assert.Equal(t, original.Compaction.EnableAutoCompaction, clone.Compaction.EnableAutoCompaction)

	// Verify it's a deep copy by modifying clone
	clone.LSM.Core.BlockSize = 32768
	assert.NotEqual(t, original.GetBlockSize(), clone.GetBlockSize())
}

func TestConfigGetters(t *testing.T) {
	cfg := DefaultConfig()

	// Test all getter methods return expected types and values
	assert.IsType(t, 0, cfg.GetBlockSize())
	assert.IsType(t, 0, cfg.GetBlockCacheCapacity())
	assert.IsType(t, 0, cfg.GetBlockCacheK())
	assert.IsType(t, int64(0), cfg.GetPerMemSizeLimit())
	assert.IsType(t, int64(0), cfg.GetTotalMemSizeLimit())
	assert.IsType(t, 0, cfg.GetSSTLevelRatio())
	assert.IsType(t, int64(0), cfg.GetWALBufferSize())
	assert.IsType(t, int64(0), cfg.GetWALFileSizeLimit())
	assert.IsType(t, 0, cfg.GetWALCleanInterval())

	// Verify all values are positive
	assert.Positive(t, cfg.GetBlockSize())
	assert.Positive(t, cfg.GetBlockCacheCapacity())
	assert.Positive(t, cfg.GetBlockCacheK())
	assert.Positive(t, cfg.GetPerMemSizeLimit())
	assert.Positive(t, cfg.GetTotalMemSizeLimit())
	assert.Positive(t, cfg.GetSSTLevelRatio())
	assert.Positive(t, cfg.GetWALBufferSize())
	assert.Positive(t, cfg.GetWALFileSizeLimit())
	assert.Positive(t, cfg.GetWALCleanInterval())
}

func TestConfigSizeRelationships(t *testing.T) {
	cfg := DefaultConfig()

	// Test that size configurations make sense relative to each other
	assert.Less(t, cfg.GetBlockSize(), int(cfg.GetPerMemSizeLimit()))
	assert.LessOrEqual(t, cfg.GetPerMemSizeLimit(), cfg.GetTotalMemSizeLimit())
	assert.Less(t, cfg.GetWALBufferSize(), cfg.GetWALFileSizeLimit())
}

func TestConfigSSRLevelRatio(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid SST level ratio
	cfg.LSM.Core.SSTLevelRatio = 1
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SST level ratio")

	// Test valid SST level ratio
	cfg.LSM.Core.SSTLevelRatio = 4
	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigCacheK(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid cache K value
	cfg.LSM.Cache.BlockCacheK = 0
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cache K")

	// Test valid cache K value
	cfg.LSM.Cache.BlockCacheK = 8
	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestConfigMemorySizeRelationship(t *testing.T) {
	cfg := DefaultConfig()

	// Test invalid memory size relationship
	cfg.LSM.Core.PerMemSizeLimit = cfg.LSM.Core.TotalMemSizeLimit + 1
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "per-memory size limit cannot exceed total")

	// Test valid relationship
	cfg.LSM.Core.PerMemSizeLimit = cfg.LSM.Core.TotalMemSizeLimit / 2
	err = cfg.Validate()
	assert.NoError(t, err)
}

func TestGlobalConfig(t *testing.T) {
	// Test getting global config
	globalCfg := GetGlobalConfig()
	assert.NotNil(t, globalCfg)

	// Test setting global config
	customCfg := DefaultConfig()
	customCfg.LSM.Core.BlockSize = 16384
	SetGlobalConfig(customCfg)

	retrievedCfg := GetGlobalConfig()
	assert.Equal(t, 16384, retrievedCfg.GetBlockSize())
}

func TestInitGlobalConfigFromFile(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "global_config.toml")

	configContent := `
[lsm.core]
LSM_BLOCK_SIZE = 65536
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Initialize global config from file
	err = InitGlobalConfig(configFile)
	require.NoError(t, err)

	// Verify global config was updated
	globalCfg := GetGlobalConfig()
	assert.Equal(t, 65536, globalCfg.GetBlockSize())
}

func BenchmarkConfigValidation(b *testing.B) {
	cfg := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := cfg.Validate()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfigClone(b *testing.B) {
	cfg := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.Clone()
	}
}

func BenchmarkConfigLoadFromString(b *testing.B) {
	tomlContent := `
[lsm.core]
LSM_BLOCK_SIZE = 32768
LSM_PER_MEM_SIZE_LIMIT = 16777216

[lsm.cache]
LSM_BLOCK_CACHE_CAPACITY = 1024
`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadFromString(tomlContent)
		if err != nil {
			b.Fatal(err)
		}
	}
}
