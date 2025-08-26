package config

import (
	"fmt"
	"sync"

	"github.com/BurntSushi/toml"
)

// Config represents the configuration for the LSM-tree storage engine
type Config struct {
	// LSM Core Configuration
	LSM struct {
		Core struct {
			// Total memory table size limit (bytes)
			TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
			// Per-memory table size limit (bytes)
			PerMemSizeLimit int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
			// Block size for SST files (bytes)
			BlockSize int `toml:"LSM_BLOCK_SIZE"`
			// SST level size ratio
			SSTLevelRatio int `toml:"LSM_SST_LEVEL_RATIO"`
		} `toml:"core"`

		Cache struct {
			// Block cache capacity (number of blocks)
			BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
			// LRU-K K value for cache
			BlockCacheK int `toml:"LSM_BLOCK_CACHE_K"`
		} `toml:"cache"`
	} `toml:"lsm"`

	// Bloom Filter Configuration
	BloomFilter struct {
		// Expected number of elements
		ExpectedSize int `toml:"BLOOM_FILTER_EXPECTED_SIZE"`
		// Expected false positive rate
		ExpectedErrorRate float64 `toml:"BLOOM_FILTER_EXPECTED_ERROR_RATE"`
	} `toml:"bloom_filter"`

	// WAL Configuration
	WAL struct {
		// WAL buffer size (bytes)
		BufferSize int64 `toml:"WAL_BUFFER_SIZE"`
		// WAL file size limit (bytes)
		FileSizeLimit int64 `toml:"WAL_FILE_SIZE_LIMIT"`
		// WAL clean interval (seconds)
		CleanInterval int `toml:"WAL_CLEAN_INTERVAL"`
	} `toml:"wal"`

	// Compaction Configuration
	Compaction struct {
		// Enable automatic compaction
		EnableAutoCompaction bool `toml:"ENABLE_AUTO_COMPACTION"`
		// Compaction trigger ratio
		TriggerRatio int `toml:"COMPACTION_TRIGGER_RATIO"`
		// Maximum compaction threads
		MaxThreads int `toml:"MAX_COMPACTION_THREADS"`
	} `toml:"compaction"`

	// Redis Configuration
	Redis struct {
		// TTL expire header prefix
		ExpireHeader string `toml:"REDIS_EXPIRE_HEADER"`
		// Hash value prefix
		HashValuePrefix string `toml:"REDIS_HASH_VALUE_PREFIX"`
		// Hash field prefix
		FieldPrefix string `toml:"REDIS_FIELD_PREFIX"`
		// Field separator character
		FieldSeparator string `toml:"REDIS_FIELD_SEPARATOR"`
		// List separator character
		ListSeparator string `toml:"REDIS_LIST_SEPARATOR"`
		// Sorted set prefix
		SortedSetPrefix string `toml:"REDIS_SORTED_SET_PREFIX"`
		// Sorted set score length
		SortedSetScoreLen int `toml:"REDIS_SORTED_SET_SCORE_LEN"`
		// Set prefix
		SetPrefix string `toml:"REDIS_SET_PREFIX"`
	} `toml:"redis"`

	mu sync.RWMutex
}

var (
	globalConfig *Config
	configOnce   sync.Once
)

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		LSM: struct {
			Core struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			} `toml:"core"`
			Cache struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			} `toml:"cache"`
		}{
			Core: struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			}{
				TotalMemSizeLimit: 64 * 1024 * 1024, // 64MB
				PerMemSizeLimit:   4 * 1024 * 1024,  // 4MB
				BlockSize:         32 * 1024,        // 32KB
				SSTLevelRatio:     4,
			},
			Cache: struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			}{
				BlockCacheCapacity: 1024,
				BlockCacheK:        8,
			},
		},
		BloomFilter: struct {
			ExpectedSize      int     `toml:"BLOOM_FILTER_EXPECTED_SIZE"`
			ExpectedErrorRate float64 `toml:"BLOOM_FILTER_EXPECTED_ERROR_RATE"`
		}{
			ExpectedSize:      65536,
			ExpectedErrorRate: 0.1,
		},
		WAL: struct {
			BufferSize    int64 `toml:"WAL_BUFFER_SIZE"`
			FileSizeLimit int64 `toml:"WAL_FILE_SIZE_LIMIT"`
			CleanInterval int   `toml:"WAL_CLEAN_INTERVAL"`
		}{
			BufferSize:    1024 * 1024,      // 1MB
			FileSizeLimit: 64 * 1024 * 1024, // 64MB
			CleanInterval: 300,              // 5 minutes
		},
		Compaction: struct {
			EnableAutoCompaction bool `toml:"ENABLE_AUTO_COMPACTION"`
			TriggerRatio         int  `toml:"COMPACTION_TRIGGER_RATIO"`
			MaxThreads           int  `toml:"MAX_COMPACTION_THREADS"`
		}{
			EnableAutoCompaction: true,
			TriggerRatio:         2,
			MaxThreads:           2,
		},
		Redis: struct {
			ExpireHeader      string `toml:"REDIS_EXPIRE_HEADER"`
			HashValuePrefix   string `toml:"REDIS_HASH_VALUE_PREFIX"`
			FieldPrefix       string `toml:"REDIS_FIELD_PREFIX"`
			FieldSeparator    string `toml:"REDIS_FIELD_SEPARATOR"`
			ListSeparator     string `toml:"REDIS_LIST_SEPARATOR"`
			SortedSetPrefix   string `toml:"REDIS_SORTED_SET_PREFIX"`
			SortedSetScoreLen int    `toml:"REDIS_SORTED_SET_SCORE_LEN"`
			SetPrefix         string `toml:"REDIS_SET_PREFIX"`
		}{
			ExpireHeader:      "__expire__:",
			HashValuePrefix:   "__hash__:",
			FieldPrefix:       "__field__:",
			FieldSeparator:    "|",
			ListSeparator:     "|",
			SortedSetPrefix:   "__zset__:",
			SortedSetScoreLen: 10,
			SetPrefix:         "__set__:",
		},
	}
}

// LoadFromFile loads configuration from a TOML file
func LoadFromFile(filename string) (*Config, error) {
	config := DefaultConfig()

	if _, err := toml.DecodeFile(filename, config); err != nil {
		return nil, fmt.Errorf("failed to decode config file %s: %w", filename, err)
	}

	return config, nil
}

// LoadFromString loads configuration from a TOML string
func LoadFromString(data string) (*Config, error) {
	config := DefaultConfig()

	if _, err := toml.Decode(data, config); err != nil {
		return nil, fmt.Errorf("failed to decode config string: %w", err)
	}

	return config, nil
}

// GetGlobalConfig returns the global configuration instance
func GetGlobalConfig() *Config {
	configOnce.Do(func() {
		globalConfig = DefaultConfig()
	})
	return globalConfig
}

// SetGlobalConfig sets the global configuration instance
func SetGlobalConfig(config *Config) {
	globalConfig = config
}

// InitGlobalConfig initializes the global configuration from a file
func InitGlobalConfig(filename string) error {
	config, err := LoadFromFile(filename)
	if err != nil {
		return err
	}

	SetGlobalConfig(config)
	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.LSM.Core.TotalMemSizeLimit <= 0 {
		return fmt.Errorf("LSM total memory size limit must be positive")
	}

	if c.LSM.Core.PerMemSizeLimit <= 0 {
		return fmt.Errorf("LSM per-memory size limit must be positive")
	}

	if c.LSM.Core.PerMemSizeLimit > c.LSM.Core.TotalMemSizeLimit {
		return fmt.Errorf("LSM per-memory size limit cannot exceed total memory size limit")
	}

	if c.LSM.Core.BlockSize <= 0 {
		return fmt.Errorf("LSM block size must be positive")
	}

	if c.LSM.Core.SSTLevelRatio <= 1 {
		return fmt.Errorf("LSM SST level ratio must be greater than 1")
	}

	if c.LSM.Cache.BlockCacheCapacity <= 0 {
		return fmt.Errorf("block cache capacity must be positive")
	}

	if c.LSM.Cache.BlockCacheK <= 0 {
		return fmt.Errorf("block cache K value must be positive")
	}

	if c.BloomFilter.ExpectedSize <= 0 {
		return fmt.Errorf("bloom filter expected size must be positive")
	}

	if c.BloomFilter.ExpectedErrorRate <= 0 || c.BloomFilter.ExpectedErrorRate >= 1 {
		return fmt.Errorf("bloom filter expected error rate must be between 0 and 1")
	}

	if c.WAL.BufferSize <= 0 {
		return fmt.Errorf("WAL buffer size must be positive")
	}

	if c.WAL.FileSizeLimit <= 0 {
		return fmt.Errorf("WAL file size limit must be positive")
	}

	if c.WAL.CleanInterval <= 0 {
		return fmt.Errorf("WAL clean interval must be positive")
	}

	if c.Compaction.TriggerRatio <= 1 {
		return fmt.Errorf("compaction trigger ratio must be greater than 1")
	}

	if c.Compaction.MaxThreads <= 0 {
		return fmt.Errorf("compaction max threads must be positive")
	}

	return nil
}

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &Config{
		LSM: struct {
			Core struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			} `toml:"core"`
			Cache struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			} `toml:"cache"`
		}{
			Core: struct {
				TotalMemSizeLimit int64 `toml:"LSM_TOL_MEM_SIZE_LIMIT"`
				PerMemSizeLimit   int64 `toml:"LSM_PER_MEM_SIZE_LIMIT"`
				BlockSize         int   `toml:"LSM_BLOCK_SIZE"`
				SSTLevelRatio     int   `toml:"LSM_SST_LEVEL_RATIO"`
			}{
				TotalMemSizeLimit: c.LSM.Core.TotalMemSizeLimit,
				PerMemSizeLimit:   c.LSM.Core.PerMemSizeLimit,
				BlockSize:         c.LSM.Core.BlockSize,
				SSTLevelRatio:     c.LSM.Core.SSTLevelRatio,
			},
			Cache: struct {
				BlockCacheCapacity int `toml:"LSM_BLOCK_CACHE_CAPACITY"`
				BlockCacheK        int `toml:"LSM_BLOCK_CACHE_K"`
			}{
				BlockCacheCapacity: c.LSM.Cache.BlockCacheCapacity,
				BlockCacheK:        c.LSM.Cache.BlockCacheK,
			},
		},
		BloomFilter: struct {
			ExpectedSize      int     `toml:"BLOOM_FILTER_EXPECTED_SIZE"`
			ExpectedErrorRate float64 `toml:"BLOOM_FILTER_EXPECTED_ERROR_RATE"`
		}{
			ExpectedSize:      c.BloomFilter.ExpectedSize,
			ExpectedErrorRate: c.BloomFilter.ExpectedErrorRate,
		},
		WAL: struct {
			BufferSize    int64 `toml:"WAL_BUFFER_SIZE"`
			FileSizeLimit int64 `toml:"WAL_FILE_SIZE_LIMIT"`
			CleanInterval int   `toml:"WAL_CLEAN_INTERVAL"`
		}{
			BufferSize:    c.WAL.BufferSize,
			FileSizeLimit: c.WAL.FileSizeLimit,
			CleanInterval: c.WAL.CleanInterval,
		},
		Compaction: struct {
			EnableAutoCompaction bool `toml:"ENABLE_AUTO_COMPACTION"`
			TriggerRatio         int  `toml:"COMPACTION_TRIGGER_RATIO"`
			MaxThreads           int  `toml:"MAX_COMPACTION_THREADS"`
		}{
			EnableAutoCompaction: c.Compaction.EnableAutoCompaction,
			TriggerRatio:         c.Compaction.TriggerRatio,
			MaxThreads:           c.Compaction.MaxThreads,
		},
		Redis: struct {
			ExpireHeader      string `toml:"REDIS_EXPIRE_HEADER"`
			HashValuePrefix   string `toml:"REDIS_HASH_VALUE_PREFIX"`
			FieldPrefix       string `toml:"REDIS_FIELD_PREFIX"`
			FieldSeparator    string `toml:"REDIS_FIELD_SEPARATOR"`
			ListSeparator     string `toml:"REDIS_LIST_SEPARATOR"`
			SortedSetPrefix   string `toml:"REDIS_SORTED_SET_PREFIX"`
			SortedSetScoreLen int    `toml:"REDIS_SORTED_SET_SCORE_LEN"`
			SetPrefix         string `toml:"REDIS_SET_PREFIX"`
		}{
			ExpireHeader:      c.Redis.ExpireHeader,
			HashValuePrefix:   c.Redis.HashValuePrefix,
			FieldPrefix:       c.Redis.FieldPrefix,
			FieldSeparator:    c.Redis.FieldSeparator,
			ListSeparator:     c.Redis.ListSeparator,
			SortedSetPrefix:   c.Redis.SortedSetPrefix,
			SortedSetScoreLen: c.Redis.SortedSetScoreLen,
			SetPrefix:         c.Redis.SetPrefix,
		},
	}
}

// GetTotalMemSizeLimit returns the total memory size limit
func (c *Config) GetTotalMemSizeLimit() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.TotalMemSizeLimit
}

// GetPerMemSizeLimit returns the per-memory table size limit
func (c *Config) GetPerMemSizeLimit() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.PerMemSizeLimit
}

// GetBlockSize returns the block size
func (c *Config) GetBlockSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.BlockSize
}

// GetSSTLevelRatio returns the SST level ratio
func (c *Config) GetSSTLevelRatio() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Core.SSTLevelRatio
}

// GetBlockCacheCapacity returns the block cache capacity
func (c *Config) GetBlockCacheCapacity() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Cache.BlockCacheCapacity
}

// GetBlockCacheK returns the block cache K value
func (c *Config) GetBlockCacheK() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LSM.Cache.BlockCacheK
}

// GetWALBufferSize returns the WAL buffer size
func (c *Config) GetWALBufferSize() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.WAL.BufferSize
}

// GetWALFileSizeLimit returns the WAL file size limit
func (c *Config) GetWALFileSizeLimit() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.WAL.FileSizeLimit
}

// GetWALCleanInterval returns the WAL clean interval
func (c *Config) GetWALCleanInterval() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.WAL.CleanInterval
}

// Redis configuration getters

// GetRedisExpireHeader returns the Redis expire header prefix
func (c *Config) GetRedisExpireHeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.ExpireHeader
}

// GetRedisHashValuePrefix returns the Redis hash value prefix
func (c *Config) GetRedisHashValuePrefix() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.HashValuePrefix
}

// GetRedisFieldPrefix returns the Redis field prefix
func (c *Config) GetRedisFieldPrefix() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.FieldPrefix
}

// GetRedisFieldSeparator returns the Redis field separator
func (c *Config) GetRedisFieldSeparator() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.FieldSeparator
}

// GetRedisListSeparator returns the Redis list separator
func (c *Config) GetRedisListSeparator() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.ListSeparator
}

// GetRedisSortedSetPrefix returns the Redis sorted set prefix
func (c *Config) GetRedisSortedSetPrefix() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.SortedSetPrefix
}

// GetRedisSortedSetScoreLen returns the Redis sorted set score length
func (c *Config) GetRedisSortedSetScoreLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.SortedSetScoreLen
}

// GetRedisSetPrefix returns the Redis set prefix
func (c *Config) GetRedisSetPrefix() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Redis.SetPrefix
}
