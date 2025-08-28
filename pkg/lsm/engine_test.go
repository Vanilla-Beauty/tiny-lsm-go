package lsm

import (
	"fmt"
	"os"
	"testing"

	"tiny-lsm-go/pkg/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestEngine(t *testing.T) (*Engine, func()) {
	tempDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.LSM.Core.PerMemSizeLimit = 1024 * 1024 // 1MB for testing
	cfg.LSM.Core.BlockSize = 4096              // 4KB blocks

	engine, err := NewEngine(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create test engine: %v", err)
	}

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tempDir)
	}

	return engine, cleanup
}

func TestBasicOperations(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Test Put and Get
	err := engine.Put("key1", "value1")
	require.NoError(t, err)

	value, found, err := engine.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", value)

	// Test update
	err = engine.Put("key1", "new_value")
	require.NoError(t, err)

	value, found, err = engine.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "new_value", value)

	// Test Delete
	err = engine.Delete("key1")
	require.NoError(t, err)

	value, found, err = engine.Get("key1")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", value)

	// Test non-existent key
	value, found, err = engine.Get("nonexistent")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", value)
}

func TestPersistence(t *testing.T) {
	tempDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.LSM.Core.PerMemSizeLimit = 1024 * 1024 // 1MB for testing
	cfg.LSM.Core.BlockSize = 4096              // 4KB blocks

	kvs := make(map[string]string)
	del_kvs := make(map[string]string)
	var metaData *EngineMetadata
	{
		engine, err := NewEngine(cfg, tempDir)
		require.NoError(t, err)
		defer engine.Close()

		num := 1000

		for i := 0; i < num; i++ {
			key := "key" + fmt.Sprintf("%d", i)
			value := "value" + fmt.Sprintf("%d", i)
			err := engine.Put(key, value)
			require.NoError(t, err)
			kvs[key] = value

			// Delete keys divisible by 10
			if i%10 == 0 && i != 0 {
				delKey := "key" + fmt.Sprintf("%d", i-10)
				err := engine.Delete(delKey)
				require.NoError(t, err)
				delete(kvs, delKey)
				del_kvs[delKey] = value
			}
		}

		engine.Close()
		metaData = engine.GetMeta()
	}

	// Reopen engine and verify data
	engine, err := NewEngine(cfg, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	NewMetaData := engine.GetMeta()

	assert.Equal(t, metaData.NextSSTID, NewMetaData.NextSSTID)
	assert.Equal(t, metaData.NextTxnID, NewMetaData.NextTxnID)

	// Verify exist data
	for key, expectedValue := range kvs {
		value, found, err := engine.Get(key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, expectedValue, value)
	}
	// Verify deleted data
	for key := range del_kvs {
		value, found, err := engine.Get(key)
		require.NoError(t, err)
		assert.False(t, found)
		if !found {
			t.Log("Correctly missing deleted key:", key)
		}
		assert.Equal(t, "", value)
	}

	// Test non-existent key
	value, found, err := engine.Get("nonexistent")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", value)
}

func TestLargeScaleOperations(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert enough data to trigger multiple flushes

	// Insert enough data to trigger multiple flushes
	data := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := "key" + fmt.Sprintf("%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		err := engine.Put(key, value)
		require.NoError(t, err)
		data[key] = value
	}

	// Verify all data
	for key, expectedValue := range data {
		value, found, err := engine.Get(key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, expectedValue, value)
	}
}

func TestMVCC(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Insert keys with different transaction IDs
	// key00-key19 first with txnID=1
	for i := 0; i < 20; i++ {
		key := "key" + fmt.Sprintf("%02d", i) // Zero-padded for consistent ordering
		err := engine.PutWithTxnID(key, "tranc1", 1)
		require.NoError(t, err)
	}

	// Force flush to SST
	err := engine.Flush()
	require.NoError(t, err)

	// key00-key09 again with txnID=2
	for i := 0; i < 10; i++ {
		key := "key" + fmt.Sprintf("%02d", i) // Zero-padded for consistent ordering
		err := engine.PutWithTxnID(key, "tranc2", 2)
		require.NoError(t, err)
	}

	// When querying with txnID=1, should only see tranc1 values
	for i := 0; i < 20; i++ {
		key := "key" + fmt.Sprintf("%02d", i) // Zero-padded for consistent ordering
		value, found, err := engine.GetWithTxnID(key, 1)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "tranc1", value)
	}

	// When querying with txnID=2, should see tranc2 for first 10 keys and tranc1 for the rest
	for i := 0; i < 20; i++ {
		key := "key" + fmt.Sprintf("%02d", i) // Zero-padded for consistent ordering
		value, found, err := engine.GetWithTxnID(key, 2)
		require.NoError(t, err)
		assert.True(t, found)

		if i < 10 {
			assert.Equal(t, "tranc2", value)
		} else {
			assert.Equal(t, "tranc1", value)
		}
	}
}
