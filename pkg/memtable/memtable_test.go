package memtable

import (
	"fmt"
	"testing"
	"tiny-lsm-go/pkg/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemTableBasicOperations(t *testing.T) {
	mt := New()

	// Test initial state
	assert.Equal(t, 0, mt.GetCurrentSize())
	assert.Equal(t, 0, mt.GetFrozenSize())
	assert.Equal(t, 0, mt.GetTotalSize())

	// Test Put and Get
	err := mt.Put("key1", "value1", 1)
	require.NoError(t, err)

	value, found, err := mt.Get("key1", 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", value)

	// Test non-existent key
	value, found, err = mt.Get("nonexistent", 0)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", value)
}

func TestMemTableMVCC(t *testing.T) {
	mt := New()

	// Add multiple versions of the same key
	err := mt.Put("key1", "value1", 1)
	require.NoError(t, err)

	err = mt.Put("key1", "value2", 2)
	require.NoError(t, err)

	err = mt.Put("key1", "value3", 3)
	require.NoError(t, err)

	// Should see latest version with txnID=0 (see all)
	value, found, err := mt.Get("key1", 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value3", value)

	// Should see version 2 with txnID=2
	value, found, err = mt.Get("key1", 2)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value2", value)

	// Should see version 1 with txnID=1
	value, found, err = mt.Get("key1", 1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", value)
}

func TestMemTableDelete(t *testing.T) {
	mt := New()

	// Add a key
	err := mt.Put("key1", "value1", 1)
	require.NoError(t, err)

	// Verify it exists
	value, found, err := mt.Get("key1", 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", value)

	// Delete the key
	err = mt.Delete("key1", 2)
	require.NoError(t, err)

	// Should not be found after deletion
	_, found, err = mt.Get("key1", 0)
	require.NoError(t, err)
	assert.False(t, found)

	// But should still be found for transaction snapshot before deletion
	value, found, err = mt.Get("key1", 1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", value)
}

func TestMemTableBatchOperations(t *testing.T) {
	mt := New()

	// Test batch put
	kvs := []common.KVPair{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	err := mt.PutBatch(kvs, 1)
	require.NoError(t, err)

	// Test batch get
	keys := []string{"key1", "key2", "key3", "nonexistent"}
	results, err := mt.GetBatch(keys, 0)
	require.NoError(t, err)
	require.Len(t, results, 4)

	assert.True(t, results[0].Found)
	assert.Equal(t, "value1", results[0].Value)

	assert.True(t, results[1].Found)
	assert.Equal(t, "value2", results[1].Value)

	assert.True(t, results[2].Found)
	assert.Equal(t, "value3", results[2].Value)

	assert.False(t, results[3].Found)

	// Test batch delete
	deleteKeys := []string{"key1", "key3"}
	err = mt.DeleteBatch(deleteKeys, 2)
	require.NoError(t, err)

	// Verify deletions
	results, err = mt.GetBatch(keys, 0)
	require.NoError(t, err)

	assert.False(t, results[0].Found) // key1 deleted
	assert.True(t, results[1].Found)  // key2 still exists
	assert.False(t, results[2].Found) // key3 deleted
	assert.False(t, results[3].Found) // nonexistent still not found
}

func TestMemTableFreezing(t *testing.T) {
	mt := New()

	// Add some data
	for i := 0; i < 10; i++ {
		err := mt.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), uint64(i+1))
		require.NoError(t, err)
	}

	initialCurrentSize := mt.GetCurrentSize()
	assert.Greater(t, initialCurrentSize, 0)
	assert.Equal(t, 0, mt.GetFrozenSize())
	assert.Equal(t, 0, mt.GetFrozenTableCount())

	// Manually freeze current table
	mt.FreezeCurrentTable()

	// Current table should be empty now, and we should have frozen data
	assert.Equal(t, 0, mt.GetCurrentSize())
	assert.Equal(t, initialCurrentSize, mt.GetFrozenSize())
	assert.Equal(t, 1, mt.GetFrozenTableCount())

	// Should still be able to read the data
	for i := 0; i < 10; i++ {
		value, found, err := mt.Get(fmt.Sprintf("key%d", i), 0)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, fmt.Sprintf("value%d", i), value)
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := New()

	// Add data in non-sorted order
	testData := map[string]string{
		"banana": "yellow",
		"apple":  "red",
		"cherry": "red",
		"date":   "brown",
	}

	txnID := uint64(1)
	for key, value := range testData {
		err := mt.Put(key, value, txnID)
		require.NoError(t, err)
		txnID++
	}

	// Test iteration - should be in sorted order
	iter := mt.NewIterator(0)
	defer iter.Close()

	expectedKeys := []string{"apple", "banana", "cherry", "date"}
	expectedValues := []string{"red", "yellow", "red", "brown"}

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		require.Less(t, i, len(expectedKeys), "Too many entries")
		assert.Equal(t, expectedKeys[i], iter.Key())
		assert.Equal(t, expectedValues[i], iter.Value())
		assert.False(t, iter.IsDeleted())
		i++
	}
	assert.Equal(t, len(expectedKeys), i, "Not enough entries")
}

func TestMemTableIteratorWithFrozenTables(t *testing.T) {
	mt := New()

	// Add some data to current table
	err := mt.Put("key1", "value1", 1)
	require.NoError(t, err)
	err = mt.Put("key2", "value2", 2)
	require.NoError(t, err)

	// Freeze current table
	mt.FreezeCurrentTable()

	// Add more data to new current table
	err = mt.Put("key3", "value3", 3)
	require.NoError(t, err)
	err = mt.Put("key1", "new_value1", 4) // Update key1 with newer version
	require.NoError(t, err)

	// Test iteration should merge data from both tables
	iter := mt.NewIterator(0)
	defer iter.Close()

	entries := make(map[string]string)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		entries[iter.Key()] = iter.Value()
	}

	// Should see newest version of key1, and all other keys
	expected := map[string]string{
		"key1": "new_value1", // Updated version from current table
		"key2": "value2",     // From frozen table
		"key3": "value3",     // From current table
	}

	assert.Equal(t, expected, entries)
}

func TestMemTableFlush(t *testing.T) {
	mt := New()

	// Add some data
	for i := 0; i < 10; i++ {
		err := mt.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), uint64(i+1))
		require.NoError(t, err)
	}

	// Should be able to flush
	assert.False(t, mt.CanFlush())
	assert.Equal(t, 1, mt.GetFlushableTableCount()) // Current table can be flushed

	// Freeze current table
	mt.FreezeCurrentTable()
	assert.True(t, mt.CanFlush())
	assert.Equal(t, 1, mt.GetFlushableTableCount()) // Frozen table can be flushed

	// Flush oldest table
	result, err := mt.FlushOldest()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Len(t, result.Entries, 10)
	assert.Equal(t, "key0", result.FirstKey)
	assert.Equal(t, "key9", result.LastKey)
	assert.Equal(t, uint64(1), result.MinTxnID)
	assert.Equal(t, uint64(10), result.MaxTxnID)

	// Should not be able to flush anymore
	assert.False(t, mt.CanFlush())
	assert.Equal(t, 0, mt.GetFlushableTableCount())
}

func TestMemTableStats(t *testing.T) {
	mt := New()

	// Initially empty
	stats := mt.GetMemTableStats()
	assert.Equal(t, 0, stats.CurrentTableSize)
	assert.Equal(t, 0, stats.FrozenTablesSize)
	assert.Equal(t, 0, stats.TotalSize)

	// Add some data
	for i := 0; i < 10; i++ {
		err := mt.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), uint64(i+1))
		require.NoError(t, err)
	}

	stats = mt.GetMemTableStats()
	assert.Greater(t, stats.CurrentTableSize, 0)
	assert.Equal(t, 0, stats.FrozenTablesSize)
	assert.Equal(t, stats.CurrentTableSize, stats.TotalSize)
	assert.Equal(t, 10, stats.CurrentTableEntries)

	// Freeze table
	mt.FreezeCurrentTable()

	stats = mt.GetMemTableStats()
	assert.Equal(t, 0, stats.CurrentTableSize)
	assert.Greater(t, stats.FrozenTablesSize, 0)
	assert.Equal(t, stats.FrozenTablesSize, stats.TotalSize)
	assert.Equal(t, 1, stats.FrozenTablesCount)
}

func TestMemTableClear(t *testing.T) {
	mt := New()

	// Add data
	for i := 0; i < 10; i++ {
		err := mt.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), uint64(i+1))
		require.NoError(t, err)
	}

	// Freeze some tables
	mt.FreezeCurrentTable()

	// Add more data
	err := mt.Put("extra", "data", 100)
	require.NoError(t, err)

	// Verify we have data
	assert.Greater(t, mt.GetTotalSize(), 0)
	assert.Greater(t, mt.GetCurrentSize(), 0)
	assert.Greater(t, mt.GetFrozenSize(), 0)

	// Clear everything
	mt.Clear()

	// Should be empty
	assert.Equal(t, 0, mt.GetTotalSize())
	assert.Equal(t, 0, mt.GetCurrentSize())
	assert.Equal(t, 0, mt.GetFrozenSize())
	assert.Equal(t, 0, mt.GetFrozenTableCount())

	// Should not find any data
	value, found, err := mt.Get("key1", 0)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", value)
}
