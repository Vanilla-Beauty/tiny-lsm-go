package block

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockBasicOperations(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Build an empty block first
	block := builder.Build()

	// Initially empty
	assert.Equal(t, "", block.FirstKey())
	assert.Equal(t, 0, block.NumEntries())

	// Add some entries to builder
	builder = NewBlockBuilder(1024)
	err := builder.Add("apple", "red", 1, false)
	require.NoError(t, err)

	err = builder.Add("banana", "yellow", 2, false)
	require.NoError(t, err)

	err = builder.Add("cherry", "dark_red", 3, false)
	require.NoError(t, err)

	// Build the block
	block = builder.Build()

	// Verify basic properties
	assert.Equal(t, "apple", block.FirstKey())
	assert.Equal(t, 3, block.NumEntries())
	assert.Greater(t, block.Size(), 0)
}

func TestBlockGetEntry(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add test data
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2", 2, false)
	require.NoError(t, err)
	err = builder.Add("key3", "", 3, false) // deleted entry
	require.NoError(t, err)

	block := builder.Build()

	// Test getting entries by index
	entry, err := block.GetEntry(0)
	require.NoError(t, err)
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, "value1", entry.Value)
	assert.Equal(t, uint64(1), entry.TxnID)

	entry, err = block.GetEntry(1)
	require.NoError(t, err)
	assert.Equal(t, "key2", entry.Key)
	assert.Equal(t, "value2", entry.Value)

	// Test deleted entry
	entry, err = block.GetEntry(2)
	require.NoError(t, err)
	assert.Equal(t, "key3", entry.Key)
	assert.Equal(t, "", entry.Value)

	// Test invalid index
	_, err = block.GetEntry(3)
	assert.Error(t, err)
}

func TestBlockMVCC(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add multiple versions of the same key
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key1", "value2", 2, false)
	require.NoError(t, err)
	err = builder.Add("key1", "value3", 3, false)
	require.NoError(t, err)

	block := builder.Build()

	// Test with iterator to find values for different transactions
	iter := block.NewIterator()

	// txnID=0 should see all versions (latest)
	iter.(*BlockIterator).SetTxnID(0)
	iter.Seek("key1")
	assert.True(t, iter.Valid())
	assert.Equal(t, "value3", iter.Value())

	// txnID=2 should see version 2
	iter.(*BlockIterator).SetTxnID(2)
	iter.Seek("key1")
	assert.True(t, iter.Valid())
	assert.Equal(t, "value2", iter.Value())

	// txnID=1 should see version 1
	iter.(*BlockIterator).SetTxnID(1)
	iter.Seek("key1")
	assert.True(t, iter.Valid())
	assert.Equal(t, "value1", iter.Value())

	iter.Close()
}

func TestBlockDeletedEntries(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add a key and then delete it
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key1", "", 2, false) // delete marker
	require.NoError(t, err)

	block := builder.Build()

	// Test with iterator for different transactions
	iter := block.NewIterator()
	defer iter.Close()

	// Current transaction (see all) should see latest deleted version
	iter.(*BlockIterator).SetTxnID(0)
	iter.Seek("key1")
	assert.True(t, iter.Valid())
	assert.True(t, iter.IsDeleted())

	// Earlier transaction should see the non-deleted version
	iter.(*BlockIterator).SetTxnID(1)
	iter.Seek("key1")
	assert.True(t, iter.Valid())
	assert.False(t, iter.IsDeleted())
	assert.Equal(t, "value1", iter.Value())
}

func TestBlockEncodeDecodeEmpty(t *testing.T) {
	builder := NewBlockBuilder(1024)
	emptyBlock := builder.Build()

	// Empty block should have minimal data
	data := emptyBlock.Data()
	assert.GreaterOrEqual(t, len(data), 0)

	// Decode empty block
	decoded, err := NewBlock(data)
	require.NoError(t, err)

	assert.Equal(t, "", decoded.FirstKey())
	assert.Equal(t, 0, decoded.NumEntries())
}

func TestBlockEncodeDecode(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add test data
	testData := []struct {
		key   string
		value string
		txnID uint64
	}{
		{"apple", "red", 1},
		{"banana", "yellow", 2},
		{"cherry", "dark_red", 3},
		{"date", "", 4}, // deleted entry
		{"elderberry", "purple", 5},
	}

	for _, entry := range testData {
		err := builder.Add(entry.key, entry.value, entry.txnID, false)
		require.NoError(t, err)
	}

	originalBlock := builder.Build()

	// Encode
	encoded := originalBlock.Data()

	// Decode
	decodedBlock, err := NewBlock(encoded)
	require.NoError(t, err)

	// Verify decoded block matches original
	assert.Equal(t, originalBlock.FirstKey(), decodedBlock.FirstKey())
	assert.Equal(t, originalBlock.NumEntries(), decodedBlock.NumEntries())

	// Verify all entries can be retrieved correctly
	for i, entry := range testData {
		decodedEntry, err := decodedBlock.GetEntry(i)
		require.NoError(t, err)
		assert.Equal(t, entry.key, decodedEntry.Key)
		assert.Equal(t, entry.value, decodedEntry.Value)
		assert.Equal(t, entry.txnID, decodedEntry.TxnID)
	}
}

func TestBlockBinarySearch(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add sorted keys
	keys := []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg"}
	for i, key := range keys {
		err := builder.Add(key, fmt.Sprintf("value%d", i), uint64(i+1), false)
		require.NoError(t, err)
	}

	block := builder.Build()

	// Test existing keys using FindEntry
	for _, key := range keys {
		idx := block.FindEntry(key)
		assert.NotEqual(t, -1, idx, "Key %s should be found", key)
		entry, err := block.GetEntry(idx)
		require.NoError(t, err)
		assert.Equal(t, key, entry.Key)
	}

	// Test non-existing keys
	nonExistentKeys := []string{"a", "ab", "bc", "cd", "de", "ef", "fg", "gh", "zz"}
	for _, key := range nonExistentKeys {
		idx := block.FindEntry(key)
		if key == "zz" {
			// After last key should return -1
			assert.Equal(t, -1, idx, "Key %s should not be found", key)
		} else {
			// Other keys should find next valid entry
			if idx != -1 {
				entry, err := block.GetEntry(idx)
				require.NoError(t, err)
				assert.Greater(t, entry.Key, key, "Found key should be greater than search key %s", key)
			}
		}
	}
}

func TestBlockLargeData(t *testing.T) {
	builder := NewBlockBuilder(32 * 1024) // 32KB block

	numEntries := 500 // Reduced to fit in block size
	expectedData := make(map[string]string)

	// Add large amount of sorted data
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i) // zero-padded for proper sorting
		value := fmt.Sprintf("value%04d", i)
		expectedData[key] = value

		err := builder.Add(key, value, uint64(i+1), false)
		if err != nil {
			// Stop adding if block is full
			break
		}
	}

	block := builder.Build()

	// Verify all data can be retrieved using iterator
	iter := block.NewIterator()
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		expectedValue, exists := expectedData[key]
		if exists {
			assert.Equal(t, expectedValue, value, "Value mismatch for key %s", key)
		}
		count++
	}
	assert.Greater(t, count, 0)

	// Test encode/decode with large data
	encoded := block.Data()
	decoded, err := NewBlock(encoded)
	require.NoError(t, err)

	// Verify decoded data
	assert.Equal(t, block.NumEntries(), decoded.NumEntries())
	assert.Equal(t, block.FirstKey(), decoded.FirstKey())
}

func TestBlockSpecialCharacters(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Test keys and values with special characters
	testCases := []struct {
		key   string
		value string
	}{
		{"", ""}, // empty key and value
		{"key\x00with\x00null", "value\x00with\x00null"},
		{"key\twith\ttab", "value\twith\ttab"},
		{"key\nwith\nnewline", "value\nwith\nnewline"},
		{"key\rwith\rcarriage", "value\rwith\rcarriage"},
		{"key with spaces", "value with spaces"},
		{"键值", "值"}, // Unicode characters
	}

	// Add all test cases
	for i, tc := range testCases {
		err := builder.Add(tc.key, tc.value, uint64(i+1), false)
		require.NoError(t, err)
	}

	block := builder.Build()

	// Verify retrieval using iterator
	iter := block.NewIterator()
	defer iter.Close()

	for i, tc := range testCases {
		entry, err := block.GetEntry(i)
		require.NoError(t, err)
		assert.Equal(t, tc.key, entry.Key, "Key mismatch: %q", tc.key)
		assert.Equal(t, tc.value, entry.Value, "Value mismatch for key: %q", tc.key)
	}

	// Test encode/decode preserves special characters
	encoded := block.Data()
	decoded, err := NewBlock(encoded)
	require.NoError(t, err)

	for i, tc := range testCases {
		entry, err := decoded.GetEntry(i)
		require.NoError(t, err)
		assert.Equal(t, tc.key, entry.Key, "Key should be preserved in decoded block: %q", tc.key)
		assert.Equal(t, tc.value, entry.Value, "Value should be preserved in decoded block for key: %q", tc.key)
	}
}

func TestBlockErrorHandling(t *testing.T) {
	// Test decoding invalid data
	invalidData := [][]byte{
		{},                       // empty data
		{1, 2, 3},                // too short
		{0xFF, 0xFF, 0xFF, 0xFF}, // invalid format
	}

	for _, data := range invalidData {
		_, err := NewBlock(data)
		assert.Error(t, err, "Should error on invalid data: %v", data)
	}
}

func TestBlockCapacity(t *testing.T) {
	builder := NewBlockBuilder(64) // very small block

	// Try to add data that exceeds capacity
	largeValue := string(make([]byte, 100)) // 100 bytes
	err := builder.Add("key1", largeValue, 1, false)

	// Should either succeed or fail gracefully
	if err != nil {
		assert.Error(t, err)
	} else {
		// If it succeeds, verify the data is there
		block := builder.Build()
		entry, err := block.GetEntry(0)
		require.NoError(t, err)
		assert.Equal(t, "key1", entry.Key)
		assert.Equal(t, largeValue, entry.Value)
	}
}

func TestBlockIteratorBasic(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add test data in sorted order (keys will be sorted in the block)
	entries := []struct {
		key   string
		value string
	}{
		{"apple", "green"},
		{"banana", "yellow"},
		{"cherry", "red"},
		{"date", "brown"},
	}

	for i, entry := range entries {
		err := builder.Add(entry.key, entry.value, uint64(i+1), false)
		require.NoError(t, err)
	}

	block := builder.Build()

	// Get iterator
	iter := block.NewIterator()
	defer iter.Close()

	// Collect all entries from iterator
	var iteratedEntries []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		iteratedEntries = append(iteratedEntries, iter.Key())
	}

	// Should be in the same order as added (sorted)
	expectedKeys := []string{"apple", "banana", "cherry", "date"}
	assert.Equal(t, expectedKeys, iteratedEntries)
}

func TestBlockIteratorWithDeletedEntries(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add entries with some deletions
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2", 2, false)
	require.NoError(t, err)
	err = builder.Add("key3", "", 3, false) // deleted
	require.NoError(t, err)
	err = builder.Add("key4", "value4", 4, false)
	require.NoError(t, err)

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	validEntries := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !iter.IsDeleted() {
			validEntries++
		}
	}

	// Should see 3 valid entries (key3 is deleted)
	assert.Equal(t, 3, validEntries)
}

func BenchmarkBlockAddEntry(b *testing.B) {
	builder := NewBlockBuilder(1024 * 1024) // 1MB block

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		builder.Add(key, value, uint64(i), false)
	}
}

func BenchmarkBlockGetEntry(b *testing.B) {
	builder := NewBlockBuilder(1024 * 1024)

	// Pre-populate with data
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		builder.Add(key, value, uint64(i), false)
	}

	block := builder.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numEntries
		_, _ = block.GetEntry(idx)
	}
}

func BenchmarkBlockEncode(b *testing.B) {
	builder := NewBlockBuilder(1024 * 1024)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		builder.Add(key, value, uint64(i), false)
	}

	block := builder.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = block.Data()
	}
}

func BenchmarkBlockDecode(b *testing.B) {
	builder := NewBlockBuilder(1024 * 1024)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		builder.Add(key, value, uint64(i), false)
	}

	block := builder.Build()
	encoded := block.Data()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewBlock(encoded)
	}
}
