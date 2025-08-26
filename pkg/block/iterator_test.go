package block

import (
	"fmt"
	"testing"

	"tiny-lsm-go/pkg/iterator"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockIteratorBasicIteration(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add test entries in sorted order
	testData := []struct {
		key   string
		value string
		txnID uint64
	}{
		{"apple", "green", 1},
		{"banana", "yellow", 2},
		{"cherry", "red", 3},
		{"date", "brown", 4},
	}

	for _, entry := range testData {
		err := builder.Add(entry.key, entry.value, entry.txnID, false)
		require.NoError(t, err)
	}

	block := builder.Build()

	// Create iterator
	iter := block.NewIterator()
	defer iter.Close()

	// Test forward iteration - should be in the order added (sorted)
	expectedOrder := []string{"apple", "banana", "cherry", "date"}
	expectedValues := []string{"green", "yellow", "red", "brown"}

	// Seek to first
	iter.SeekToFirst()
	for i, expectedKey := range expectedOrder {
		require.True(t, iter.Valid(), "Iterator should be valid at position %d", i)
		assert.Equal(t, expectedKey, iter.Key())
		assert.Equal(t, expectedValues[i], iter.Value())
		assert.False(t, iter.IsDeleted())

		if i < len(expectedOrder)-1 {
			iter.Next()
		}
	}

	// Move past last element
	iter.Next()
	assert.False(t, iter.Valid())
}

func TestBlockIteratorEmptyBlock(t *testing.T) {
	builder := NewBlockBuilder(1024)
	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// Empty block iterator should be invalid
	iter.SeekToFirst()
	assert.False(t, iter.Valid())

	// Seeking in empty block should remain invalid
	iter.Seek("any_key")
	assert.False(t, iter.Valid())
}

func TestBlockIteratorSeekOperations(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add sorted test data
	keys := []string{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"}
	for i, key := range keys {
		err := builder.Add(key, fmt.Sprintf("value_%s", key), uint64(i+1), false)
		require.NoError(t, err)
	}

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// Test seeking to existing keys
	for _, key := range keys {
		iter.Seek(key)
		require.True(t, iter.Valid(), "Should find key %s", key)
		assert.Equal(t, key, iter.Key())
		assert.Equal(t, fmt.Sprintf("value_%s", key), iter.Value())
	}

	// Test seeking to non-existing keys (should find next key)
	testCases := []struct {
		seekKey     string
		expectedKey string
	}{
		{"a", "aa"},  // before first key
		{"ab", "bb"}, // between aa and bb
		{"bc", "cc"}, // between bb and cc
		{"dd", "dd"}, // exact match
		{"de", "ee"}, // between dd and ee
		{"zz", ""},   // after last key (invalid)
	}

	for _, tc := range testCases {
		iter.Seek(tc.seekKey)
		if tc.expectedKey == "" {
			assert.False(t, iter.Valid(), "Seek to %s should be invalid", tc.seekKey)
		} else {
			require.True(t, iter.Valid(), "Seek to %s should be valid", tc.seekKey)
			assert.Equal(t, tc.expectedKey, iter.Key(), "Seek to %s should find %s", tc.seekKey, tc.expectedKey)
		}
	}
}

func TestBlockIteratorMVCC(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add multiple versions of keys
	err := builder.Add("key1", "value1_v1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key1", "value1_v2", 2, false)
	require.NoError(t, err)
	err = builder.Add("key1", "value1_v3", 3, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2_v1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2_v2", 4, false)
	require.NoError(t, err)

	block := builder.Build()

	// Test iterator with different transaction IDs
	testCases := []struct {
		txnID           uint64
		expectedEntries map[string]string
	}{
		{0, map[string]string{"key1": "value1_v3", "key2": "value2_v2"}}, // see all
		{1, map[string]string{"key1": "value1_v1", "key2": "value2_v1"}}, // see v1 only
		{2, map[string]string{"key1": "value1_v2", "key2": "value2_v1"}}, // see v1 and v2
		{3, map[string]string{"key1": "value1_v3", "key2": "value2_v1"}}, // see v1, v2, v3
		{4, map[string]string{"key1": "value1_v3", "key2": "value2_v2"}}, // see all
	}

	for _, tc := range testCases {
		iter := block.NewIterator()
		iter.(*BlockIterator).SetTxnID(tc.txnID)
		results := make(map[string]string)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if !iter.IsDeleted() {
				results[iter.Key()] = iter.Value()
			}
		}
		iter.Close()

		assert.Equal(t, tc.expectedEntries, results, "Transaction %d should see correct entries", tc.txnID)
	}
}

func TestBlockIteratorWithDeletes(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add entries with some deletes
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2", 2, false)
	require.NoError(t, err)
	err = builder.Add("key3", "value3", 3, false)
	require.NoError(t, err)
	err = builder.Add("key2", "", 4, false)
	require.NoError(t, err)
	err = builder.Add("key4", "value4", 5, false)
	require.NoError(t, err)

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// Collect all entries including deleted ones
	var allEntries []struct {
		key     string
		value   string
		deleted bool
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		allEntries = append(allEntries, struct {
			key     string
			value   string
			deleted bool
		}{
			key:     iter.Key(),
			value:   iter.Value(),
			deleted: iter.IsDeleted(),
		})
	}

	// Should see key1, key2(deleted), key3, key4
	assert.Equal(t, 4, len(allEntries))
	assert.Equal(t, "key1", allEntries[0].key)
	assert.False(t, allEntries[0].deleted)
	assert.Equal(t, "key2", allEntries[1].key)
	assert.True(t, allEntries[1].deleted)
	assert.Equal(t, "key3", allEntries[2].key)
	assert.False(t, allEntries[2].deleted)
	assert.Equal(t, "key4", allEntries[3].key)
	assert.False(t, allEntries[3].deleted)
}

func TestBlockIteratorSeekToFirst(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add some entries in sorted order
	entries := []string{"aa", "bb", "cc", "dd"}
	for i, key := range entries {
		err := builder.Add(key, fmt.Sprintf("value_%d", i), uint64(i+1), false)
		require.NoError(t, err)
	}

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// SeekToFirst should go to the first key alphabetically
	iter.SeekToFirst()
	require.True(t, iter.Valid())
	assert.Equal(t, "aa", iter.Key())

	// Move to middle
	iter.Next()
	iter.Next()
	assert.Equal(t, "cc", iter.Key())

	// SeekToFirst should reset to beginning
	iter.SeekToFirst()
	require.True(t, iter.Valid())
	assert.Equal(t, "aa", iter.Key())
}

func TestBlockIteratorBoundaryConditions(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add single entry
	err := builder.Add("single", "value", 1, false)
	require.NoError(t, err)

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// Test single entry iteration
	iter.SeekToFirst()
	require.True(t, iter.Valid())
	assert.Equal(t, "single", iter.Key())
	assert.Equal(t, "value", iter.Value())

	// Move past single entry
	iter.Next()
	assert.False(t, iter.Valid())

	// Seek to the single entry
	iter.Seek("single")
	require.True(t, iter.Valid())
	assert.Equal(t, "single", iter.Key())

	// Seek past single entry
	iter.Seek("z")
	assert.False(t, iter.Valid())

	// Seek before single entry
	iter.Seek("a")
	require.True(t, iter.Valid())
	assert.Equal(t, "single", iter.Key())
}

func TestBlockIteratorLargeDataset(t *testing.T) {
	builder := NewBlockBuilder(64 * 1024) // 64KB block

	numEntries := 500 // Reduced to fit block size limit
	// Add large dataset
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		err := builder.Add(key, value, uint64(i+1), false)
		if err != nil {
			// Stop adding if block is full
			numEntries = i
			break
		}
	}

	block := builder.Build()

	iter := block.NewIterator()
	defer iter.Close()

	// Test full iteration
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		expectedKey := fmt.Sprintf("key_%04d", count)
		expectedValue := fmt.Sprintf("value_%04d", count)
		assert.Equal(t, expectedKey, iter.Key())
		assert.Equal(t, expectedValue, iter.Value())
		assert.False(t, iter.IsDeleted())
		count++
	}
	assert.Equal(t, numEntries, count)

	// Test random seeks - only test indices that exist
	testIndices := []int{0}
	if numEntries > 100 {
		testIndices = append(testIndices, 100)
	}
	if numEntries > 250 {
		testIndices = append(testIndices, numEntries/2)
	}
	if numEntries > 10 {
		testIndices = append(testIndices, numEntries-1)
	}

	for _, idx := range testIndices {
		seekKey := fmt.Sprintf("key_%04d", idx)
		iter.Seek(seekKey)
		require.True(t, iter.Valid(), "Should find key at index %d (numEntries=%d)", idx, numEntries)
		assert.Equal(t, seekKey, iter.Key())
	}
}

func TestBlockIteratorMultipleVersionsWithDeletes(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Create complex scenario with multiple versions and deletes
	err := builder.Add("key1", "v1_1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "v2_1", 2, false)
	require.NoError(t, err)
	err = builder.Add("key1", "v1_2", 3, false)
	require.NoError(t, err)
	err = builder.Add("key3", "v3_1", 4, false)
	require.NoError(t, err)
	err = builder.Add("key2", "", 5, false) // delete key,true2
	require.NoError(t, err)
	err = builder.Add("key1", "v1_3", 6, false)
	require.NoError(t, err)
	err = builder.Add("key4", "v4_1", 7, false)
	require.NoError(t, err)
	err = builder.Add("key3", "", 8, false) // delete key,true3
	require.NoError(t, err)

	block := builder.Build()

	// Test with transaction that sees everything
	iter := block.NewIterator()
	defer iter.Close()

	var results []struct {
		key     string
		value   string
		deleted bool
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		results = append(results, struct {
			key     string
			value   string
			deleted bool
		}{
			key:     iter.Key(),
			value:   iter.Value(),
			deleted: iter.IsDeleted(),
		})
	}

	// Should see: key1 (latest), key2 (deleted), key3 (deleted), key4
	require.Equal(t, 4, len(results))
	assert.Equal(t, "key1", results[0].key)
	assert.Equal(t, "v1_3", results[0].value)
	assert.False(t, results[0].deleted)

	assert.Equal(t, "key2", results[1].key)
	assert.True(t, results[1].deleted)

	assert.Equal(t, "key3", results[2].key)
	assert.True(t, results[2].deleted)

	assert.Equal(t, "key4", results[3].key)
	assert.Equal(t, "v4_1", results[3].value)
	assert.False(t, results[3].deleted)
}

func TestBlockIteratorTransactionIsolation(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add entries with increasing transaction IDs
	err := builder.Add("key1", "value1", 10, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2", 20, false)
	require.NoError(t, err)
	err = builder.Add("key3", "value3", 30, false)
	require.NoError(t, err)

	block := builder.Build()

	// Iterator with txnID=15 should only see key1
	iter := block.NewIterator()
	iter.(*BlockIterator).SetTxnID(15)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
		assert.Equal(t, "key1", iter.Key())
		assert.Equal(t, "value1", iter.Value())
	}
	assert.Equal(t, 1, count)

	// Iterator with txnID=25 should see key1 and key2
	iter2 := block.NewIterator()
	iter2.(*BlockIterator).SetTxnID(25)
	defer iter2.Close()

	expectedKeys := []string{"key1", "key2"}
	expectedValues := []string{"value1", "value2"}
	count = 0
	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		assert.Less(t, count, len(expectedKeys))
		assert.Equal(t, expectedKeys[count], iter2.Key())
		assert.Equal(t, expectedValues[count], iter2.Value())
		count++
	}
	assert.Equal(t, 2, count)
}

func TestBlockIteratorInterface(t *testing.T) {
	builder := NewBlockBuilder(1024)

	// Add some test data
	err := builder.Add("key1", "value1", 1, false)
	require.NoError(t, err)
	err = builder.Add("key2", "value2", 2, false)
	require.NoError(t, err)

	block := builder.Build()

	// Test that block iterator implements the Iterator interface
	var iter iterator.Iterator = block.NewIterator()
	defer iter.Close()

	// Test interface methods
	iter.SeekToFirst()
	assert.True(t, iter.Valid())
	assert.Equal(t, "key1", iter.Key())
	assert.Equal(t, "value1", iter.Value())

	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, "key2", iter.Key())

	iter.Next()
	assert.False(t, iter.Valid())
}

func BenchmarkBlockIteratorSeekToFirst(b *testing.B) {
	builder := NewBlockBuilder(64 * 1024)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		builder.Add(key, value, uint64(i+1), false)
	}

	block := builder.Build()
	iter := block.NewIterator()
	defer iter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekToFirst()
	}
}

func BenchmarkBlockIteratorSeek(b *testing.B) {
	builder := NewBlockBuilder(64 * 1024)

	// Pre-populate with data
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		builder.Add(key, value, uint64(i+1), false)
	}

	block := builder.Build()
	iter := block.NewIterator()
	defer iter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seekKey := fmt.Sprintf("key_%04d", i%numEntries)
		iter.Seek(seekKey)
	}
}

func BenchmarkBlockIteratorIteration(b *testing.B) {
	builder := NewBlockBuilder(64 * 1024)

	// Pre-populate with data
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		builder.Add(key, value, uint64(i+1), false)
	}

	block := builder.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := block.NewIterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			// Just iterate
		}
		iter.Close()
	}
}
