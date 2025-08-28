package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectIteratorBasic(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key3", "value3", 3},
	})

	iter2 := NewMockIterator([]Entry{
		{"key2", "value2", 2},
		{"key4", "value4", 4},
		{"key5", "", 5},
	})

	selectIter := NewSelectIterator([]Iterator{iter1, iter2})
	defer selectIter.Close()

	// Test SeekToFirst
	selectIter.SeekToFirst()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key1", selectIter.Key())
	assert.Equal(t, "value1", selectIter.Value())
	assert.Equal(t, uint64(1), selectIter.TxnID())

	// Test Next
	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key2", selectIter.Key())
	assert.Equal(t, "value2", selectIter.Value())

	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key3", selectIter.Key())
	assert.Equal(t, "value3", selectIter.Value())

	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key4", selectIter.Key())
	assert.Equal(t, "value4", selectIter.Value())

	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key5", selectIter.Key())
	assert.Equal(t, "", selectIter.Value())
	assert.True(t, selectIter.IsDeleted())

	// Move past end
	selectIter.Next()
	assert.False(t, selectIter.Valid())
}

func TestSelectIteratorOverlappingKeys(t *testing.T) {
	// Test that when keys overlap, the iterator with lower index is selected
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1_iter1", 1},
		{"key2", "value2_iter1", 2},
	})

	iter2 := NewMockIterator([]Entry{
		{"key1", "value1_iter2", 3}, // Same key as iter1
		{"key3", "value3_iter2", 4},
	})

	selectIter := NewSelectIterator([]Iterator{iter1, iter2})
	defer selectIter.Close()

	// Should select key1 from iter1 (lower index) even though both have the same key
	selectIter.SeekToFirst()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key1", selectIter.Key())
	assert.Equal(t, "value1_iter1", selectIter.Value())
	assert.Equal(t, uint64(1), selectIter.TxnID())

	// After calling Next, both iterators with key1 should be advanced
	// So we should see key2 from iter1, not key1 from iter2
	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key2", selectIter.Key())
	assert.Equal(t, "value2_iter1", selectIter.Value())

	// Next should be key3 from iter2
	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key3", selectIter.Key())
	assert.Equal(t, "value3_iter2", selectIter.Value())

	selectIter.Next()
	assert.False(t, selectIter.Valid())
}

func TestSelectIteratorSeek(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"apple", "red", 1},
		{"cherry", "dark_red", 3},
	})

	iter2 := NewMockIterator([]Entry{
		{"banana", "yellow", 2},
		{"date", "brown", 4},
	})

	selectIter := NewSelectIterator([]Iterator{iter1, iter2})
	defer selectIter.Close()

	// Seek to existing key
	selectIter.Seek("banana")
	require.True(t, selectIter.Valid())
	assert.Equal(t, "banana", selectIter.Key())

	// Seek to non-existing key (should find next larger)
	selectIter.Seek("blueberry")
	require.True(t, selectIter.Valid())
	assert.Equal(t, "cherry", selectIter.Key())

	// Seek before all keys
	selectIter.Seek("a")
	require.True(t, selectIter.Valid())
	assert.Equal(t, "apple", selectIter.Key())

	// Seek after all keys
	selectIter.Seek("zebra")
	assert.False(t, selectIter.Valid())
}

func TestSelectIteratorEmpty(t *testing.T) {
	// Test select iterator with no iterators
	selectIter := NewSelectIterator([]Iterator{})
	defer selectIter.Close()

	selectIter.SeekToFirst()
	assert.False(t, selectIter.Valid())

	// Test select iterator with empty iterators
	iter1 := NewMockIterator([]Entry{})
	iter2 := NewMockIterator([]Entry{})

	selectIter2 := NewSelectIterator([]Iterator{iter1, iter2})
	defer selectIter2.Close()

	selectIter2.SeekToFirst()
	assert.False(t, selectIter2.Valid())
}

func TestSelectIteratorSingleIterator(t *testing.T) {
	// Test select iterator with single underlying iterator
	iter := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
	})

	selectIter := NewSelectIterator([]Iterator{iter})
	defer selectIter.Close()

	// Should behave like the single iterator
	selectIter.SeekToFirst()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key1", selectIter.Key())

	selectIter.Next()
	require.True(t, selectIter.Valid())
	assert.Equal(t, "key2", selectIter.Key())

	selectIter.Next()
	assert.False(t, selectIter.Valid())
}

func TestSelectIteratorWithDeletes(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key3", "", 3}, // deleted
	})

	iter2 := NewMockIterator([]Entry{
		{"key2", "value2", 2},
		{"key4", "value4", 4},
	})

	selectIter := NewSelectIterator([]Iterator{iter1, iter2})
	defer selectIter.Close()

	// Count total entries including deleted ones
	totalEntries := 0
	deletedEntries := 0
	for selectIter.SeekToFirst(); selectIter.Valid(); selectIter.Next() {
		totalEntries++
		if selectIter.IsDeleted() {
			deletedEntries++
		}
	}

	assert.Equal(t, 4, totalEntries)
	assert.Equal(t, 1, deletedEntries)
}
