package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcatIteratorBasic(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
	})

	iter2 := NewMockIterator([]Entry{
		{"key3", "value3", 3},
		{"key4", "value4", 4},
	})

	concatIter := NewConcatIterator([]Iterator{iter1, iter2})
	defer concatIter.Close()

	// Test SeekToFirst
	concatIter.SeekToFirst()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key1", concatIter.Key())
	assert.Equal(t, "value1", concatIter.Value())
	assert.Equal(t, uint64(1), concatIter.TxnID())

	// Test Next through first iterator
	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key2", concatIter.Key())
	assert.Equal(t, "value2", concatIter.Value())

	// Next should move to second iterator
	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key3", concatIter.Key())
	assert.Equal(t, "value3", concatIter.Value())

	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key4", concatIter.Key())
	assert.Equal(t, "value4", concatIter.Value())

	// Move past end
	concatIter.Next()
	assert.False(t, concatIter.Valid())
}

func TestConcatIteratorSeek(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"apple", "red", 1},
		{"banana", "yellow", 2},
	})

	iter2 := NewMockIterator([]Entry{
		{"cherry", "dark_red", 3},
		{"date", "brown", 4},
	})

	concatIter := NewConcatIterator([]Iterator{iter1, iter2})
	defer concatIter.Close()

	// Seek to key in first iterator
	concatIter.Seek("apple")
	require.True(t, concatIter.Valid())
	assert.Equal(t, "apple", concatIter.Key())

	// Seek to key in second iterator
	concatIter.Seek("cherry")
	require.True(t, concatIter.Valid())
	assert.Equal(t, "cherry", concatIter.Key())

	// Seek to non-existing key that would be in first iterator
	concatIter.Seek("apricot")
	require.True(t, concatIter.Valid())
	assert.Equal(t, "banana", concatIter.Key())

	// Seek to non-existing key that would be in second iterator
	concatIter.Seek("citrus")
	require.True(t, concatIter.Valid())
	assert.Equal(t, "date", concatIter.Key())

	// Seek past all keys
	concatIter.Seek("zucchini")
	assert.False(t, concatIter.Valid())
}

func TestConcatIteratorEmpty(t *testing.T) {
	// Test concat iterator with no iterators
	concatIter := NewConcatIterator([]Iterator{})
	defer concatIter.Close()

	concatIter.SeekToFirst()
	assert.False(t, concatIter.Valid())

	// Test concat iterator with empty iterators
	iter1 := NewMockIterator([]Entry{})
	iter2 := NewMockIterator([]Entry{})

	concatIter2 := NewConcatIterator([]Iterator{iter1, iter2})
	defer concatIter2.Close()

	concatIter2.SeekToFirst()
	assert.False(t, concatIter2.Valid())
}

func TestConcatIteratorSingleIterator(t *testing.T) {
	// Test concat iterator with single underlying iterator
	iter := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
	})

	concatIter := NewConcatIterator([]Iterator{iter})
	defer concatIter.Close()

	// Should behave like the single iterator
	concatIter.SeekToFirst()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key1", concatIter.Key())

	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key2", concatIter.Key())

	concatIter.Next()
	assert.False(t, concatIter.Valid())
}

func TestConcatIteratorWithDeletes(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "", 2}, // deleted
	})

	iter2 := NewMockIterator([]Entry{
		{"key3", "value3", 3},
		{"key4", "value4", 4},
	})

	concatIter := NewConcatIterator([]Iterator{iter1, iter2})
	defer concatIter.Close()

	// Test iteration includes deleted entries
	concatIter.SeekToFirst()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key1", concatIter.Key())
	assert.False(t, concatIter.IsDeleted())

	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key2", concatIter.Key())
	assert.True(t, concatIter.IsDeleted())

	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key3", concatIter.Key())
	assert.False(t, concatIter.IsDeleted())

	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key4", concatIter.Key())
	assert.False(t, concatIter.IsDeleted())

	concatIter.Next()
	assert.False(t, concatIter.Valid())
}

func TestConcatIteratorSeekToLast(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
	})

	iter2 := NewMockIterator([]Entry{
		{"key3", "value3", 3},
		{"key4", "value4", 4},
	})

	concatIter := NewConcatIterator([]Iterator{iter1, iter2})
	defer concatIter.Close()

	concatIter.SeekToLast()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key4", concatIter.Key())
	assert.Equal(t, "value4", concatIter.Value())

	// Move to previous (not directly supported, but can test by seeking to first and then moving)
	concatIter.SeekToFirst()
	concatIter.Next()
	concatIter.Next()
	concatIter.Next()
	require.True(t, concatIter.Valid())
	assert.Equal(t, "key4", concatIter.Key())
}
