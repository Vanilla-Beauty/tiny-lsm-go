package iterator

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockIterator is a test implementation of Iterator
type MockIterator struct {
	data    []Entry
	current int
	valid   bool
}

func NewMockIterator(entries []Entry) *MockIterator {
	// Sort entries by key for proper iteration order
	sortedEntries := make([]Entry, len(entries))
	copy(sortedEntries, entries)
	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Key < sortedEntries[j].Key
	})

	return &MockIterator{
		data:    sortedEntries,
		current: -1,
		valid:   false,
	}
}

func (m *MockIterator) SeekToFirst() {
	if len(m.data) > 0 {
		m.current = 0
		m.valid = true
	} else {
		m.current = -1
		m.valid = false
	}
}

func (m *MockIterator) Seek(key string) {
	// Find first entry >= key
	for i, entry := range m.data {
		if entry.Key >= key {
			m.current = i
			m.valid = true
			return
		}
	}
	// Key not found and no entry >= key
	m.current = len(m.data)
	m.valid = false
}

func (m *MockIterator) SeekToLast() {
	if len(m.data) > 0 {
		m.current = len(m.data) - 1
		m.valid = true
	} else {
		m.current = -1
		m.valid = false
	}
}

func (m *MockIterator) Next() {
	if m.valid && m.current < len(m.data)-1 {
		m.current++
	} else {
		m.valid = false
		m.current = len(m.data)
	}
}

func (m *MockIterator) Valid() bool {
	return m.valid && m.current >= 0 && m.current < len(m.data)
}

func (m *MockIterator) Key() string {
	if m.Valid() {
		return m.data[m.current].Key
	}
	return ""
}

func (m *MockIterator) Value() string {
	if m.Valid() {
		return m.data[m.current].Value
	}
	return ""
}

func (m *MockIterator) TxnID() uint64 {
	if m.Valid() {
		return m.data[m.current].TxnID
	}
	return 0
}

func (m *MockIterator) IsDeleted() bool {
	if m.Valid() {
		return m.data[m.current].Value == ""
	}
	return false
}

func (m *MockIterator) Entry() Entry {
	if m.Valid() {
		return m.data[m.current]
	}
	return Entry{}
}

func (m *MockIterator) GetType() IteratorType {
	return SkipListIteratorType // Mock iterator pretends to be a skip list iterator
}

func (m *MockIterator) Close() {
	m.valid = false
	m.current = -1
}

func TestEmptyIterator(t *testing.T) {
	iter := NewEmptyIterator()
	defer iter.Close()

	// Should be invalid from start
	assert.False(t, iter.Valid())

	// SeekToFirst on empty should remain invalid
	iter.SeekToFirst()
	assert.False(t, iter.Valid())

	// Seek on empty should remain invalid
	iter.Seek("any_key")
	assert.False(t, iter.Valid())

	// Next on empty should remain invalid
	iter.Next()
	assert.False(t, iter.Valid())

	// Methods should return empty/zero values
	assert.Equal(t, "", iter.Key())
	assert.Equal(t, "", iter.Value())
	assert.Equal(t, uint64(0), iter.TxnID())
	assert.False(t, iter.IsDeleted())
}

func TestMockIteratorBasic(t *testing.T) {
	entries := []Entry{
		{"key1", "value1", 1},
		{"key3", "value3", 3},
		{"key2", "value2", 2},
	}

	iter := NewMockIterator(entries)
	defer iter.Close()

	// Test SeekToFirst
	iter.SeekToFirst()
	require.True(t, iter.Valid())
	assert.Equal(t, "key1", iter.Key())
	assert.Equal(t, "value1", iter.Value())
	assert.Equal(t, uint64(1), iter.TxnID())
	assert.False(t, iter.IsDeleted())

	// Test Next
	iter.Next()
	require.True(t, iter.Valid())
	assert.Equal(t, "key2", iter.Key())
	assert.Equal(t, "value2", iter.Value())

	iter.Next()
	require.True(t, iter.Valid())
	assert.Equal(t, "key3", iter.Key())
	assert.Equal(t, "value3", iter.Value())

	// Move past end
	iter.Next()
	assert.False(t, iter.Valid())
}

func TestMockIteratorSeek(t *testing.T) {
	entries := []Entry{
		{"apple", "red", 1},
		{"banana", "yellow", 2},
		{"cherry", "dark_red", 3},
		{"date", "brown", 4},
	}

	iter := NewMockIterator(entries)
	defer iter.Close()

	// Seek to existing key
	iter.Seek("banana")
	require.True(t, iter.Valid())
	assert.Equal(t, "banana", iter.Key())

	// Seek to non-existing key (should find next larger)
	iter.Seek("blueberry")
	require.True(t, iter.Valid())
	assert.Equal(t, "cherry", iter.Key())

	// Seek before first key
	iter.Seek("a")
	require.True(t, iter.Valid())
	assert.Equal(t, "apple", iter.Key())

	// Seek after last key
	iter.Seek("zebra")
	assert.False(t, iter.Valid())
}

func TestMockIteratorWithDeletes(t *testing.T) {
	entries := []Entry{
		{"key1", "value1", 1},
		{"key2", "", 2}, // deleted
		{"key3", "value3", 3},
	}

	iter := NewMockIterator(entries)
	defer iter.Close()

	// Iterate through all entries including deleted ones
	iter.SeekToFirst()
	assert.True(t, iter.Valid())
	assert.Equal(t, "key1", iter.Key())
	assert.False(t, iter.IsDeleted())

	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, "key2", iter.Key())
	assert.True(t, iter.IsDeleted())

	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, "key3", iter.Key())
	assert.False(t, iter.IsDeleted())

	iter.Next()
	assert.False(t, iter.Valid())
}

func TestMergeIteratorBasic(t *testing.T) {
	// Create multiple iterators with overlapping data
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1_iter1", 1},
		{"key3", "value3_iter1", 3},
		{"key5", "value5_iter1", 5},
	})

	iter2 := NewMockIterator([]Entry{
		{"key2", "value2_iter2", 2},
		{"key4", "value4_iter2", 4},
		{"key6", "value6_iter2", 6},
	})

	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	// Test forward iteration - should be in sorted order
	expectedKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6"}
	expectedValues := []string{"value1_iter1", "value2_iter2", "value3_iter1", "value4_iter2", "value5_iter1", "value6_iter2"}

	count := 0
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		require.Less(t, count, len(expectedKeys), "Too many entries")
		assert.Equal(t, expectedKeys[count], mergeIter.Key())
		assert.Equal(t, expectedValues[count], mergeIter.Value())
		assert.False(t, mergeIter.IsDeleted())
		count++
	}
	assert.Equal(t, len(expectedKeys), count)
}

func TestMergeIteratorOverlapping(t *testing.T) {
	// Create iterators with overlapping keys - merge should pick from first iterator
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1_iter1", 1},
		{"key2", "value2_iter1", 2},
	})

	iter2 := NewMockIterator([]Entry{
		{"key1", "value1_iter2", 3}, // Same key, different value
		{"key3", "value3_iter2", 4},
	})

	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	// Should see key1 from iter1 (first iterator takes precedence)
	mergeIter.SeekToFirst()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key1", mergeIter.Key())
	assert.Equal(t, "value1_iter1", mergeIter.Value())
	assert.Equal(t, uint64(1), mergeIter.TxnID())

	mergeIter.Next()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key2", mergeIter.Key())
	assert.Equal(t, "value2_iter1", mergeIter.Value())

	mergeIter.Next()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key3", mergeIter.Key())
	assert.Equal(t, "value3_iter2", mergeIter.Value())

	mergeIter.Next()
	assert.False(t, mergeIter.Valid())
}

func TestMergeIteratorEmpty(t *testing.T) {
	// Test merge iterator with no iterators
	mergeIter := NewMergeIterator([]Iterator{})
	defer mergeIter.Close()

	mergeIter.SeekToFirst()
	assert.False(t, mergeIter.Valid())

	// Test merge iterator with empty iterators
	iter1 := NewMockIterator([]Entry{})
	iter2 := NewMockIterator([]Entry{})

	mergeIter2 := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter2.Close()

	mergeIter2.SeekToFirst()
	assert.False(t, mergeIter2.Valid())
}

func TestMergeIteratorSingleIterator(t *testing.T) {
	// Test merge iterator with single underlying iterator
	iter := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key2", "value2", 2},
	})

	mergeIter := NewMergeIterator([]Iterator{iter})
	defer mergeIter.Close()

	// Should behave like the single iterator
	mergeIter.SeekToFirst()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key1", mergeIter.Key())

	mergeIter.Next()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key2", mergeIter.Key())

	mergeIter.Next()
	assert.False(t, mergeIter.Valid())
}

func TestMergeIteratorSeek(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"apple", "red", 1},
		{"cherry", "dark_red", 3},
	})

	iter2 := NewMockIterator([]Entry{
		{"banana", "yellow", 2},
		{"date", "brown", 4},
	})

	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	// Seek to existing key
	mergeIter.Seek("banana")
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "banana", mergeIter.Key())

	// Seek to non-existing key (should find next larger)
	mergeIter.Seek("blueberry")
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "cherry", mergeIter.Key())

	// Seek before all keys
	mergeIter.Seek("a")
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "apple", mergeIter.Key())

	// Seek after all keys
	mergeIter.Seek("zebra")
	assert.False(t, mergeIter.Valid())
}

func TestMergeIteratorWithDeletes(t *testing.T) {
	iter1 := NewMockIterator([]Entry{
		{"key1", "value1", 1},
		{"key3", "", 3}, // deleted
	})

	iter2 := NewMockIterator([]Entry{
		{"key2", "value2", 2},
		{"key4", "value4", 4},
	})

	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	// Count total entries including deleted ones
	totalEntries := 0
	deletedEntries := 0
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		totalEntries++
		if mergeIter.IsDeleted() {
			deletedEntries++
		}
	}

	assert.Equal(t, 4, totalEntries)
	assert.Equal(t, 1, deletedEntries)
}

func TestMergeIteratorManyIterators(t *testing.T) {
	const numIterators = 10
	const entriesPerIterator = 20

	iterators := make([]Iterator, numIterators)
	allExpectedKeys := make([]string, 0, numIterators*entriesPerIterator)

	// Create multiple iterators with interleaved keys
	for i := 0; i < numIterators; i++ {
		entries := make([]Entry, entriesPerIterator)
		for j := 0; j < entriesPerIterator; j++ {
			key := fmt.Sprintf("key_%02d_%02d", i, j)
			value := fmt.Sprintf("value_%d_%d", i, j)
			entries[j] = Entry{key, value, uint64(i*entriesPerIterator + j + 1)}
			allExpectedKeys = append(allExpectedKeys, key)
		}
		iterators[i] = NewMockIterator(entries)
	}

	// Sort expected keys for comparison
	sort.Strings(allExpectedKeys)

	mergeIter := NewMergeIterator(iterators)
	defer mergeIter.Close()

	// Verify all keys are returned in sorted order
	actualKeys := make([]string, 0, len(allExpectedKeys))
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		actualKeys = append(actualKeys, mergeIter.Key())
	}

	assert.Equal(t, allExpectedKeys, actualKeys)
}

func TestMergeIteratorMVCCPrecedence(t *testing.T) {
	// Test that newer transactions (later iterators) take precedence for same key
	iter1 := NewMockIterator([]Entry{
		{"key1", "old_value", 1},
	})

	iter2 := NewMockIterator([]Entry{
		{"key1", "new_value", 2}, // Same key, newer transaction
	})

	// iter2 should take precedence since it comes first in the list
	mergeIter := NewMergeIterator([]Iterator{iter2, iter1})
	defer mergeIter.Close()

	mergeIter.SeekToFirst()
	require.True(t, mergeIter.Valid())
	assert.Equal(t, "key1", mergeIter.Key())
	assert.Equal(t, "new_value", mergeIter.Value())
	assert.Equal(t, uint64(2), mergeIter.TxnID())

	// Only one entry should be visible
	mergeIter.Next()
	assert.False(t, mergeIter.Valid())
}

func TestMergeIteratorStability(t *testing.T) {
	// Test that merge iterator behaves consistently across multiple iterations
	iter1 := NewMockIterator([]Entry{
		{"a", "1", 1},
		{"c", "3", 3},
	})

	iter2 := NewMockIterator([]Entry{
		{"b", "2", 2},
		{"d", "4", 4},
	})

	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	// First pass
	keys1 := make([]string, 0)
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		keys1 = append(keys1, mergeIter.Key())
	}

	// Second pass
	keys2 := make([]string, 0)
	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		keys2 = append(keys2, mergeIter.Key())
	}

	assert.Equal(t, keys1, keys2)
	assert.Equal(t, []string{"a", "b", "c", "d"}, keys1)
}

func BenchmarkMergeIteratorTwoIterators(b *testing.B) {
	// Create two large iterators
	entries1 := make([]Entry, 1000)
	entries2 := make([]Entry, 1000)

	for i := 0; i < 1000; i++ {
		entries1[i] = Entry{fmt.Sprintf("key_1_%04d", i), fmt.Sprintf("value1_%d", i), uint64(i)}
		entries2[i] = Entry{fmt.Sprintf("key_2_%04d", i), fmt.Sprintf("value2_%d", i), uint64(i + 1000)}
	}

	iter1 := NewMockIterator(entries1)
	iter2 := NewMockIterator(entries2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
		count := 0
		for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
			count++
		}
		mergeIter.Close()

		if count != 2000 {
			b.Fatalf("Expected 2000 entries, got %d", count)
		}
	}
}

func BenchmarkMergeIteratorManyIterators(b *testing.B) {
	const numIterators = 10
	const entriesPerIterator = 100

	iterators := make([]Iterator, numIterators)
	for i := 0; i < numIterators; i++ {
		entries := make([]Entry, entriesPerIterator)
		for j := 0; j < entriesPerIterator; j++ {
			key := fmt.Sprintf("key_%02d_%02d", i, j)
			value := fmt.Sprintf("value_%d_%d", i, j)
			entries[j] = Entry{key, value, uint64(i*entriesPerIterator + j)}
		}
		iterators[i] = NewMockIterator(entries)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergeIter := NewMergeIterator(iterators)
		count := 0
		for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
			count++
		}
		mergeIter.Close()

		if count != numIterators*entriesPerIterator {
			b.Fatalf("Expected %d entries, got %d", numIterators*entriesPerIterator, count)
		}
	}
}

func BenchmarkMergeIteratorSeek(b *testing.B) {
	entries1 := make([]Entry, 500)
	entries2 := make([]Entry, 500)

	for i := 0; i < 500; i++ {
		entries1[i] = Entry{fmt.Sprintf("key_%04d", i*2), fmt.Sprintf("value1_%d", i), uint64(i)}
		entries2[i] = Entry{fmt.Sprintf("key_%04d", i*2+1), fmt.Sprintf("value2_%d", i), uint64(i + 500)}
	}

	iter1 := NewMockIterator(entries1)
	iter2 := NewMockIterator(entries2)
	mergeIter := NewMergeIterator([]Iterator{iter1, iter2})
	defer mergeIter.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seekKey := fmt.Sprintf("key_%04d", i%1000)
		mergeIter.Seek(seekKey)
	}
}
