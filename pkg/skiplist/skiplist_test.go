package skiplist

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSkipListBasicOperations(t *testing.T) {
	sl := New()
	assert.True(t, sl.IsEmpty())
	assert.Equal(t, 0, sl.Size())

	// Test Put and Get
	sl.Put("key1", "value1", 1)
	assert.False(t, sl.IsEmpty())
	assert.Equal(t, 1, sl.Size())

	iter := sl.Get("key1", 0)
	require.True(t, iter.Valid())
	assert.Equal(t, "key1", iter.Key())
	assert.Equal(t, "value1", iter.Value())
	assert.Equal(t, uint64(1), iter.TxnID())
	assert.False(t, iter.IsDeleted())
	iter.Close()

	// Test non-existent key
	iter = sl.Get("nonexistent", 0)
	assert.False(t, iter.Valid())
	iter.Close()
}

func TestSkipListMVCC(t *testing.T) {
	sl := New()

	// Add multiple versions of the same key
	sl.Put("key1", "value1", 1)
	sl.Put("key1", "value3", 3)
	sl.Put("key1", "value2", 2)

	// Should see the latest version with txnID=0
	iter := sl.Get("key1", 0)
	require.True(t, iter.Valid())
	assert.Equal(t, "value3", iter.Value())
	assert.Equal(t, uint64(3), iter.TxnID())
	iter.Close()

	// Should see older version with txnID=2
	iter = sl.Get("key1", 2)
	require.True(t, iter.Valid())
	assert.Equal(t, "value2", iter.Value())
	assert.Equal(t, uint64(2), iter.TxnID())
	iter.Close()

	// Should see oldest version with txnID=1
	iter = sl.Get("key1", 1)
	require.True(t, iter.Valid())
	assert.Equal(t, "value1", iter.Value())
	assert.Equal(t, uint64(1), iter.TxnID())
	iter.Close()

	// Should not see any version with txnID=0 (before any transaction)
	iter = sl.Get("key1", 0)
	require.True(t, iter.Valid()) // txnID=0 should see all
	iter.Close()
}

func TestSkipListDelete(t *testing.T) {
	sl := New()

	// Add a key
	sl.Put("key1", "value1", 1)

	// Delete the key
	sl.Delete("key1", 2)

	// Should find the delete marker
	iter := sl.Get("key1", 0)
	require.True(t, iter.Valid())
	assert.True(t, iter.IsDeleted())
	assert.Equal(t, uint64(2), iter.TxnID())
	iter.Close()

	// Transaction snapshot before delete should still see the value
	iter = sl.Get("key1", 1)
	require.True(t, iter.Valid())
	assert.False(t, iter.IsDeleted())
	assert.Equal(t, "value1", iter.Value())
	iter.Close()
}

func TestSkipListIterator(t *testing.T) {
	sl := New()

	// Add keys in non-sorted order
	keys := []string{"banana", "apple", "cherry", "date"}
	values := []string{"yellow", "red", "red", "brown"}

	for i, key := range keys {
		sl.Put(key, values[i], uint64(i+1))
	}

	// Test forward iteration - should be in sorted order
	iter := sl.NewIterator(0)
	defer iter.Close()

	expectedKeys := []string{"apple", "banana", "cherry", "date"}
	expectedValues := []string{"red", "yellow", "red", "brown"}

	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		require.Less(t, i, len(expectedKeys), "Too many entries")
		assert.Equal(t, expectedKeys[i], iter.Key())
		assert.Equal(t, expectedValues[i], iter.Value())
		i++
	}
	assert.Equal(t, len(expectedKeys), i, "Not enough entries")
}

func TestSkipListIteratorSeek(t *testing.T) {
	sl := New()

	// Add some keys
	keys := []string{"a", "c", "e", "g", "i"}
	for i, key := range keys {
		sl.Put(key, fmt.Sprintf("value%d", i), uint64(i+1))
	}

	iter := sl.NewIterator(0)
	defer iter.Close()

	// Seek to existing key
	iter.Seek("e")
	require.True(t, iter.Valid())
	assert.Equal(t, "e", iter.Key())

	// Seek to non-existing key (should find next larger key)
	iter.Seek("d")
	require.True(t, iter.Valid())
	assert.Equal(t, "e", iter.Key())

	// Seek to key larger than all keys
	iter.Seek("z")
	assert.False(t, iter.Valid())

	// Seek to key smaller than all keys
	iter.Seek("")
	require.True(t, iter.Valid())
	assert.Equal(t, "a", iter.Key())
}

func TestSkipListClear(t *testing.T) {
	sl := New()

	// Add some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		sl.Put(key, value, uint64(i+1))
	}

	assert.Equal(t, 10, sl.Size())
	assert.True(t, sl.SizeBytes() > 0)

	// Clear the skip list
	sl.Clear()

	assert.Equal(t, 0, sl.Size())
	assert.Equal(t, 0, sl.SizeBytes())
	assert.True(t, sl.IsEmpty())

	// Verify no entries can be found
	iter := sl.NewIterator(0)
	defer iter.Close()
	iter.SeekToFirst()
	assert.False(t, iter.Valid())
}

func TestSkipListFlush(t *testing.T) {
	sl := New()

	// Add some entries
	expectedEntries := make([]string, 5)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		sl.Put(key, value, uint64(i+1))
		expectedEntries[i] = key
	}

	// Flush entries
	entries := sl.Flush()
	assert.Equal(t, 5, len(entries))

	// Verify entries are in sorted order
	for i, entry := range entries {
		assert.Equal(t, expectedEntries[i], entry.Key)
		assert.Equal(t, fmt.Sprintf("value%d", i), entry.Value)
		assert.Equal(t, uint64(i+1), entry.TxnID)
	}
}

func BenchmarkSkipListPut(b *testing.B) {
	sl := New()

	keys := make([]string, b.N)
	values := make([]string, b.N)

	// Pre-generate keys and values
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key_%d", rand.Int())
		values[i] = fmt.Sprintf("value_%d", i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Put(keys[i], values[i], uint64(i+1))
	}
}

func BenchmarkSkipListGet(b *testing.B) {
	sl := New()
	numKeys := 100000

	// Pre-populate skip list
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		keys[i] = key
		sl.Put(key, fmt.Sprintf("value_%d", i), uint64(i+1))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[rand.Intn(numKeys)]
		iter := sl.Get(key, 0)
		if iter.Valid() {
			_ = iter.Value() // Access the value
		}
		iter.Close()
	}
}

func BenchmarkSkipListIteration(b *testing.B) {
	sl := New()
	numKeys := 10000

	// Pre-populate skip list
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		sl.Put(key, fmt.Sprintf("value_%d", i), uint64(i+1))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := sl.NewIterator(0)
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()

		if count != numKeys {
			b.Fatalf("Expected %d keys, got %d", numKeys, count)
		}
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
