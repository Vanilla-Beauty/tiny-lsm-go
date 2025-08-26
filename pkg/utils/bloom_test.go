package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloomFilterBasicOperations(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01) // 1000 items, 1% false positive rate

	// Test basic insertion and lookup
	testKeys := []string{"apple", "banana", "cherry", "date", "elderberry"}

	// Insert keys
	for _, key := range testKeys {
		bf.Add(key)
	}

	// All inserted keys should be found
	for _, key := range testKeys {
		assert.True(t, bf.Contains(key), "Key %s should be in bloom filter", key)
	}

	// Non-inserted keys should mostly not be found (may have false positives)
	nonInsertedKeys := []string{"grape", "kiwi", "mango", "orange", "papaya"}
	falsePositives := 0
	for _, key := range nonInsertedKeys {
		if bf.Contains(key) {
			falsePositives++
		}
	}

	// Should have very few false positives for this small set
	assert.LessOrEqual(t, falsePositives, 2, "Too many false positives")
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	// Empty bloom filter should not contain anything
	testKeys := []string{"key1", "key2", "key3"}
	for _, key := range testKeys {
		assert.False(t, bf.Contains(key), "Empty bloom filter should not contain %s", key)
	}
}

func TestBloomFilterSingleElement(t *testing.T) {
	bf := NewBloomFilter(1, 0.1)

	// Add single element
	bf.Add("single_key")

	// Should contain the added key
	assert.True(t, bf.Contains("single_key"))

	// For a very small bloom filter with 1 item, there might be false positives
	// Test with multiple keys to see if most are correctly identified as not present
	testKeys := []string{"other_key", "another_key", "yet_another", "final_key"}
	falsePositives := 0
	for _, key := range testKeys {
		if bf.Contains(key) {
			falsePositives++
		}
	}
	
	// For a bloom filter with only 1 item, we should have mostly correct negatives
	// but allow some false positives due to the small size
	assert.LessOrEqual(t, falsePositives, len(testKeys)/2, "Too many false positives for single element bloom filter")
}

func TestBloomFilterSize(t *testing.T) {
	tests := []struct {
		numItems    int
		falsePositiveRate float64
		minBits     int
	}{
		{100, 0.1, 400},    // Should need at least ~480 bits
		{1000, 0.01, 9000}, // Should need at least ~9585 bits
		{10, 0.5, 10},      // Very high false positive rate, minimal bits
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.numItems, test.falsePositiveRate)
		assert.GreaterOrEqual(t, int(bf.Size()), test.minBits,
			"Bloom filter should have at least %d bits for %d items with %.2f false positive rate",
			test.minBits, test.numItems, test.falsePositiveRate)
	}
}

func TestBloomFilterHashFunctions(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	// Should have appropriate number of hash functions
	// For 0.1 false positive rate, should have around 3-4 hash functions
	hashCount := bf.NumHashes()
	assert.Greater(t, hashCount, uint32(0), "Should have at least 1 hash function")
	assert.LessOrEqual(t, hashCount, uint32(10), "Should not have too many hash functions")
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	numItems := 1000
	targetFPRate := 0.05 // 5% false positive rate

	bf := NewBloomFilter(numItems, targetFPRate)

	// Insert known keys
	insertedKeys := make(map[string]bool)
	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("key_%d", i)
		bf.Add(key)
		insertedKeys[key] = true
	}

	// Test with many random keys that were not inserted
	testCount := 10000
	falsePositives := 0

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < testCount; i++ {
		// Generate random key that we know wasn't inserted
		testKey := fmt.Sprintf("random_key_%d_%d", rand.Int(), rand.Int())
		if !insertedKeys[testKey] && bf.Contains(testKey) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(testCount)

	// Allow some margin of error, but should be reasonably close to target
	assert.LessOrEqual(t, actualFPRate, targetFPRate*2,
		"False positive rate %.4f is too high (target: %.4f)", actualFPRate, targetFPRate)

	t.Logf("Target FP rate: %.4f, Actual FP rate: %.4f, False positives: %d/%d",
		targetFPRate, actualFPRate, falsePositives, testCount)
}

func TestBloomFilterSerialization(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	// Add some test data
	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range testKeys {
		bf.Add(key)
	}

	// Serialize
	data := bf.Serialize()
	require.NotEmpty(t, data, "Encoded data should not be empty")

	// Deserialize
	bf2 := DeserializeBloomFilter(data)
	require.NotNil(t, bf2, "Should be able to decode bloom filter")

	// Verify properties are preserved
	assert.Equal(t, bf.Size(), bf2.Size())
	assert.Equal(t, bf.NumHashes(), bf2.NumHashes())

	// Verify all original keys are still found
	for _, key := range testKeys {
		assert.True(t, bf2.Contains(key), "Decoded bloom filter should contain %s", key)
	}
}

func TestBloomFilterInvalidParameters(t *testing.T) {
	// Test invalid number of items
	bf := NewBloomFilter(0, 0.1)
	assert.NotNil(t, bf, "Should create bloom filter even with 0 items")

	// Test invalid false positive rate
	bf = NewBloomFilter(100, 0)
	assert.NotNil(t, bf, "Should create bloom filter even with 0 false positive rate")

	bf = NewBloomFilter(100, 1.0)
	assert.NotNil(t, bf, "Should create bloom filter even with 100% false positive rate")

	// These should not panic but create reasonable defaults
	bf = NewBloomFilter(100, -0.1)
	assert.NotNil(t, bf)

	bf = NewBloomFilter(100, 1.5)
	assert.NotNil(t, bf)
}

func TestBloomFilterLargeDataset(t *testing.T) {
	numItems := 10000
	bf := NewBloomFilter(numItems, 0.01)

	// Insert large number of items
	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("large_dataset_key_%06d", i)
		bf.Add(key)
	}

	// Verify random sampling of inserted keys
	sampleSize := 1000
	for i := 0; i < sampleSize; i++ {
		idx := rand.Intn(numItems)
		key := fmt.Sprintf("large_dataset_key_%06d", idx)
		assert.True(t, bf.Contains(key), "Large dataset key %s should be found", key)
	}

	// Test false positive rate with non-inserted keys
	falsePositives := 0
	testCount := 1000
	for i := 0; i < testCount; i++ {
		key := fmt.Sprintf("non_inserted_key_%06d", i+numItems)
		if bf.Contains(key) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(testCount)
	assert.LessOrEqual(t, actualFPRate, 0.05, // Allow up to 5% false positive rate
		"False positive rate %.4f is too high for large dataset", actualFPRate)
}

func TestBloomFilterDuplicateInsertions(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	key := "duplicate_key"

	// Insert same key multiple times
	bf.Add(key)
	bf.Add(key)
	bf.Add(key)

	// Should still be found
	assert.True(t, bf.Contains(key))

	// Behavior should be same as single insertion
	bf2 := NewBloomFilter(100, 0.1)
	bf2.Add(key)

	// Both should behave similarly for other keys (though not necessarily identical due to randomization)
	testKey := "test_key"
	result1 := bf.Contains(testKey)
	result2 := bf2.Contains(testKey)

	// They should both be false for this uninserted key
	assert.False(t, result1)
	assert.False(t, result2)
}

func TestBloomFilterSpecialStrings(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	// Test special strings
	specialKeys := []string{
		"",                    // empty string
		" ",                   // space
		"\n",                  // newline
		"\t",                  // tab
		"key with spaces",     // spaces
		"key\x00with\x00null", // null characters
		"😀🦊🌟",                // unicode
		strings.Repeat("a", 1000), // very long string
	}

	// Insert all special keys
	for _, key := range specialKeys {
		bf.Add(key)
	}

	// Verify all can be found
	for _, key := range specialKeys {
		assert.True(t, bf.Contains(key), "Special key should be found: %q", key)
	}
}

func TestBloomFilterClear(t *testing.T) {
	bf := NewBloomFilter(100, 0.1)

	// Add some keys
	testKeys := []string{"key1", "key2", "key3"}
	for _, key := range testKeys {
		bf.Add(key)
	}

	// Verify keys are present
	for _, key := range testKeys {
		assert.True(t, bf.Contains(key))
	}

	// Clear the bloom filter
	bf.Clear()

	// Keys should no longer be found
	for _, key := range testKeys {
		assert.False(t, bf.Contains(key), "Key %s should not be found after clear", key)
	}
}

func TestBloomFilterDecodingErrors(t *testing.T) {
	// Test decoding invalid data
	invalidData := [][]byte{
		{}, // empty data
		{1, 2, 3}, // too short
		{0xFF, 0xFF, 0xFF, 0xFF}, // invalid format
	}

	for i, data := range invalidData {
		bf := DeserializeBloomFilter(data)
		assert.Nil(t, bf, "Should return nil for invalid data %d: %v", i, data)
	}
}

func TestBloomFilterOptimalParameters(t *testing.T) {
	tests := []struct {
		numItems int
		fpRate   float64
	}{
		{100, 0.1},
		{1000, 0.01},
		{10000, 0.001},
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.numItems, test.fpRate)

		// Verify bit count and hash count are reasonable
		bitCount := int(bf.Size())
		hashCount := int(bf.NumHashes())

		assert.Greater(t, bitCount, 0, "Should have positive bit count")
		assert.Greater(t, hashCount, 0, "Should have positive hash count")
		assert.LessOrEqual(t, hashCount, 20, "Hash count should be reasonable")

		// Bit count should grow with number of items and shrink with higher false positive rate
		expectedMinBits := int(float64(test.numItems) * 3) // Very rough estimate
		assert.GreaterOrEqual(t, bitCount, expectedMinBits,
			"Bit count should be at least %d for %d items", expectedMinBits, test.numItems)
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("benchmark_key_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(keys[i])
	}
}

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)

	// Pre-populate with keys
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("existing_key_%d", i)
		bf.Add(key)
	}

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("test_key_%d", i%10000)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(keys[i])
	}
}

func BenchmarkBloomFilterEncodeDecode(b *testing.B) {
	bf := NewBloomFilter(1000, 0.01)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("benchmark_key_%d", i)
		bf.Add(key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := bf.Serialize()
		bf2 := DeserializeBloomFilter(data)
		if bf2 == nil {
			b.Fatal("Failed to deserialize")
		}
	}
}
