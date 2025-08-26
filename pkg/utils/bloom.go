package utils

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter represents a Bloom filter for probabilistic membership testing
type BloomFilter struct {
	bitset    []byte
	size      uint32 // size of the bit array
	numHashes uint32 // number of hash functions
	numItems  uint32 // number of items added
}

// NewBloomFilter creates a new Bloom filter with the specified parameters
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	// Calculate optimal bit array size and number of hash functions
	size := uint32(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))
	numHashes := uint32(float64(size) * math.Log(2) / float64(expectedItems))

	// Ensure minimum values
	if size == 0 {
		size = 1
	}
	if numHashes == 0 {
		numHashes = 1
	}

	// Round up to the nearest byte
	sizeBytes := (size + 7) / 8

	return &BloomFilter{
		bitset:    make([]byte, sizeBytes),
		size:      size,
		numHashes: numHashes,
		numItems:  0,
	}
}

// NewBloomFilterFromData creates a Bloom filter from existing data
func NewBloomFilterFromData(data []byte, numHashes uint32) *BloomFilter {
	return &BloomFilter{
		bitset:    data,
		size:      uint32(len(data) * 8),
		numHashes: numHashes,
		numItems:  0, // We don't track this for deserialized filters
	}
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(key string) {
	hashes := bf.getHashes([]byte(key))
	
	for i := uint32(0); i < bf.numHashes; i++ {
		bit := hashes[i] % bf.size
		byteIndex := bit / 8
		bitIndex := bit % 8
		bf.bitset[byteIndex] |= (1 << bitIndex)
	}
	
	bf.numItems++
}

// Contains checks if an item might be in the Bloom filter
// Returns false if the item is definitely not in the set
// Returns true if the item might be in the set (could be a false positive)
func (bf *BloomFilter) Contains(key string) bool {
	hashes := bf.getHashes([]byte(key))
	
	for i := uint32(0); i < bf.numHashes; i++ {
		bit := hashes[i] % bf.size
		byteIndex := bit / 8
		bitIndex := bit % 8
		
		if (bf.bitset[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	
	return true
}

// getHashes generates multiple hash values for a key using double hashing
func (bf *BloomFilter) getHashes(key []byte) []uint32 {
	h1 := bf.hash1(key)
	h2 := bf.hash2(key)
	
	hashes := make([]uint32, bf.numHashes)
	for i := uint32(0); i < bf.numHashes; i++ {
		hashes[i] = h1 + (i * h2)
	}
	
	return hashes
}

// hash1 computes the first hash function using FNV-1a
func (bf *BloomFilter) hash1(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

// hash2 computes the second hash function using a simple polynomial hash
func (bf *BloomFilter) hash2(key []byte) uint32 {
	var hash uint32 = 5381
	for _, b := range key {
		hash = ((hash << 5) + hash) + uint32(b)
	}
	return hash
}

// Size returns the size of the bit array in bits
func (bf *BloomFilter) Size() uint32 {
	return bf.size
}

// NumHashes returns the number of hash functions used
func (bf *BloomFilter) NumHashes() uint32 {
	return bf.numHashes
}

// NumItems returns the number of items added to the filter
func (bf *BloomFilter) NumItems() uint32 {
	return bf.numItems
}

// Data returns the raw bit array data
func (bf *BloomFilter) Data() []byte {
	return bf.bitset
}

// EstimateFalsePositiveRate estimates the current false positive rate
func (bf *BloomFilter) EstimateFalsePositiveRate() float64 {
	if bf.numItems == 0 {
		return 0
	}
	
	// Calculate the probability that a bit is still 0
	p := math.Pow(1.0-1.0/float64(bf.size), float64(bf.numHashes)*float64(bf.numItems))
	
	// Calculate false positive probability
	return math.Pow(1.0-p, float64(bf.numHashes))
}

// Clear resets the Bloom filter
func (bf *BloomFilter) Clear() {
	for i := range bf.bitset {
		bf.bitset[i] = 0
	}
	bf.numItems = 0
}

// Serialize serializes the Bloom filter to bytes
func (bf *BloomFilter) Serialize() []byte {
	// Format: [size:4][numHashes:4][numItems:4][bitset...]
	result := make([]byte, 12+len(bf.bitset))
	
	binary.LittleEndian.PutUint32(result[0:4], bf.size)
	binary.LittleEndian.PutUint32(result[4:8], bf.numHashes)
	binary.LittleEndian.PutUint32(result[8:12], bf.numItems)
	copy(result[12:], bf.bitset)
	
	return result
}

// Deserialize creates a Bloom filter from serialized bytes
func DeserializeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 12 {
		return nil
	}
	
	size := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	numItems := binary.LittleEndian.Uint32(data[8:12])
	bitset := make([]byte, len(data)-12)
	copy(bitset, data[12:])
	
	return &BloomFilter{
		bitset:    bitset,
		size:      size,
		numHashes: numHashes,
		numItems:  numItems,
	}
}

// Union combines this Bloom filter with another (both must have same parameters)
func (bf *BloomFilter) Union(other *BloomFilter) error {
	if bf.size != other.size || bf.numHashes != other.numHashes {
		return ErrIncompatibleBloomFilters
	}
	
	for i := range bf.bitset {
		bf.bitset[i] |= other.bitset[i]
	}
	
	// Note: numItems is not accurate after union
	bf.numItems = bf.numItems + other.numItems
	
	return nil
}

// Clone creates a copy of the Bloom filter
func (bf *BloomFilter) Clone() *BloomFilter {
	newBitset := make([]byte, len(bf.bitset))
	copy(newBitset, bf.bitset)
	
	return &BloomFilter{
		bitset:    newBitset,
		size:      bf.size,
		numHashes: bf.numHashes,
		numItems:  bf.numItems,
	}
}
