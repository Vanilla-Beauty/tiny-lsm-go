package block

import (
	"encoding/binary"
	"fmt"
	"tiny-lsm-go/pkg/iterator"
)

// Block represents a data block in SST files
type Block struct {
	data      []byte           // raw block data
	offsets   []uint16         // offsets of each key-value pair
	entries   []iterator.Entry // cached entries for faster access
	blockSize int              // target block size
}

// BlockBuilder helps build a block incrementally
type BlockBuilder struct {
	data      []byte
	offsets   []uint16
	blockSize int
	firstKey  string
	lastKey   string
}

// NewBlockBuilder creates a new block builder with the specified target size
func NewBlockBuilder(blockSize int) *BlockBuilder {
	return &BlockBuilder{
		data:      make([]byte, 0),
		offsets:   make([]uint16, 0),
		blockSize: blockSize,
	}
}

// Add adds a key-value entry to the block being built
func (bb *BlockBuilder) Add(key, value string, txnID uint64, forceFlush bool) error {
	// Calculate the size needed for this entry
	entrySize := 8 + 8 + 1 + 2 + len(key) + 2 + len(value) // txnID + flags + keyLen + key + valueLen + value

	// Check if adding this entry would exceed the block size
	if !forceFlush && len(bb.data)+entrySize+len(bb.offsets)*2+2 > bb.blockSize && len(bb.offsets) > 0 {
		return fmt.Errorf("block size limit exceeded")
	}

	// Record the offset of this entry
	bb.offsets = append(bb.offsets, uint16(len(bb.data)))

	// Encode the entry
	// Format: [txnID:8][keyLen:2][key][valueLen:2][value]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, txnID)
	bb.data = append(bb.data, buf...)

	// Key length and key
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(key)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(key)...)

	// Value length and value
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(value)))
	bb.data = append(bb.data, buf[:2]...)
	bb.data = append(bb.data, []byte(value)...)

	// Update first and last keys
	if bb.firstKey == "" {
		bb.firstKey = key
	}
	bb.lastKey = key

	return nil
}

// EstimatedSize returns the estimated size of the current block
func (bb *BlockBuilder) EstimatedSize() int {
	return len(bb.data) + len(bb.offsets)*2 + 2 // data + offsets + offset count
}

// IsEmpty returns true if the block builder is empty
func (bb *BlockBuilder) IsEmpty() bool {
	return len(bb.offsets) == 0
}

// FirstKey returns the first key in the block
func (bb *BlockBuilder) FirstKey() string {
	return bb.firstKey
}

// LastKey returns the last key in the block
func (bb *BlockBuilder) LastKey() string {
	return bb.lastKey
}

// Build finalizes and returns the built block
func (bb *BlockBuilder) Build() *Block {
	if len(bb.offsets) == 0 {
		// Empty block: just the number of entries (0)
		emptyData := make([]byte, 2)
		binary.LittleEndian.PutUint16(emptyData, 0) // 0 entries
		return &Block{
			data:      emptyData,
			offsets:   []uint16{},
			entries:   []iterator.Entry{},
			blockSize: bb.blockSize,
		}
	}

	// Build the final data with offsets at the end
	finalData := make([]byte, 0, len(bb.data)+len(bb.offsets)*2+2)
	finalData = append(finalData, bb.data...)

	// Append offsets
	for _, offset := range bb.offsets {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, offset)
		finalData = append(finalData, buf...)
	}

	// Append number of entries
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(len(bb.offsets)))
	finalData = append(finalData, buf...)

	// Parse entries for caching
	entries, _ := parseEntries(finalData, bb.offsets)

	return &Block{
		data:      finalData,
		offsets:   bb.offsets,
		entries:   entries,
		blockSize: bb.blockSize,
	}
}

// parseEntries parses the raw data into entries
func parseEntries(data []byte, offsets []uint16) ([]iterator.Entry, error) {
	entries := make([]iterator.Entry, len(offsets))

	for i, offset := range offsets {
		if int(offset)+8+1+2 > len(data) {
			return nil, fmt.Errorf("invalid offset")
		}

		pos := int(offset)

		// Read transaction ID
		txnID := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		// Read key length and key
		keyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(keyLen) > len(data) {
			return nil, fmt.Errorf("invalid key length")
		}

		key := string(data[pos : pos+int(keyLen)])
		pos += int(keyLen)

		// Read value length and value
		if pos+2 > len(data) {
			return nil, fmt.Errorf("invalid value length position")
		}

		valueLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(valueLen) > len(data) {
			return nil, fmt.Errorf("invalid value length")
		}

		value := string(data[pos : pos+int(valueLen)])

		entries[i] = iterator.Entry{
			Key:   key,
			Value: value,
			TxnID: txnID,
		}
	}

	return entries, nil
}

// NewBlock creates a block from raw data
func NewBlock(data []byte) (*Block, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid block data: too short")
	}

	// Read number of entries from the end
	numEntries := binary.LittleEndian.Uint16(data[len(data)-2:])

	if len(data) < int(numEntries)*2+2 {
		return nil, fmt.Errorf("invalid block data: insufficient data for offsets")
	}

	// Read offsets
	offsets := make([]uint16, numEntries)
	offsetStart := len(data) - 2 - int(numEntries)*2

	for i := 0; i < int(numEntries); i++ {
		offset := offsetStart + i*2
		offsets[i] = binary.LittleEndian.Uint16(data[offset : offset+2])
	}

	// Parse entries
	actualData := data[:offsetStart]
	entries, err := parseEntries(actualData, offsets)
	if err != nil {
		return nil, err
	}

	return &Block{
		data:    data,
		offsets: offsets,
		entries: entries,
	}, nil
}

// NumEntries returns the number of entries in the block
func (b *Block) NumEntries() int {
	return len(b.offsets)
}

// GetEntry returns the entry at the specified index
func (b *Block) GetEntry(index int) (iterator.Entry, error) {
	if index < 0 || index >= len(b.entries) {
		return iterator.Entry{}, fmt.Errorf("index out of range")
	}
	return b.entries[index], nil
}

// Data returns the raw block data
func (b *Block) Data() []byte {
	return b.data
}

// Size returns the size of the block in bytes
func (b *Block) Size() int {
	return len(b.data)
}

// FirstKey returns the first key in the block
func (b *Block) FirstKey() string {
	if len(b.entries) == 0 {
		return ""
	}
	return b.entries[0].Key
}

// LastKey returns the last key in the block
func (b *Block) LastKey() string {
	if len(b.entries) == 0 {
		return ""
	}
	return b.entries[len(b.entries)-1].Key
}

// FindEntry finds the index of the first entry with key >= target
// Returns -1 if no such entry exists
func (b *Block) FindEntry(key string) int {
	// Binary search
	left, right := 0, len(b.entries)

	for left < right {
		mid := (left + right) / 2
		if iterator.CompareKeys(b.entries[mid].Key, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	if left >= len(b.entries) {
		return -1
	}

	return left
}

// GetNumEntries returns the number of entries in the block (alias for NumEntries)
func (b *Block) GetNumEntries() int {
	return len(b.offsets)
}

// GetValue searches for a key and returns its value with MVCC support
// Returns (value, found, error)
func (b *Block) GetValue(key string, txnID uint64) (string, bool) {
	// Collect all entries with the target key
	var matchingEntries []iterator.Entry
	for _, entry := range b.entries {
		if entry.Key == key {
			matchingEntries = append(matchingEntries, entry)
		}
	}

	if len(matchingEntries) == 0 {
		return "", false // Key not found
	}

	// Debug: print all matching entries
	// fmt.Printf("GetValue(%s, %d): found %d entries\n", key, txnID, len(matchingEntries))
	// for _, entry := range matchingEntries {
	//	fmt.Printf("  TxnID=%d, Value=%s\n", entry.TxnID, entry.Value)
	// }

	// If txnID is 0, return the latest version
	if txnID == 0 {
		// Find the entry with the highest transaction ID
		latestEntry := matchingEntries[0]
		for _, entry := range matchingEntries {
			if entry.TxnID > latestEntry.TxnID {
				latestEntry = entry
			}
		}

		return latestEntry.Value, true
	}

	// Find the latest version that is <= txnID
	var bestEntry *iterator.Entry
	for i, entry := range matchingEntries {
		if entry.TxnID <= txnID {
			if bestEntry == nil || entry.TxnID > bestEntry.TxnID {
				bestEntry = &matchingEntries[i]
			}
		}
	}

	// Debug: print best entry
	// if bestEntry != nil {
	//	fmt.Printf("  Best entry: TxnID=%d, Value=%s\n", bestEntry.TxnID, bestEntry.Value)
	// }

	if bestEntry != nil {
		return bestEntry.Value, true
	}

	return "", false // No suitable version found
}

// NewIterator creates a new iterator for this block
func (b *Block) NewIterator() iterator.Iterator {
	return NewBlockIterator(b)
}
