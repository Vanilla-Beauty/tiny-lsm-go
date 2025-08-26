package sst

import (
	"encoding/binary"
	"hash/crc32"
)

// BlockMeta represents metadata for a block in an SST file
type BlockMeta struct {
	Offset   uint32 // Block offset in the SST file
	FirstKey string // First key in the block
	LastKey  string // Last key in the block
}

// NewBlockMeta creates a new BlockMeta
func NewBlockMeta(offset uint32, firstKey, lastKey string) BlockMeta {
	return BlockMeta{
		Offset:   offset,
		FirstKey: firstKey,
		LastKey:  lastKey,
	}
}

// EncodeBlockMetas encodes a slice of BlockMeta into bytes
// Format for each meta entry:
// | offset(4) | first_key_len(2) | first_key | last_key_len(2) | last_key |
// Overall format:
// | num_entries(4) | meta_entry1 | ... | meta_entryN | checksum(4) |
func EncodeBlockMetas(metas []BlockMeta) []byte {
	if len(metas) == 0 {
		// Return minimal encoding for empty metadata
		result := make([]byte, 8) // 4 bytes for count + 4 bytes for checksum
		binary.LittleEndian.PutUint32(result[0:4], 0)
		checksum := crc32.ChecksumIEEE(result[0:4])
		binary.LittleEndian.PutUint32(result[4:8], checksum)
		return result
	}

	// Calculate total size needed
	totalSize := 4 // num_entries
	for _, meta := range metas {
		totalSize += 4                          // offset
		totalSize += 2 + len(meta.FirstKey)     // first_key_len + first_key
		totalSize += 2 + len(meta.LastKey)      // last_key_len + last_key
	}
	totalSize += 4 // checksum

	result := make([]byte, totalSize)
	pos := 0

	// Write number of entries
	binary.LittleEndian.PutUint32(result[pos:pos+4], uint32(len(metas)))
	pos += 4

	// Write each meta entry
	for _, meta := range metas {
		// Write offset
		binary.LittleEndian.PutUint32(result[pos:pos+4], meta.Offset)
		pos += 4

		// Write first key length and first key
		firstKeyLen := uint16(len(meta.FirstKey))
		binary.LittleEndian.PutUint16(result[pos:pos+2], firstKeyLen)
		pos += 2
		copy(result[pos:pos+len(meta.FirstKey)], meta.FirstKey)
		pos += len(meta.FirstKey)

		// Write last key length and last key
		lastKeyLen := uint16(len(meta.LastKey))
		binary.LittleEndian.PutUint16(result[pos:pos+2], lastKeyLen)
		pos += 2
		copy(result[pos:pos+len(meta.LastKey)], meta.LastKey)
		pos += len(meta.LastKey)
	}

	// Calculate checksum of the metadata (excluding the checksum itself)
	checksum := crc32.ChecksumIEEE(result[0 : pos])
	binary.LittleEndian.PutUint32(result[pos:pos+4], checksum)

	return result
}

// DecodeBlockMetas decodes BlockMeta slice from bytes
func DecodeBlockMetas(data []byte) ([]BlockMeta, error) {
	if len(data) < 8 {
		return nil, ErrInvalidMetadata
	}

	pos := 0

	// Read number of entries
	numEntries := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if numEntries == 0 {
		// Verify checksum for empty metadata
		expectedChecksum := binary.LittleEndian.Uint32(data[4:8])
		actualChecksum := crc32.ChecksumIEEE(data[0:4])
		if expectedChecksum != actualChecksum {
			return nil, ErrInvalidChecksum
		}
		return []BlockMeta{}, nil
	}

	metas := make([]BlockMeta, numEntries)

	// Read each meta entry
	for i := uint32(0); i < numEntries; i++ {
		if pos+4 > len(data) {
			return nil, ErrInvalidMetadata
		}

		// Read offset
		offset := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		// Read first key
		if pos+2 > len(data) {
			return nil, ErrInvalidMetadata
		}
		firstKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(firstKeyLen) > len(data) {
			return nil, ErrInvalidMetadata
		}
		firstKey := string(data[pos : pos+int(firstKeyLen)])
		pos += int(firstKeyLen)

		// Read last key
		if pos+2 > len(data) {
			return nil, ErrInvalidMetadata
		}
		lastKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(lastKeyLen) > len(data) {
			return nil, ErrInvalidMetadata
		}
		lastKey := string(data[pos : pos+int(lastKeyLen)])
		pos += int(lastKeyLen)

		metas[i] = BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		}
	}

	// Verify checksum
	if pos+4 > len(data) {
		return nil, ErrInvalidMetadata
	}
	expectedChecksum := binary.LittleEndian.Uint32(data[pos : pos+4])
	actualChecksum := crc32.ChecksumIEEE(data[0:pos])
	if expectedChecksum != actualChecksum {
		return nil, ErrInvalidChecksum
	}

	return metas, nil
}

// Size returns the encoded size of this BlockMeta
func (bm *BlockMeta) Size() int {
	return 4 + 2 + len(bm.FirstKey) + 2 + len(bm.LastKey)
}

// Contains checks if a key falls within this block's range
func (bm *BlockMeta) Contains(key string) bool {
	return key >= bm.FirstKey && key <= bm.LastKey
}
