package sst

import (
	"encoding/binary"
	"os"
	"tiny-lsm-go/pkg/block"
	"tiny-lsm-go/pkg/cache"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/iterator"
	"tiny-lsm-go/pkg/utils"
)

// SST represents a Sorted String Table file
// File structure:
// | Block Section | Meta Section | Bloom Filter | Meta Offset(4) | Bloom Offset(4) | Min TxnID(8) | Max TxnID(8) |
type SST struct {
	sstID       uint64             // SST file ID
	file        *os.File           // File handle
	filePath    string             // File path
	metaEntries []BlockMeta        // Block metadata
	metaOffset  uint32             // Offset of metadata section
	bloomOffset uint32             // Offset of bloom filter
	firstKey    string             // First key in SST
	lastKey     string             // Last key in SST
	bloomFilter *utils.BloomFilter // Bloom filter for keys
	blockCache  *cache.BlockCache  // Block cache
	minTxnID    uint64             // Minimum transaction ID
	maxTxnID    uint64             // Maximum transaction ID
	fileSize    int64              // Total file size
}

// OpenSST opens an SST file with a file manager (test compatibility)
func OpenSST(sstID uint64, filePath string, blockCache *cache.BlockCache, fileManager *utils.FileManager) (*SST, error) {
	return Open(sstID, filePath, blockCache)
}

// Open opens an existing SST file
func Open(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	fileSize := fileInfo.Size()
	if fileSize < 24 { // Min size: 4+4+8+8 = 24 bytes for offsets and transaction IDs
		file.Close()
		return nil, utils.ErrInvalidSSTFile
	}

	sst := &SST{
		sstID:      sstID,
		file:       file,
		filePath:   filePath,
		blockCache: blockCache,
		fileSize:   fileSize,
	}

	// Read footer: MetaOffset(4) + BloomOffset(4) + MinTxnID(8) + MaxTxnID(8)
	footerSize := int64(24)
	footer := make([]byte, footerSize)
	_, err = file.ReadAt(footer, fileSize-footerSize)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Parse footer
	sst.metaOffset = binary.LittleEndian.Uint32(footer[0:4])
	sst.bloomOffset = binary.LittleEndian.Uint32(footer[4:8])
	sst.minTxnID = binary.LittleEndian.Uint64(footer[8:16])
	sst.maxTxnID = binary.LittleEndian.Uint64(footer[16:24])

	// Validate offsets
	if int64(sst.metaOffset) >= fileSize || int64(sst.bloomOffset) >= fileSize {
		file.Close()
		return nil, utils.ErrCorruptedFile
	}

	// Read and decode metadata
	metaSize := sst.bloomOffset - sst.metaOffset
	metaData := make([]byte, metaSize)
	_, err = file.ReadAt(metaData, int64(sst.metaOffset))
	if err != nil {
		file.Close()
		return nil, err
	}

	sst.metaEntries, err = DecodeBlockMetas(metaData)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Set first and last keys
	if len(sst.metaEntries) > 0 {
		sst.firstKey = sst.metaEntries[0].FirstKey
		sst.lastKey = sst.metaEntries[len(sst.metaEntries)-1].LastKey
	}

	// Read bloom filter if it exists
	bloomSize := int64(sst.metaOffset) - int64(sst.bloomOffset)
	if bloomSize > 0 {
		bloomData := make([]byte, bloomSize)
		_, err = file.ReadAt(bloomData, int64(sst.bloomOffset))
		if err != nil {
			file.Close()
			return nil, err
		}

		sst.bloomFilter = utils.DeserializeBloomFilter(bloomData)
	}

	return sst, nil
}

// Close closes the SST file
func (sst *SST) Close() error {
	if sst.file != nil {
		return sst.file.Close()
	}
	return nil
}

func (sst *SST) MetaOffset() int64 {
	return int64(sst.metaOffset)
}

// ID returns the SST ID
func (sst *SST) ID() uint64 {
	return sst.sstID
}

// FirstKey returns the first key in the SST
func (sst *SST) FirstKey() string {
	return sst.firstKey
}

// LastKey returns the last key in the SST
func (sst *SST) LastKey() string {
	return sst.lastKey
}

// Size returns the file size
func (sst *SST) Size() int64 {
	return sst.fileSize
}

// NumBlocks returns the number of blocks in the SST
func (sst *SST) NumBlocks() int {
	return len(sst.metaEntries)
}

// GetSSTID returns the SST ID (alias for ID)
func (sst *SST) GetSSTID() uint64 {
	return sst.sstID
}

// GetFirstKey returns the first key in the SST (alias for FirstKey)
func (sst *SST) GetFirstKey() string {
	return sst.firstKey
}

// GetLastKey returns the last key in the SST (alias for LastKey)
func (sst *SST) GetLastKey() string {
	return sst.lastKey
}

// GetNumBlocks returns the number of blocks in the SST (alias for NumBlocks)
func (sst *SST) GetNumBlocks() int {
	return len(sst.metaEntries)
}

// TxnRange returns the transaction ID range
func (sst *SST) TxnRange() (uint64, uint64) {
	return sst.minTxnID, sst.maxTxnID
}

// ReadBlock reads a block by index
func (sst *SST) ReadBlock(blockIdx int) (*block.Block, error) {
	if blockIdx < 0 || blockIdx >= len(sst.metaEntries) {
		return nil, utils.ErrBlockNotFound
	}

	// Try cache first
	if sst.blockCache != nil {
		if cached := sst.blockCache.Get(sst.sstID, uint64(blockIdx)); cached != nil {
			return cached, nil
		}
	}

	// Calculate block size
	meta := sst.metaEntries[blockIdx]
	var blockSize uint32
	if blockIdx == len(sst.metaEntries)-1 {
		// Last block: size = metaOffset - block offset
		blockSize = sst.metaOffset - meta.Offset
	} else {
		// Other blocks: size = next block offset - current block offset
		blockSize = sst.metaEntries[blockIdx+1].Offset - meta.Offset
	}

	// Read block data
	blockData := make([]byte, blockSize)
	_, err := sst.file.ReadAt(blockData, int64(meta.Offset))
	if err != nil {
		return nil, err
	}

	// Decode block
	blk, err := block.NewBlock(blockData)
	if err != nil {
		return nil, err
	}

	// Cache the block
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.sstID, uint64(blockIdx), blk)
	}

	return blk, nil
}

// FindBlockIndex finds the block index that might contain the key
func (sst *SST) FindBlockIndex(key string) int {
	// First check bloom filter (only for keys that should be in the SST)
	if sst.bloomFilter != nil && key >= sst.firstKey && key <= sst.lastKey && !sst.bloomFilter.Contains(key) {
		return -1 // Key definitely not in this SST
	}

	// If key is after the last key, it's not in any block
	if key > sst.lastKey {
		return -1
	}

	// If key is before the first key, it should go to the first block
	if key < sst.firstKey {
		return 0
	}

	// Binary search through block metadata
	left, right := 0, len(sst.metaEntries)
	for left < right {
		mid := (left + right) / 2
		meta := sst.metaEntries[mid]

		if key < meta.FirstKey {
			right = mid
		} else if key > meta.LastKey {
			left = mid + 1
		} else {
			return mid // Found the block that contains this key range
		}
	}

	if left < len(sst.metaEntries) {
		return left
	}
	return -1
}

// FindBlockIdx is an alias for FindBlockIndex (test compatibility)
func (sst *SST) FindBlockIdx(key string) int {
	return sst.FindBlockIndex(key)
}

// Get searches for a key in the SST
func (sst *SST) Get(key string, txnID uint64) (iterator.Iterator, error) {
	blockIdx := sst.FindBlockIndex(key)
	if blockIdx == -1 {
		return iterator.NewEmptyIterator(), nil
	}

	blk, err := sst.ReadBlock(blockIdx)
	if err != nil {
		return nil, err
	}

	blockIter := blk.NewIterator()
	blockIter.Seek(key)

	// Return iterator positioned at the key
	return &SSTIterator{
		sst:       sst,
		blockIdx:  blockIdx,
		blockIter: blockIter,
		txnID:     txnID,
	}, nil
}

// NewIterator creates a new iterator for the SST
func (sst *SST) NewIterator(txnID uint64) iterator.Iterator {
	return &SSTIterator{
		sst:       sst,
		blockIdx:  0,
		blockIter: nil, // Will be initialized on first access
		txnID:     txnID,
	}
}

// Delete deletes the SST file
func (sst *SST) Delete() error {
	if sst.file != nil {
		sst.file.Close()
		sst.file = nil
	}
	return os.Remove(sst.filePath)
}

// SSTBuilder builds SST files
type SSTBuilder struct {
	blockBuilder *block.BlockBuilder
	metas        []BlockMeta
	data         []byte
	blockSize    int
	bloomFilter  *utils.BloomFilter
	firstKey     string
	lastKey      string
	minTxnID     uint64
	maxTxnID     uint64
	hasBloom     bool
}

// NewSSTBuilder creates a new SST builder
func NewSSTBuilder(blockSize int, hasBloom bool) *SSTBuilder {
	var bloomFilter *utils.BloomFilter
	if hasBloom {
		cfg := config.GetGlobalConfig()
		bloomFilter = utils.NewBloomFilter(
			cfg.BloomFilter.ExpectedSize,
			cfg.BloomFilter.ExpectedErrorRate,
		)
	}

	return &SSTBuilder{
		blockBuilder: block.NewBlockBuilder(blockSize),
		metas:        make([]BlockMeta, 0),
		data:         make([]byte, 0),
		blockSize:    blockSize,
		bloomFilter:  bloomFilter,
		minTxnID:     ^uint64(0), // Max uint64
		maxTxnID:     0,
		hasBloom:     hasBloom,
	}
}

func (builder *SSTBuilder) GetDataSize() int {
	return builder.blockBuilder.DataSize() + len(builder.data)
}

// Add adds a key-value pair to the SST
func (builder *SSTBuilder) Add(key, value string, txnID uint64) error {
	// Update first and last keys
	if builder.firstKey == "" {
		builder.firstKey = key
	}

	// Update transaction ID range
	if txnID < builder.minTxnID {
		builder.minTxnID = txnID
	}
	if txnID > builder.maxTxnID {
		builder.maxTxnID = txnID
	}

	// Add to bloom filter
	if builder.bloomFilter != nil {
		builder.bloomFilter.Add(key)
	}

	forceFlush := key == builder.lastKey

	// Try to add to current block
	err := builder.blockBuilder.Add(key, value, txnID, forceFlush)
	if err == nil {
		builder.lastKey = key
		return nil // Successfully added
	}

	// Current block is full, finish it and start a new one
	err = builder.finishBlock()
	if err != nil {
		return err
	}

	// Add to new block
	builder.firstKey = key
	builder.lastKey = key

	return builder.blockBuilder.Add(key, value, txnID, false)
}

// finishBlock completes the current block and starts a new one
func (builder *SSTBuilder) finishBlock() error {
	if builder.blockBuilder.IsEmpty() {
		return nil // No block to finish
	}

	// Build the block
	blk := builder.blockBuilder.Build()
	blockData := blk.Data()

	// Create metadata for this block
	meta := NewBlockMeta(
		uint32(len(builder.data)),
		builder.blockBuilder.FirstKey(),
		builder.blockBuilder.LastKey(),
	)
	builder.metas = append(builder.metas, meta)

	// Append block data
	builder.data = append(builder.data, blockData...)

	// Create new block builder
	builder.blockBuilder = block.NewBlockBuilder(builder.blockSize)

	return nil
}

// Build builds the SST file
func (builder *SSTBuilder) Build(sstID uint64, filePath string, blockCache *cache.BlockCache) (*SST, error) {
	// Finish the last block
	if !builder.blockBuilder.IsEmpty() {
		if err := builder.finishBlock(); err != nil {
			return nil, err
		}
	}

	if len(builder.metas) == 0 {
		return nil, utils.ErrEmptySST
	}

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	// Write blocks data
	_, err = file.Write(builder.data)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Record metadata offset
	metaOffset := uint32(len(builder.data))

	// Write metadata
	metaData := EncodeBlockMetas(builder.metas)
	_, err = file.Write(metaData)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Record bloom filter offset
	bloomOffset := uint32(len(builder.data) + len(metaData))

	// Write bloom filter
	if builder.bloomFilter != nil {
		bloomData := builder.bloomFilter.Serialize()
		_, err = file.Write(bloomData)
		if err != nil {
			file.Close()
			os.Remove(filePath)
			return nil, err
		}
	}

	// Write footer
	footer := make([]byte, 24)
	binary.LittleEndian.PutUint32(footer[0:4], metaOffset)
	binary.LittleEndian.PutUint32(footer[4:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], builder.minTxnID)
	binary.LittleEndian.PutUint64(footer[16:24], builder.maxTxnID)

	_, err = file.Write(footer)
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	// Sync file
	err = file.Sync()
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, err
	}

	file.Close()

	// Open the built SST
	return Open(sstID, filePath, blockCache)
}

// SSTIterator implements iterator for SST
type SSTIterator struct {
	sst       *SST
	blockIdx  int
	blockIter iterator.Iterator
	txnID     uint64
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *SSTIterator) Valid() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return true
	}
	return false
}

// Key returns the key of the current entry
func (iter *SSTIterator) Key() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Key()
	}
	return ""
}

// Value returns the value of the current entry
func (iter *SSTIterator) Value() string {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Value()
	}
	return ""
}

// TxnID returns the transaction ID of the current entry
func (iter *SSTIterator) TxnID() uint64 {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.TxnID()
	}
	return 0
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *SSTIterator) IsDeleted() bool {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.IsDeleted()
	}
	return false
}

// Entry returns the current entry
func (iter *SSTIterator) Entry() iterator.Entry {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		return iter.blockIter.Entry()
	}
	return iterator.Entry{}
}

// Next advances the iterator to the next entry
func (iter *SSTIterator) Next() {
	if iter.blockIter != nil && iter.blockIter.Valid() {
		iter.blockIter.Next()
		if !iter.blockIter.Valid() {
			// Move to next block
			iter.moveToNextBlock()
		}
	}
}

// moveToNextBlock moves to the next block
func (iter *SSTIterator) moveToNextBlock() {
	iter.blockIdx++
	if iter.blockIdx >= iter.sst.NumBlocks() {
		// No more blocks
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		return
	}

	// Load next block
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// Seek positions the iterator at the first entry with key >= target
func (iter *SSTIterator) Seek(key string) bool {
	blockIdx := iter.sst.FindBlockIndex(key)
	if blockIdx == -1 {
		// Key not found in any block
		if iter.blockIter != nil {
			iter.blockIter.Close()
			iter.blockIter = nil
		}
		iter.blockIdx = iter.sst.NumBlocks()
		return false
	}

	iter.blockIdx = blockIdx
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return false
	}

	iter.blockIter = blk.NewIterator()
	found := iter.blockIter.Seek(key)

	// If not found in this block, move to next block
	if !iter.blockIter.Valid() {
		iter.moveToNextBlock()
	}

	return found
}

// SeekToFirst positions the iterator at the first entry
func (iter *SSTIterator) SeekToFirst() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = 0
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(0)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToFirst()
}

// SeekToLast positions the iterator at the last entry
func (iter *SSTIterator) SeekToLast() {
	if iter.sst.NumBlocks() == 0 {
		return
	}

	iter.blockIdx = iter.sst.NumBlocks() - 1
	if iter.blockIter != nil {
		iter.blockIter.Close()
	}

	blk, err := iter.sst.ReadBlock(iter.blockIdx)
	if err != nil {
		iter.blockIter = nil
		return
	}

	iter.blockIter = blk.NewIterator()
	iter.blockIter.SeekToLast()
}

// GetType returns the iterator type
func (iter *SSTIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases resources held by the iterator
func (iter *SSTIterator) Close() {
	if iter.blockIter != nil {
		iter.blockIter.Close()
		iter.blockIter = nil
	}
}
