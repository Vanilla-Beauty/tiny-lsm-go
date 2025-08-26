package block

import (
	"tiny-lsm-go/pkg/iterator"
)

// BlockIterator implements the Iterator interface for blocks with MVCC support
type BlockIterator struct {
	block      *Block
	index      int
	txnID      uint64
	closed     bool
	aggregated []iterator.Entry // Pre-aggregated MVCC entries
}

// NewBlockIterator creates a new block iterator
func NewBlockIterator(block *Block) *BlockIterator {
	iter := &BlockIterator{
		block:  block,
		index:  -1,
		txnID:  0,
		closed: false,
	}

	// Pre-aggregate MVCC entries
	iter.buildAggregatedEntries()

	return iter
}

// buildAggregatedEntries pre-aggregates all entries by key for MVCC
func (iter *BlockIterator) buildAggregatedEntries() {
	if iter.block.NumEntries() == 0 {
		iter.aggregated = []iterator.Entry{}
		return
	}

	// Group entries by key and keep the best version for each key based on txnID
	keyMap := make(map[string]*iterator.Entry)

	for i := 0; i < iter.block.NumEntries(); i++ {
		entry, err := iter.block.GetEntry(i)
		if err != nil {
			continue
		}

		// Check if this entry is visible to our transaction
		if iter.txnID > 0 && entry.TxnID > iter.txnID {
			continue // Skip entries from future transactions
		}

		// Keep the latest visible version of each key
		if existing, exists := keyMap[entry.Key]; !exists || entry.TxnID > existing.TxnID {
			keyMap[entry.Key] = &entry
		}
	}

	// Convert map to sorted slice
	iter.aggregated = make([]iterator.Entry, 0, len(keyMap))
	for _, entry := range keyMap {
		iter.aggregated = append(iter.aggregated, *entry)
	}

	// Sort by key
	for i := 0; i < len(iter.aggregated); i++ {
		for j := i + 1; j < len(iter.aggregated); j++ {
			if iter.aggregated[i].Key > iter.aggregated[j].Key {
				iter.aggregated[i], iter.aggregated[j] = iter.aggregated[j], iter.aggregated[i]
			}
		}
	}
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *BlockIterator) Valid() bool {
	return !iter.closed && iter.index >= 0 && iter.index < len(iter.aggregated)
}

// Key returns the key of the current entry
func (iter *BlockIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Key
}

// Value returns the value of the current entry
func (iter *BlockIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.aggregated[iter.index].Value
}

// TxnID returns the transaction ID of the current entry
func (iter *BlockIterator) TxnID() uint64 {
	if !iter.Valid() {
		return 0
	}
	return iter.aggregated[iter.index].TxnID
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *BlockIterator) IsDeleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.aggregated[iter.index].Value == ""
}

// Entry returns the current entry
func (iter *BlockIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.aggregated[iter.index]
}

// Next advances the iterator to the next key (with MVCC aggregation)
func (iter *BlockIterator) Next() {
	if iter.closed {
		return
	}

	// Move to next entry
	iter.index++
}

// seekToKey positions the iterator at the first entry with key >= target in aggregated entries
func (iter *BlockIterator) seekToKey(key string) int {
	// Binary search in aggregated entries
	left, right := 0, len(iter.aggregated)

	for left < right {
		mid := (left + right) / 2
		if iter.aggregated[mid].Key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// Seek positions the iterator at the first entry with key >= target
func (iter *BlockIterator) Seek(key string) {
	if iter.closed {
		return
	}

	// Find the first entry >= key in aggregated entries
	iter.index = iter.seekToKey(key)
}

// SeekToFirst positions the iterator at the first entry
func (iter *BlockIterator) SeekToFirst() {
	if iter.closed {
		return
	}

	iter.index = 0
}

// SeekToLast positions the iterator at the last entry
func (iter *BlockIterator) SeekToLast() {
	if iter.closed {
		return
	}

	iter.index = len(iter.aggregated) - 1
}

// GetType returns the iterator type
func (iter *BlockIterator) GetType() iterator.IteratorType {
	return iterator.SSTIteratorType
}

// Close releases any resources held by the iterator
func (iter *BlockIterator) Close() {
	iter.closed = true
	iter.block = nil
}

// SetTxnID sets the transaction ID for visibility checking
func (iter *BlockIterator) SetTxnID(txnID uint64) {
	if iter.txnID != txnID {
		iter.txnID = txnID
		// Re-aggregate entries with new transaction visibility
		iter.buildAggregatedEntries()
		// Reset position
		iter.index = -1
	}
}
