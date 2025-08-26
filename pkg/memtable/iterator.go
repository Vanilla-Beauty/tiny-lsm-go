package memtable

import (
	"container/heap"
	"tiny-lsm-go/pkg/iterator"
)

// MemTableIterator iterates over all entries in a MemTable (current + frozen tables)
// It uses a min-heap to merge iterators from multiple skip lists in sorted order
type MemTableIterator struct {
	memTable *MemTable
	txnID    uint64
	heap     *IteratorHeap
	current  *iterator.Entry
	// valid    bool
	closed bool
}

// NewMemTableIterator creates a new iterator for the MemTable
func NewMemTableIterator(memTable *MemTable, txnID uint64) *MemTableIterator {
	iter := &MemTableIterator{
		memTable: memTable,
		txnID:    txnID,
		heap:     NewIteratorHeap(),
		closed:   false,
	}

	// Initialize iterators for all tables
	iter.initializeIterators()
	iter.advance()

	return iter
}

// initializeIterators sets up iterators for current and frozen tables
func (iter *MemTableIterator) initializeIterators() {
	// Add iterator for current table
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID)
	currentIter.SeekToFirst()
	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0, // Current table has ID 0
		})
	} else {
		currentIter.Close()
	}
	iter.memTable.currentMutex.RUnlock()

	// Add iterators for frozen tables
	iter.memTable.frozenMutex.RLock()
	for i, frozenTable := range iter.memTable.frozenTables {
		frozenIter := frozenTable.NewIterator(iter.txnID)
		frozenIter.SeekToFirst()
		if frozenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: frozenIter,
				TableID:  i + 1, // Frozen tables start from ID 1
			})
		} else {
			frozenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

// advance moves to the next valid entry, handling duplicates and transaction visibility
func (iter *MemTableIterator) advance() {
	iter.current = nil

	if iter.closed || iter.heap.Len() == 0 {
		return
	}

	var lastKey string
	var bestEntry *iterator.Entry
	var bestTxnID uint64

	// Process entries until we find a unique key with the highest visible transaction ID
	for iter.heap.Len() > 0 {
		item := heap.Pop(iter.heap).(*HeapItem)
		entry := item.Iterator.Entry()

		// If this is a new key, we found our result
		if bestEntry == nil || entry.Key != lastKey {
			// Save the previous best entry if we had one
			if bestEntry != nil {
				iter.current = bestEntry
				// Put back the current item since it's for a different key
				heap.Push(iter.heap, item)
				return
			}
			bestEntry = &entry
			bestTxnID = entry.TxnID
			lastKey = entry.Key
		} else {
			// Same key - check if this version is better (higher transaction ID)
			if entry.TxnID > bestTxnID && (iter.txnID == 0 || entry.TxnID <= iter.txnID) {
				bestEntry = &entry
				bestTxnID = entry.TxnID
			}
		}

		// Advance this iterator
		item.Iterator.Next()
		if item.Iterator.Valid() {
			heap.Push(iter.heap, item)
		} else {
			item.Iterator.Close()
		}
	}

	// Set the final result
	if bestEntry != nil {
		iter.current = bestEntry
		// iter.valid = !
	}
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *MemTableIterator) Valid() bool {
	return !iter.closed && iter.current != nil
}

// Key returns the key of the current entry
func (iter *MemTableIterator) Key() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Key
}

// Value returns the value of the current entry
func (iter *MemTableIterator) Value() string {
	if !iter.Valid() {
		return ""
	}
	return iter.current.Value
}

// TxnID returns the transaction ID of the current entry
func (iter *MemTableIterator) TxnID() uint64 {
	if !iter.Valid() {
		return 0
	}
	return iter.current.TxnID
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *MemTableIterator) IsDeleted() bool {
	if !iter.Valid() {
		return false
	}
	return iter.current.Value == ""
}

// Entry returns the current entry
func (iter *MemTableIterator) Entry() iterator.Entry {
	if !iter.Valid() {
		return iterator.Entry{}
	}
	return *iter.current
}

// Next advances the iterator to the next entry
func (iter *MemTableIterator) Next() {
	if iter.closed {
		return
	}
	iter.advance()
}

// Seek positions the iterator at the first entry with key >= target
func (iter *MemTableIterator) Seek(key string) {
	if iter.closed {
		return
	}

	// Close all current iterators and re-initialize
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()

	// Re-initialize iterators and seek to target key
	iter.seekToKey(key)
	iter.advance()
}

// seekToKey initializes iterators and seeks them to the target key
func (iter *MemTableIterator) seekToKey(key string) {
	// Add iterator for current table
	iter.memTable.currentMutex.RLock()
	currentIter := iter.memTable.currentTable.NewIterator(iter.txnID)
	currentIter.Seek(key)
	if currentIter.Valid() {
		heap.Push(iter.heap, &HeapItem{
			Iterator: currentIter,
			TableID:  0,
		})
	} else {
		currentIter.Close()
	}
	iter.memTable.currentMutex.RUnlock()

	// Add iterators for frozen tables
	iter.memTable.frozenMutex.RLock()
	for i, frozenTable := range iter.memTable.frozenTables {
		frozenIter := frozenTable.NewIterator(iter.txnID)
		frozenIter.Seek(key)
		if frozenIter.Valid() {
			heap.Push(iter.heap, &HeapItem{
				Iterator: frozenIter,
				TableID:  i + 1,
			})
		} else {
			frozenIter.Close()
		}
	}
	iter.memTable.frozenMutex.RUnlock()
}

// SeekToFirst positions the iterator at the first entry
func (iter *MemTableIterator) SeekToFirst() {
	if iter.closed {
		return
	}

	// Close all current iterators and re-initialize
	iter.closeAllIterators()
	iter.heap = NewIteratorHeap()

	iter.initializeIterators()
	iter.advance()
}

// SeekToLast positions the iterator at the last entry
func (iter *MemTableIterator) SeekToLast() {
	if iter.closed {
		return
	}

	// For simplicity, we'll seek to first and iterate to last
	// This is not the most efficient but works correctly
	iter.SeekToFirst()

	var lastEntry *iterator.Entry
	for iter.Valid() {
		entry := iter.Entry()
		lastEntry = &entry
		iter.Next()
	}

	if lastEntry != nil {
		iter.current = lastEntry
	}
}

// GetType returns the iterator type
func (iter *MemTableIterator) GetType() iterator.IteratorType {
	return iterator.HeapIteratorType
}

// Close releases any resources held by the iterator
func (iter *MemTableIterator) Close() {
	if iter.closed {
		return
	}

	iter.closeAllIterators()
	iter.closed = true
	iter.current = nil
}

// closeAllIterators closes all iterators in the heap
func (iter *MemTableIterator) closeAllIterators() {
	for iter.heap.Len() > 0 {
		item := heap.Pop(iter.heap).(*HeapItem)
		item.Iterator.Close()
	}
}

// HeapItem represents an item in the iterator heap
type HeapItem struct {
	Iterator iterator.Iterator
	TableID  int // 0 for current table, 1+ for frozen tables
}

// IteratorHeap implements a min-heap of iterators ordered by their current key
type IteratorHeap []*HeapItem

// NewIteratorHeap creates a new iterator heap
func NewIteratorHeap() *IteratorHeap {
	h := make(IteratorHeap, 0)
	heap.Init(&h)
	return &h
}

// Len returns the number of items in the heap
func (h IteratorHeap) Len() int { return len(h) }

// Less compares two heap items
func (h IteratorHeap) Less(i, j int) bool {
	keyI := h[i].Iterator.Key()
	keyJ := h[j].Iterator.Key()

	if keyI != keyJ {
		return keyI < keyJ
	}

	// Same key - prioritize by transaction ID (higher first), then table ID (lower first for newer tables)
	txnI := h[i].Iterator.TxnID()
	txnJ := h[j].Iterator.TxnID()

	if txnI != txnJ {
		return txnI > txnJ // Higher transaction ID first
	}

	return h[i].TableID < h[j].TableID // Lower table ID (newer) first
}

// Swap swaps two items in the heap
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an item to the heap
func (h *IteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapItem))
}

// Pop removes and returns the minimum item from the heap
func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
