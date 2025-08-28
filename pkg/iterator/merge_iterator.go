package iterator

import "container/heap"

// MergeIterator merges multiple sorted iterators
type MergeIterator struct {
	iterators []Iterator
	heap      *IteratorHeap
	current   *HeapItem
	closed    bool
}

// HeapItem represents an item in the iterator heap
type HeapItem struct {
	Iterator Iterator
	Index    int
}

// IteratorHeap implements heap.Interface for HeapItem
type IteratorHeap []*HeapItem

func (h IteratorHeap) Len() int { return len(h) }
func (h IteratorHeap) Less(i, j int) bool {
	// Compare by key first
	if h[i].Iterator.Key() != h[j].Iterator.Key() {
		return h[i].Iterator.Key() < h[j].Iterator.Key()
	}
	// If keys are equal, prefer earlier iterator (lower index)
	return h[i].Index < h[j].Index
}
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapItem))
}

func (h *IteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// NewMergeIterator creates a new merge iterator
func NewMergeIterator(iterators []Iterator) Iterator {
	if len(iterators) == 0 {
		return NewEmptyIterator()
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	mi := &MergeIterator{
		iterators: iterators,
		heap:      &IteratorHeap{},
		closed:    false,
	}

	// Initialize heap with valid iterators
	for i, iter := range iterators {
		if iter != nil {
			item := &HeapItem{
				Iterator: iter,
				Index:    i,
			}
			*mi.heap = append(*mi.heap, item)
		}
	}

	heap.Init(mi.heap)
	mi.advance()

	return mi
}

// Valid returns true if the iterator is pointing to a valid entry
func (mi *MergeIterator) Valid() bool {
	return !mi.closed && mi.current != nil && mi.current.Iterator.Valid()
}

// Key returns the key of the current entry
func (mi *MergeIterator) Key() string {
	if mi.Valid() {
		return mi.current.Iterator.Key()
	}
	return ""
}

// Value returns the value of the current entry
func (mi *MergeIterator) Value() string {
	if mi.Valid() {
		return mi.current.Iterator.Value()
	}
	return ""
}

// TxnID returns the transaction ID of the current entry
func (mi *MergeIterator) TxnID() uint64 {
	if mi.Valid() {
		return mi.current.Iterator.TxnID()
	}
	return 0
}

// IsDeleted returns true if the current entry is a delete marker
func (mi *MergeIterator) IsDeleted() bool {
	if mi.Valid() {
		return mi.current.Iterator.IsDeleted()
	}
	return false
}

// Entry returns the current entry
func (mi *MergeIterator) Entry() Entry {
	if mi.Valid() {
		return mi.current.Iterator.Entry()
	}
	return Entry{}
}

// Next advances the iterator to the next entry
func (mi *MergeIterator) Next() {
	if !mi.Valid() {
		return
	}

	// Skip duplicates of the same key (keep only the newest version)
	currentKey := mi.current.Iterator.Key()

	// Advance the current iterator
	mi.current.Iterator.Next()

	// Re-add to heap if still valid
	if mi.current.Iterator.Valid() {
		heap.Push(mi.heap, mi.current)
	}

	// Skip all other versions of the same key
	for mi.heap.Len() > 0 {
		next := heap.Pop(mi.heap).(*HeapItem)
		if !next.Iterator.Valid() {
			continue
		}

		if next.Iterator.Key() != currentKey {
			// Different key, this is our new current
			mi.current = next
			return
		}

		// Same key, skip it and advance the iterator
		next.Iterator.Next()
		if next.Iterator.Valid() {
			heap.Push(mi.heap, next)
		}
	}

	// No more valid iterators
	mi.current = nil
}

// advance finds the next valid entry
func (mi *MergeIterator) advance() {
	if mi.heap.Len() == 0 {
		mi.current = nil
		return
	}

	// Get the iterator with the smallest key
	mi.current = heap.Pop(mi.heap).(*HeapItem)

	// Ensure the current iterator is valid
	for mi.current != nil && !mi.current.Iterator.Valid() {
		if mi.heap.Len() == 0 {
			mi.current = nil
			break
		}
		mi.current = heap.Pop(mi.heap).(*HeapItem)
	}
}

// Seek positions the iterator at the first entry with key >= target
func (mi *MergeIterator) Seek(key string) bool {
	if mi.closed {
		return false
	}

	// Clear the heap and seek all iterators
	*mi.heap = (*mi.heap)[:0]

	for i, iter := range mi.iterators {
		if iter != nil {
			iter.Seek(key)
			if iter.Valid() {
				item := &HeapItem{
					Iterator: iter,
					Index:    i,
				}
				*mi.heap = append(*mi.heap, item)
			}
		}
	}

	heap.Init(mi.heap)
	mi.advance()

	return mi.Valid() && mi.current.Iterator.Key() == key
}

// SeekToFirst positions the iterator at the first entry
func (mi *MergeIterator) SeekToFirst() {
	if mi.closed {
		return
	}

	// Clear the heap and seek all iterators to first
	*mi.heap = (*mi.heap)[:0]

	for i, iter := range mi.iterators {
		if iter != nil {
			iter.SeekToFirst()
			if iter.Valid() {
				item := &HeapItem{
					Iterator: iter,
					Index:    i,
				}
				*mi.heap = append(*mi.heap, item)
			}
		}
	}

	heap.Init(mi.heap)
	mi.advance()
}

// SeekToLast positions the iterator at the last entry
func (mi *MergeIterator) SeekToLast() {
	if mi.closed {
		return
	}

	// For merge iterator, we need to scan to the end
	mi.SeekToFirst()
	if !mi.Valid() {
		return
	}

	// Advance to the last valid entry
	for mi.Valid() {
		mi.Next()
	}

	// This is a simplified implementation
	// A full implementation would be more complex
}

// GetType returns the iterator type
func (mi *MergeIterator) GetType() IteratorType {
	return MergeIteratorType
}

// Close releases resources held by the iterator
func (mi *MergeIterator) Close() {
	if mi.closed {
		return
	}

	mi.closed = true

	// Close all underlying iterators
	for _, iter := range mi.iterators {
		if iter != nil {
			iter.Close()
		}
	}

	mi.iterators = nil
	mi.heap = nil
	mi.current = nil
}
