package skiplist

import (
	"sync"
	"tiny-lsm-go/pkg/iterator"
)

// SkipListIterator implements the Iterator interface for skip lists
type SkipListIterator struct {
	current *Node
	sl      *SkipList
	txnID   uint64
	closed  bool
	mu      sync.RWMutex
}

// NewSkipListIterator creates a new skip list iterator
func NewSkipListIterator(node *Node, sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		current: node,
		sl:      sl,
		txnID:   0,
		closed:  false,
	}
}

// Valid returns true if the iterator is pointing to a valid entry
func (iter *SkipListIterator) Valid() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if iter.closed || iter.current == nil {
		return false
	}

	// Check if the current entry is visible to this transaction
	if iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		return false
	}

	return true
}

// Key returns the key of the current entry
func (iter *SkipListIterator) Key() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return ""
	}
	return iter.current.Key()
}

// Value returns the value of the current entry
func (iter *SkipListIterator) Value() string {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return ""
	}
	return iter.current.Value()
}

// TxnID returns the transaction ID of the current entry
func (iter *SkipListIterator) TxnID() uint64 {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return 0
	}
	return iter.current.TxnID()
}

// IsDeleted returns true if the current entry is a delete marker
func (iter *SkipListIterator) IsDeleted() bool {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return false
	}
	return iter.current.IsDeleted()
}

// Entry returns the current entry
func (iter *SkipListIterator) Entry() iterator.Entry {
	iter.mu.RLock()
	defer iter.mu.RUnlock()

	if !iter.Valid() {
		return iterator.Entry{}
	}
	return iter.current.Entry()
}

// Next advances the iterator to the next entry
func (iter *SkipListIterator) Next() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed || iter.current == nil {
		return
	}

	iter.current = iter.current.Next()

	// Skip entries that are not visible to this transaction
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}

// Seek positions the iterator at the first entry with key >= target
func (iter *SkipListIterator) Seek(key string) {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	current := iter.sl.header

	// Navigate to the target key or the first key greater than target
	for i := iter.sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].CompareKey(key) < 0 {
			current = current.forward[i]
		}
	}

	iter.current = current.forward[0]

	// Skip entries that are not visible to this transaction
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}

// SeekToFirst positions the iterator at the first entry
func (iter *SkipListIterator) SeekToFirst() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	iter.current = iter.sl.header.forward[0]

	// Skip entries that are not visible to this transaction
	for iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		iter.current = iter.current.Next()
	}
}

// SeekToLast positions the iterator at the last entry
func (iter *SkipListIterator) SeekToLast() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	if iter.closed {
		return
	}

	current := iter.sl.header

	// Navigate to the last node
	for i := iter.sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}

	iter.current = current

	// If this entry is not visible, move backwards to find a visible one
	// Note: This is a simplified implementation. In practice, you might want
	// to add backward pointers for more efficient backward traversal
	if iter.current != nil && iter.txnID > 0 && iter.current.TxnID() > iter.txnID {
		// For simplicity, we'll just seek to first and iterate to the last visible entry
		iter.current = iter.sl.header.forward[0]
		var lastVisible *Node

		for iter.current != nil {
			if iter.txnID == 0 || iter.current.TxnID() <= iter.txnID {
				lastVisible = iter.current
			}
			iter.current = iter.current.Next()
		}

		iter.current = lastVisible
	}
}

// GetType returns the iterator type
func (iter *SkipListIterator) GetType() iterator.IteratorType {
	return iterator.SkipListIteratorType
}

// Close releases any resources held by the iterator
func (iter *SkipListIterator) Close() {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.closed = true
	iter.current = nil
	iter.sl = nil
}

// SetTxnID sets the transaction ID for visibility checking
func (iter *SkipListIterator) SetTxnID(txnID uint64) {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.txnID = txnID
}
