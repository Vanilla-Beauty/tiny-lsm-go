package iterator

// SelectIterator selects from multiple iterators, always choosing the one with the smallest key
type SelectIterator struct {
	iterators []Iterator
	current   int
	closed    bool
}

// NewSelectIterator creates a new select iterator
func NewSelectIterator(iterators []Iterator) Iterator {
	if len(iterators) == 0 {
		return NewEmptyIterator()
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	si := &SelectIterator{
		iterators: iterators,
		current:   -1,
		closed:    false,
	}

	return si
}

// Valid returns true if the iterator is pointing to a valid entry
func (si *SelectIterator) Valid() bool {
	return !si.closed && si.current >= 0 && si.current < len(si.iterators) && si.iterators[si.current].Valid()
}

// Key returns the key of the current entry
func (si *SelectIterator) Key() string {
	if si.Valid() {
		return si.iterators[si.current].Key()
	}
	return ""
}

// Value returns the value of the current entry
func (si *SelectIterator) Value() string {
	if si.Valid() {
		return si.iterators[si.current].Value()
	}
	return ""
}

// TxnID returns the transaction ID of the current entry
func (si *SelectIterator) TxnID() uint64 {
	if si.Valid() {
		return si.iterators[si.current].TxnID()
	}
	return 0
}

// IsDeleted returns true if the current entry is a delete marker
func (si *SelectIterator) IsDeleted() bool {
	if si.Valid() {
		return si.iterators[si.current].IsDeleted()
	}
	return false
}

// Entry returns the current entry
func (si *SelectIterator) Entry() Entry {
	if si.Valid() {
		return si.iterators[si.current].Entry()
	}
	return Entry{}
}

// findNextIterator finds the iterator with the smallest key
func (si *SelectIterator) findNextIterator() int {
	bestIndex := -1
	for i, iter := range si.iterators {
		if iter.Valid() {
			if bestIndex == -1 || iter.Key() < si.iterators[bestIndex].Key() {
				bestIndex = i
			}
		}
	}
	return bestIndex
}

// Next advances the iterator to the next entry
func (si *SelectIterator) Next() {
	if !si.Valid() {
		return
	}

	currentKey := si.iterators[si.current].Key()

	// Advance all iterators with the same key
	for _, iter := range si.iterators {
		if iter.Valid() && iter.Key() == currentKey {
			iter.Next()
		}
	}

	// Find the next iterator with the smallest key
	si.current = si.findNextIterator()
}

// Seek positions the iterator at the first entry with key >= target
func (si *SelectIterator) Seek(key string) bool {
	if si.closed {
		return false
	}

	// Seek all iterators
	for _, iter := range si.iterators {
		iter.Seek(key)
	}

	// Find the iterator with the smallest key
	si.current = si.findNextIterator()

	// Return true if the found key equals the target key
	if si.Valid() {
		return si.iterators[si.current].Key() == key
	}
	return false
}

// SeekToFirst positions the iterator at the first entry
func (si *SelectIterator) SeekToFirst() {
	if si.closed {
		return
	}

	// Seek all iterators to first
	for _, iter := range si.iterators {
		iter.SeekToFirst()
	}

	// Find the iterator with the smallest key
	si.current = si.findNextIterator()
}

// SeekToLast positions the iterator at the last entry
func (si *SelectIterator) SeekToLast() {
	if si.closed {
		return
	}

	// Seek all iterators to last
	for _, iter := range si.iterators {
		iter.SeekToLast()
	}

	// Find the iterator with the largest key
	bestIndex := -1
	for i, iter := range si.iterators {
		if iter.Valid() {
			if bestIndex == -1 || iter.Key() > si.iterators[bestIndex].Key() {
				bestIndex = i
			}
		}
	}
	si.current = bestIndex
}

// GetType returns the iterator type
func (si *SelectIterator) GetType() IteratorType {
	return SelectIteratorType
}

// Close releases resources held by the iterator
func (si *SelectIterator) Close() {
	if si.closed {
		return
	}

	si.closed = true

	// Close all underlying iterators
	for _, iter := range si.iterators {
		if iter != nil {
			iter.Close()
		}
	}

	si.iterators = nil
	si.current = -1
}
