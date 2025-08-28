package iterator

// ConcatIterator concatenates multiple sorted iterators
type ConcatIterator struct {
	iterators []Iterator
	current   int
	closed    bool
}

// NewConcatIterator creates a new concat iterator
func NewConcatIterator(iterators []Iterator) Iterator {
	if len(iterators) == 0 {
		return NewEmptyIterator()
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	ci := &ConcatIterator{
		iterators: iterators,
		current:   -1,
		closed:    false,
	}

	return ci
}

// Valid returns true if the iterator is pointing to a valid entry
func (ci *ConcatIterator) Valid() bool {
	return !ci.closed && ci.current >= 0 && ci.current < len(ci.iterators) && ci.iterators[ci.current].Valid()
}

// Key returns the key of the current entry
func (ci *ConcatIterator) Key() string {
	if ci.Valid() {
		return ci.iterators[ci.current].Key()
	}
	return ""
}

// Value returns the value of the current entry
func (ci *ConcatIterator) Value() string {
	if ci.Valid() {
		return ci.iterators[ci.current].Value()
	}
	return ""
}

// TxnID returns the transaction ID of the current entry
func (ci *ConcatIterator) TxnID() uint64 {
	if ci.Valid() {
		return ci.iterators[ci.current].TxnID()
	}
	return 0
}

// IsDeleted returns true if the current entry is a delete marker
func (ci *ConcatIterator) IsDeleted() bool {
	if ci.Valid() {
		return ci.iterators[ci.current].IsDeleted()
	}
	return false
}

// Entry returns the current entry
func (ci *ConcatIterator) Entry() Entry {
	if ci.Valid() {
		return ci.iterators[ci.current].Entry()
	}
	return Entry{}
}

// Next advances the iterator to the next entry
func (ci *ConcatIterator) Next() {
	if !ci.Valid() {
		return
	}

	// Advance the current iterator
	ci.iterators[ci.current].Next()

	// If current iterator is exhausted, move to the next one
	for ci.current < len(ci.iterators) && !ci.iterators[ci.current].Valid() {
		ci.current++
	}
}

// Seek positions the iterator at the first entry with key >= target
func (ci *ConcatIterator) Seek(key string) bool {
	if ci.closed {
		return false
	}

	// Find the first iterator that might contain the key
	for i, iter := range ci.iterators {
		iter.Seek(key)
		if iter.Valid() {
			ci.current = i
			return iter.Key() == key
		}
	}

	// If no iterator contains the key, set current to beyond the last iterator
	ci.current = len(ci.iterators)
	return false
}

// SeekToFirst positions the iterator at the first entry
func (ci *ConcatIterator) SeekToFirst() {
	if ci.closed {
		return
	}

	// Find the first valid iterator
	for _, iter := range ci.iterators {
		iter.SeekToFirst()
	}

	ci.current = 0
}

// SeekToLast positions the iterator at the last entry
func (ci *ConcatIterator) SeekToLast() {
	if ci.closed {
		return
	}

	// Find the last valid iterator
	for i := len(ci.iterators) - 1; i >= 0; i-- {
		iter := ci.iterators[i]
		iter.SeekToLast()
		if iter.Valid() {
			ci.current = i
			return
		}
	}

	// If no iterator is valid, set current to before the first iterator
	ci.current = -1
}

// GetType returns the iterator type
func (ci *ConcatIterator) GetType() IteratorType {
	return ConcatIteratorType
}

// Close releases resources held by the iterator
func (ci *ConcatIterator) Close() {
	if ci.closed {
		return
	}

	ci.closed = true

	// Close all underlying iterators
	for _, iter := range ci.iterators {
		if iter != nil {
			iter.Close()
		}
	}

	ci.iterators = nil
	ci.current = -1
}
