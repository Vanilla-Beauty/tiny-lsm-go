package iterator

import "sort"

// MockIterator is a test implementation of Iterator
type MockIterator struct {
	data    []Entry
	current int
	valid   bool
}

func NewMockIterator(entries []Entry) *MockIterator {
	// Sort entries by key for proper iteration order
	sortedEntries := make([]Entry, len(entries))
	copy(sortedEntries, entries)
	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Key < sortedEntries[j].Key
	})

	return &MockIterator{
		data:    sortedEntries,
		current: 0,
		valid:   len(sortedEntries) > 0,
	}
}

func (m *MockIterator) SeekToFirst() {
	if len(m.data) > 0 {
		m.current = 0
		m.valid = true
	} else {
		m.current = 0
		m.valid = false
	}
}

func (m *MockIterator) Seek(key string) bool {
	// Find first entry >= key
	for i, entry := range m.data {
		if entry.Key >= key {
			m.current = i
			m.valid = true
			return entry.Key == key
		}
	}
	// Key not found and no entry >= key
	m.current = len(m.data)
	m.valid = false
	return false
}

func (m *MockIterator) SeekToLast() {
	if len(m.data) > 0 {
		m.current = len(m.data) - 1
		m.valid = true
	} else {
		m.current = -1
		m.valid = false
	}
}

func (m *MockIterator) Next() {
	if m.valid && m.current < len(m.data)-1 {
		m.current++
	} else {
		m.valid = false
		m.current = len(m.data)
	}
}

func (m *MockIterator) Valid() bool {
	return m.valid && m.current >= 0 && m.current < len(m.data)
}

func (m *MockIterator) Key() string {
	if m.Valid() {
		return m.data[m.current].Key
	}
	return ""
}

func (m *MockIterator) Value() string {
	if m.Valid() {
		return m.data[m.current].Value
	}
	return ""
}

func (m *MockIterator) TxnID() uint64 {
	if m.Valid() {
		return m.data[m.current].TxnID
	}
	return 0
}

func (m *MockIterator) IsDeleted() bool {
	if m.Valid() {
		return m.data[m.current].Value == ""
	}
	return false
}

func (m *MockIterator) Entry() Entry {
	if m.Valid() {
		return m.data[m.current]
	}
	return Entry{}
}

func (m *MockIterator) GetType() IteratorType {
	return SkipListIteratorType // Mock iterator pretends to be a skip list iterator
}

func (m *MockIterator) Close() {
	m.valid = false
	m.current = -1
}
