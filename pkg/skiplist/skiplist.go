package skiplist

import (
	"math/rand"
	"time"
	"tiny-lsm-go/pkg/iterator"
)

const (
	// MaxLevel is the maximum level of the skip list
	MaxLevel = 16
	// Probability for level promotion
	Probability = 0.5
)

// SkipList represents a skip list data structure
type SkipList struct {
	header    *Node      // header node (sentinel)
	level     int        // current maximum level
	size      int        // number of elements
	sizeBytes int        // approximate memory size in bytes
	rng       *rand.Rand // random number generator
}

// New creates a new skip list
func New() *SkipList {
	header := NewNode("", "", 0, MaxLevel)
	return &SkipList{
		header:    header,
		level:     1,
		size:      0,
		sizeBytes: 0,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < MaxLevel && sl.rng.Float64() < Probability {
		level++
	}
	return level
}

// Delete marks a key as deleted (tombstone)
func (sl *SkipList) Delete(key string, txnID uint64) {
	sl.Put(key, "", txnID)
}

// put is the internal method for inserting/updating entries
func (sl *SkipList) Put(key, value string, txnID uint64) {
	update := make([]*Node, MaxLevel)
	current := sl.header

	tmpNode := NewNode(key, value, txnID, 1)
	// Find the insertion point
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].CompareNode(tmpNode) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// If key already exists, we need to handle MVCC properly
	// In LSM-tree, we always insert new versions
	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	newNode := NewNode(key, value, txnID, level)

	// Set forward pointers and update backward pointers
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		if update[i].forward[i] != nil {
			update[i].forward[i].backward = newNode
		}
		update[i].forward[i] = newNode
	}

	// Set backward pointer
	newNode.backward = update[0]
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	}

	sl.size++
	sl.sizeBytes += len(key) + len(value) + 8 // approximate size calculation
}

// Get finds the most recent version of a key visible to the given transaction
func (sl *SkipList) Get(key string, txnID uint64) *SkipListIterator {
	current := sl.header

	// Navigate to the key
	tempNode := NewNode(key, "", txnID, 1)
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].CompareNode(tempNode) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	// Find the first version visible to this transaction
	for current != nil && current.Key() == key {
		if txnID == 0 || current.TxnID() <= txnID {
			// Found a version visible to this transaction
			iter := NewSkipListIterator(current, sl)
			iter.SetTxnID(txnID)
			return iter
		}
		current = current.forward[0]
	}

	// Key not found or no visible version
	iter := NewSkipListIterator(nil, sl)
	iter.SetTxnID(txnID)
	return iter
}

// Size returns the number of entries in the skip list
func (sl *SkipList) Size() int {
	return sl.size
}

// SizeBytes returns the approximate memory usage in bytes
func (sl *SkipList) SizeBytes() int {

	return sl.sizeBytes
}

// IsEmpty returns true if the skip list is empty
func (sl *SkipList) IsEmpty() bool {

	return sl.size == 0
}

// Clear removes all entries from the skip list
func (sl *SkipList) Clear() {

	sl.header = NewNode("", "", 0, MaxLevel)
	sl.level = 1
	sl.size = 0
	sl.sizeBytes = 0
}

// NewIterator creates a new iterator for the skip list
func (sl *SkipList) NewIterator(txnID uint64) iterator.Iterator {

	iter := NewSkipListIterator(sl.header.forward[0], sl)
	iter.SetTxnID(txnID)
	return iter
}

// Flush returns all entries in the skip list as a slice
func (sl *SkipList) Flush() []iterator.Entry {

	entries := make([]iterator.Entry, 0, sl.size)
	current := sl.header.forward[0]

	for current != nil {
		entries = append(entries, current.Entry())
		current = current.forward[0]
	}

	return entries
}
