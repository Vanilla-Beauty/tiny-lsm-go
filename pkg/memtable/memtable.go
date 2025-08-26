package memtable

import (
	"sync"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/iterator"
	"tiny-lsm-go/pkg/skiplist"
)

// MemTable represents the in-memory table component of the LSM-tree
// It manages both current (active) and frozen (immutable) skip list tables
type MemTable struct {
	// Current active table that accepts writes
	currentTable *skiplist.SkipList
	
	// Frozen tables that are immutable and waiting to be flushed
	frozenTables []*skiplist.SkipList
	
	// Size tracking
	frozenBytes int // Total bytes in frozen tables
	
	// Mutexes for thread safety
	currentMutex sync.RWMutex // Protects current table
	frozenMutex  sync.RWMutex // Protects frozen tables
}

// New creates a new MemTable instance
func New() *MemTable {
	return &MemTable{
		currentTable: skiplist.New(),
		frozenTables: make([]*skiplist.SkipList, 0),
		frozenBytes:  0,
	}
}

// Put adds a key-value pair to the current active table
func (mt *MemTable) Put(key, value string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Add to current table
	mt.currentTable.Put(key, value, txnID)

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// PutBatch adds multiple key-value pairs in a single operation
func (mt *MemTable) PutBatch(kvs []KeyValue, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Add all entries to current table
	for _, kv := range kvs {
		mt.currentTable.Put(kv.Key, kv.Value, txnID)
	}

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// Delete marks a key as deleted by putting an empty value (tombstone)
func (mt *MemTable) Delete(key string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Delete by putting empty value (tombstone)
	mt.currentTable.Delete(key, txnID)

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// DeleteBatch marks multiple keys as deleted in a single operation
func (mt *MemTable) DeleteBatch(keys []string, txnID uint64) error {
	mt.currentMutex.Lock()
	defer mt.currentMutex.Unlock()

	// Delete all keys by putting empty values (tombstones)
	for _, key := range keys {
		mt.currentTable.Delete(key, txnID)
	}

	// Check if current table size exceeds limit and needs to be frozen
	cfg := config.GetGlobalConfig()
	if mt.currentTable.SizeBytes() > int(cfg.GetPerMemSizeLimit()) {
		// Need to freeze current table
		mt.frozenMutex.Lock()
		mt.freezeCurrentTableLocked()
		mt.frozenMutex.Unlock()
	}

	return nil
}

// Get retrieves a key from the MemTable, checking current table first, then frozen tables
func (mt *MemTable) Get(key string, txnID uint64) (string, bool, error) {
	// First check current table
	mt.currentMutex.RLock()
	iter := mt.currentTable.Get(key, txnID)
	mt.currentMutex.RUnlock()

	if iter.Valid() {
		value := iter.Value()
		isDeleted := iter.IsDeleted()
		iter.Close()
		
		if isDeleted {
			return "", false, nil // Key was deleted
		}
		return value, true, nil
	}
	iter.Close()

	// If not found in current table, check frozen tables
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	// Check frozen tables from newest to oldest
	for i := 0; i < len(mt.frozenTables); i++ {
		iter := mt.frozenTables[i].Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			iter.Close()
			
			if isDeleted {
				return "", false, nil // Key was deleted
			}
			return value, true, nil
		}
		iter.Close()
	}

	return "", false, nil // Key not found
}

// GetBatch retrieves multiple keys from the MemTable
func (mt *MemTable) GetBatch(keys []string, txnID uint64) ([]GetResult, error) {
	results := make([]GetResult, len(keys))

	// First check current table for all keys
	mt.currentMutex.RLock()
	for i, key := range keys {
		iter := mt.currentTable.Get(key, txnID)
		if iter.Valid() {
			value := iter.Value()
			isDeleted := iter.IsDeleted()
			results[i] = GetResult{
				Key:   key,
				Value: value,
				Found: !isDeleted,
			}
		} else {
			results[i] = GetResult{
				Key:   key,
				Found: false,
			}
		}
		iter.Close()
	}
	mt.currentMutex.RUnlock()

	// For keys not found in current table, check frozen tables
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()

	for i, key := range keys {
		if results[i].Found {
			continue // Already found in current table
		}

		// Check frozen tables from newest to oldest
		for j := 0; j < len(mt.frozenTables); j++ {
			iter := mt.frozenTables[j].Get(key, txnID)
			if iter.Valid() {
				value := iter.Value()
				isDeleted := iter.IsDeleted()
				results[i] = GetResult{
					Key:   key,
					Value: value,
					Found: !isDeleted,
				}
				iter.Close()
				break
			}
			iter.Close()
		}
	}

	return results, nil
}

// freezeCurrentTableLocked freezes the current table and creates a new one
// Caller must hold both currentMutex and frozenMutex locks
func (mt *MemTable) freezeCurrentTableLocked() {
	// Move current table to front of frozen tables list
	mt.frozenBytes += mt.currentTable.SizeBytes()
	
	// Add to beginning of slice (newest frozen table first)
	mt.frozenTables = append([]*skiplist.SkipList{mt.currentTable}, mt.frozenTables...)
	
	// Create new current table
	mt.currentTable = skiplist.New()
}

// FreezeCurrentTable explicitly freezes the current table
func (mt *MemTable) FreezeCurrentTable() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	mt.freezeCurrentTableLocked()
	mt.frozenMutex.Unlock()
	mt.currentMutex.Unlock()
}

// GetCurrentSize returns the size of the current active table
func (mt *MemTable) GetCurrentSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.SizeBytes()
}

// GetFrozenSize returns the total size of all frozen tables
func (mt *MemTable) GetFrozenSize() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return mt.frozenBytes
}

// GetTotalSize returns the total size of current and frozen tables
func (mt *MemTable) GetTotalSize() int {
	return mt.GetCurrentSize() + mt.GetFrozenSize()
}

// GetCurrentTableSize returns the number of entries in current table
func (mt *MemTable) GetCurrentTableSize() int {
	mt.currentMutex.RLock()
	defer mt.currentMutex.RUnlock()
	return mt.currentTable.Size()
}

// GetFrozenTableCount returns the number of frozen tables
func (mt *MemTable) GetFrozenTableCount() int {
	mt.frozenMutex.RLock()
	defer mt.frozenMutex.RUnlock()
	return len(mt.frozenTables)
}

// Clear removes all data from the MemTable
func (mt *MemTable) Clear() {
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	defer mt.currentMutex.Unlock()
	defer mt.frozenMutex.Unlock()

	mt.currentTable.Clear()
	mt.frozenTables = make([]*skiplist.SkipList, 0)
	mt.frozenBytes = 0
}

// NewIterator creates a new iterator over all entries in the MemTable
func (mt *MemTable) NewIterator(txnID uint64) iterator.Iterator {
	return NewMemTableIterator(mt, txnID)
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   string
	Value string
}

// GetResult represents the result of a Get operation
type GetResult struct {
	Key   string
	Value string
	Found bool
}
