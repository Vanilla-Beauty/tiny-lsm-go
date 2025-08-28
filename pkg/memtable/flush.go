package memtable

import (
	"fmt"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/iterator"
)

// FlushResult represents the result of flushing a MemTable to storage
type FlushResult struct {
	Entries       []iterator.Entry
	MinTxnID      uint64
	MaxTxnID      uint64
	FirstKey      string
	LastKey       string
	FlushedSize   int
	FlushedTxnIDs []uint64
}

// FlushOldest flushes the oldest frozen table to storage
// Returns nil if no frozen tables exist or current table is empty
func (mt *MemTable) FlushOldest() (*FlushResult, error) {
	mt.frozenMutex.Lock()
	defer mt.frozenMutex.Unlock()

	// If no frozen tables, check if we need to freeze current table
	if len(mt.frozenTables) == 0 {
		mt.currentMutex.Lock()
		if mt.currentTable.Size() == 0 {
			mt.currentMutex.Unlock()
			return nil, nil // Nothing to flush
		}

		// Freeze current table
		mt.freezeCurrentTableLocked()
		mt.currentMutex.Unlock()
	}

	// Get the oldest frozen table (last in the slice)
	oldestTable := mt.frozenTables[len(mt.frozenTables)-1]

	// Remove it from frozen tables
	mt.frozenTables = mt.frozenTables[:len(mt.frozenTables)-1]
	mt.frozenBytes -= oldestTable.SizeBytes()

	// Flush the table
	entries := oldestTable.Flush()

	if len(entries) == 0 {
		return &FlushResult{}, nil
	}

	// Calculate statistics
	result := &FlushResult{
		Entries:       entries,
		MinTxnID:      entries[0].TxnID,
		MaxTxnID:      entries[0].TxnID,
		FirstKey:      entries[0].Key,
		LastKey:       entries[len(entries)-1].Key,
		FlushedSize:   oldestTable.SizeBytes(),
		FlushedTxnIDs: make([]uint64, 0),
	}

	// Find min/max transaction IDs and collect transaction IDs for empty entries
	for _, entry := range entries {
		if entry.TxnID < result.MinTxnID {
			result.MinTxnID = entry.TxnID
		}
		if entry.TxnID > result.MaxTxnID {
			result.MaxTxnID = entry.TxnID
		}

		// Collect transaction IDs for empty entries (these might be special markers)
		if entry.Key == "" && entry.Value == "" {
			result.FlushedTxnIDs = append(result.FlushedTxnIDs, entry.TxnID)
		}
	}

	return result, nil
}

// FlushAll flushes all tables (current and frozen) to storage
func (mt *MemTable) FlushAll() ([]*FlushResult, error) {
	var results []*FlushResult

	// First freeze current table if it has data
	mt.currentMutex.Lock()
	mt.frozenMutex.Lock()
	if mt.currentTable.Size() > 0 {
		mt.freezeCurrentTableLocked()
	}
	mt.frozenMutex.Unlock()
	mt.currentMutex.Unlock()

	// Now flush all frozen tables
	for {
		result, err := mt.FlushOldest()
		if err != nil {
			return results, err
		}
		if result == nil {
			break // No more tables to flush
		}
		results = append(results, result)
	}

	return results, nil
}

// CanFlush returns true if there are tables that can be flushed
// This should only return true when we actually need to flush, not just when there's any data
func (mt *MemTable) CanFlush() bool {
	mt.frozenMutex.RLock()
	hasFrozen := len(mt.frozenTables) > 0
	mt.frozenMutex.RUnlock()

	// Always flush frozen tables since they're already frozen
	if hasFrozen {
		return true
	}

	// Don't aggressively flush the current table unless it's reached a reasonable size
	// The background worker should not flush tiny amounts of data immediately
	return false
}

func (mt *MemTable) Empty() bool {
	return (mt.currentTable == nil || mt.currentTable.Size() == 0) && len(mt.frozenTables) == 0
}

// EstimateFlushSize returns the estimated size of data that would be flushed
func (mt *MemTable) EstimateFlushSize() int {
	mt.frozenMutex.RLock()
	frozenCount := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	if frozenCount > 0 {
		// If we have frozen tables, return the size of the oldest one
		mt.frozenMutex.RLock()
		oldestSize := mt.frozenTables[len(mt.frozenTables)-1].SizeBytes()
		mt.frozenMutex.RUnlock()
		return oldestSize
	}

	// Otherwise return current table size
	return mt.GetCurrentSize()
}

// GetFlushableTableCount returns the number of tables that can be flushed
func (mt *MemTable) GetFlushableTableCount() int {
	mt.frozenMutex.RLock()
	count := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	mt.currentMutex.RLock()
	if mt.currentTable.Size() > 0 {
		count++
	}
	mt.currentMutex.RUnlock()

	return count
}

// GetMemTableStats returns statistics about the MemTable
func (mt *MemTable) GetMemTableStats() MemTableStats {
	mt.currentMutex.RLock()
	currentSize := mt.currentTable.SizeBytes()
	currentEntries := mt.currentTable.Size()
	mt.currentMutex.RUnlock()

	mt.frozenMutex.RLock()
	frozenSize := mt.frozenBytes
	frozenCount := len(mt.frozenTables)
	mt.frozenMutex.RUnlock()

	return MemTableStats{
		CurrentTableSize:    currentSize,
		CurrentTableEntries: currentEntries,
		FrozenTablesSize:    frozenSize,
		FrozenTablesCount:   frozenCount,
		TotalSize:           currentSize + frozenSize,
		TotalEntries:        currentEntries, // Note: We don't count frozen entries to avoid complexity
	}
}

// ShouldFlush returns true if the MemTable should be flushed based on configuration
func (mt *MemTable) ShouldFlush() bool {
	stats := mt.GetMemTableStats()

	// Check if we have too many frozen tables
	if stats.FrozenTablesCount > 3 {
		return true
	}

	// Check if total size is too large
	totalSizeLimit := mt.getTotalSizeLimit()
	return stats.TotalSize > totalSizeLimit
}

// getTotalSizeLimit returns the total size limit for all MemTables
func (mt *MemTable) getTotalSizeLimit() int {
	cfg := config.GetGlobalConfig()
	return int(cfg.GetTotalMemSizeLimit())
}

// MemTableStats represents statistics about a MemTable
type MemTableStats struct {
	CurrentTableSize    int
	CurrentTableEntries int
	FrozenTablesSize    int
	FrozenTablesCount   int
	TotalSize           int
	TotalEntries        int
}

// String returns a string representation of MemTableStats
func (stats MemTableStats) String() string {
	return fmt.Sprintf("MemTableStats{Current: %d bytes (%d entries), Frozen: %d bytes (%d tables), Total: %d bytes}",
		stats.CurrentTableSize, stats.CurrentTableEntries,
		stats.FrozenTablesSize, stats.FrozenTablesCount,
		stats.TotalSize)
}
