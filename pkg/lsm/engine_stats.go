package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
)

// EngineStatistics holds statistics for the LSM engine
type EngineStatistics struct {
	// Read/Write statistics
	Reads   uint64
	Writes  uint64
	Deletes uint64

	// Flush statistics
	Flushes      uint64
	FlushedBytes uint64

	// Compaction statistics
	Compactions    uint64
	CompactedBytes uint64
	CompactedFiles uint64

	// Memory statistics
	MemTableSize    uint64
	FrozenTableSize uint64

	// SST statistics
	SSTFiles     uint64
	TotalSSTSize uint64
}

// EngineMetadata represents the metadata that needs to be persisted
type EngineMetadata struct {
	NextSSTID       uint64 `json:"next_sst_id"`
	NextTxnID       uint64 `json:"next_txn_id"`
	GlobalReadTxnID uint64 `json:"global_read_txn_id"`
}

// saveMetadata saves the engine metadata to disk
func saveMetadata(e *Engine) error {
	data, err := json.MarshalIndent(e.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(e.metadataFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// loadMetadata loads the engine metadata from disk
func loadMetadata(e *Engine) error {
	// Check if metadata file exists
	if _, err := os.Stat(e.metadataFile); os.IsNotExist(err) {
		// No existing metadata file, use defaults
		atomic.StoreUint64(&e.metadata.NextSSTID, 0)
		atomic.StoreUint64(&e.metadata.NextTxnID, 1)
		atomic.StoreUint64(&e.metadata.GlobalReadTxnID, 1)
		return nil
	}

	// Read file
	data, err := os.ReadFile(e.metadataFile)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Unmarshal JSON
	if e.metadata == nil {
		e.metadata = &EngineMetadata{}
	}
	if err := json.Unmarshal(data, e.metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return nil
}
