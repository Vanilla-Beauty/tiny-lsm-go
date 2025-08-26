package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"tiny-lsm-go/pkg/wal"
)

// TransactionPersistence handles transaction state persistence
type TransactionPersistence struct {
	dataDir string
	mu      sync.RWMutex
}

// TransactionRecord represents a persisted transaction record
type TransactionRecord struct {
	TxnID       uint64            `json:"txn_id"`
	State       TransactionState  `json:"state"`
	Isolation   IsolationLevel    `json:"isolation"`
	StartTime   int64             `json:"start_time"`  // Unix timestamp
	CommitTime  int64             `json:"commit_time"` // Unix timestamp
	Operations  []*wal.Record     `json:"operations"`
}

// NewTransactionPersistence creates a new transaction persistence manager
func NewTransactionPersistence(dataDir string) *TransactionPersistence {
	return &TransactionPersistence{
		dataDir: dataDir,
	}
}

// GetTransactionFilePath returns the file path for storing transaction state
func (tp *TransactionPersistence) GetTransactionFilePath() string {
	return filepath.Join(tp.dataDir, "transaction_state.json")
}

// SaveTransactionState saves transaction state to disk
func (tp *TransactionPersistence) SaveTransactionState(manager *TransactionManager) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// Collect transaction records
	var records []TransactionRecord
	
	manager.mu.RLock()
	
	// Save committed transactions (active ones are volatile)
	for _, txn := range manager.committedTxns {
		record := TransactionRecord{
			TxnID:      txn.id,
			State:      txn.state,
			Isolation:  txn.isolation,
			StartTime:  txn.startTime.Unix(),
			CommitTime: txn.commitTime.Unix(),
			Operations: txn.operations,
		}
		records = append(records, record)
	}
	
	manager.mu.RUnlock()

	// Marshal to JSON
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal transaction records: %w", err)
	}

	// Write to file
	filePath := tp.GetTransactionFilePath()
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write transaction state file: %w", err)
	}

	return nil
}

// LoadTransactionState loads transaction state from disk
func (tp *TransactionPersistence) LoadTransactionState() ([]TransactionRecord, error) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	filePath := tp.GetTransactionFilePath()
	
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// No existing state file, return empty records
		return []TransactionRecord{}, nil
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction state file: %w", err)
	}

	// Unmarshal JSON
	var records []TransactionRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction records: %w", err)
	}

	return records, nil
}

// CleanupTransactionState removes old transaction state files
func (tp *TransactionPersistence) CleanupTransactionState() error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	filePath := tp.GetTransactionFilePath()
	
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// File doesn't exist, nothing to cleanup
		return nil
	}

	return os.Remove(filePath)
}

// RecoverTransactionState helps recover transaction state after restart
func (tp *TransactionPersistence) RecoverTransactionState(manager *TransactionManager) error {
	records, err := tp.LoadTransactionState()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil // No state to recover
	}

	// Find the highest transaction ID to resume numbering
	var maxTxnID uint64
	for _, record := range records {
		if record.TxnID > maxTxnID {
			maxTxnID = record.TxnID
		}
	}

	// Update next transaction ID to be higher than any recovered ID
	manager.nextTxnID = maxTxnID + 1

	return nil
}
