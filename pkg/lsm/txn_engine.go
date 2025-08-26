package lsm

import (
	"tiny-lsm-go/pkg/common"
	"tiny-lsm-go/pkg/wal"
)

// GetManager returns the transaction manager
func (te *Engine) GetManager() *TransactionManager {
	return te.txnManager
}

// Begin starts a new transaction
func (te *Engine) Begin() (*Transaction, error) {
	return te.txnManager.Begin()
}

// BeginWithIsolation starts a new transaction with specified isolation level
func (te *Engine) BeginWithIsolation(isolation IsolationLevel) (*Transaction, error) {
	return te.txnManager.BeginWithIsolation(isolation)
}

// putWithTxn implements transaction-aware put operation based on isolation level
func (te *Engine) PutWithTxn(txn *Transaction, key, value string) error {
	if txn.state != TxnActive {
		return ErrTransactionNotActive
	}

	// Add WAL record
	txn.operations = append(txn.operations, wal.NewPutRecord(txn.id, key, value))

	switch txn.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Write directly to database
		// Save previous value for rollback
		prevValue, exists, err := te.GetWithTxnID(key, 0) // Get without transaction filtering
		if err != nil {
			return err
		}

		if exists {
			txn.rollbackMap[key] = &RollbackRecord{
				Value:  prevValue,
				TxnID:  txn.id,
				Exists: true,
			}
		} else {
			txn.rollbackMap[key] = &RollbackRecord{
				Exists: false,
			}
		}

		return te.PutWithTxnID(key, value, txn.id)

	default:
		// Other isolation levels: Store in temporary map
		txn.tempMap[key] = value
		return nil
	}
}

// getWithTxn implements transaction-aware get operation based on isolation level
func (te *Engine) GetWithTxn(txn *Transaction, key string) (string, bool, error) {
	if txn.state != TxnActive {
		return "", false, ErrTransactionNotActive
	}

	// First check temporary map for all isolation levels
	if value, exists := txn.tempMap[key]; exists {
		if value == "" {
			// Empty string marks deletion
			return "", false, nil
		}
		return value, true, nil
	}

	switch txn.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Read latest value without transaction filtering
		return te.GetWithTxnID(key, 0)

	case ReadCommitted:
		// READ_COMMITTED: Read with transaction filtering
		return te.GetWithTxnID(key, txn.id)

	case RepeatableRead, Serializable:
		// REPEATABLE_READ/SERIALIZABLE: Check read history first
		if readRecord, exists := txn.readMap[key]; exists {
			return readRecord.Value, readRecord.Exists, nil
		}

		// First time reading this key, save the result
		value, exists, err := te.GetWithTxnID(key, txn.id)
		if err != nil {
			return "", false, err
		}

		txn.readMap[key] = &ReadRecord{
			Value:  value,
			TxnID:  txn.id,
			Exists: exists,
		}

		return value, exists, nil

	default:
		return "", false, ErrIsolationViolation
	}
}

// deleteWithTxn implements transaction-aware delete operation based on isolation level
func (te *Engine) deleteWithTxn(txn *Transaction, key string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return ErrTransactionNotActive
	}

	// Add WAL record
	txn.operations = append(txn.operations, wal.NewDeleteRecord(txn.id, key))

	switch txn.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Delete directly from database
		// Save previous value for rollback
		prevValue, exists, err := te.GetWithTxnID(key, 0)
		if err != nil {
			return err
		}

		if exists {
			txn.rollbackMap[key] = &RollbackRecord{
				Value:  prevValue,
				TxnID:  txn.id,
				Exists: true,
			}
		} else {
			txn.rollbackMap[key] = &RollbackRecord{
				Exists: false,
			}
		}

		return te.DeleteWithTxn(key, txn.id)

	default:
		// Other isolation levels: Store deletion mark in temporary map
		txn.tempMap[key] = "" // Empty string marks deletion
		return nil
	}
}

// PutBatch inserts multiple key-value pairs using transaction context
func (te *Engine) PutBatchWithTxn(txn *Transaction, kvs []common.KVPair) error {
	if err := te.PutBatchWithTxnID(kvs, txn.id); err != nil {
		return err

	}
	return nil
}

// GetBatch retrieves multiple values by keys using transaction context
func (te *Engine) GetBatchWithTxn(txn *Transaction, keys []string) (map[string]string, error) {
	results := make(map[string]string)
	for _, key := range keys {
		value, found, err := te.GetWithTxnID(key, txn.id)
		if err != nil {
			return nil, err
		}
		if found {
			results[key] = value
		}
	}
	return results, nil
}

// DeleteBatch marks multiple keys as deleted using transaction context
func (te *Engine) DeleteBatch(txn *Transaction, keys []string) error {
	for _, key := range keys {
		if err := te.deleteWithTxn(txn, key); err != nil {
			return err
		}
	}
	return nil
}
