package lsm

import (
	"tiny-lsm-go/pkg/iterator"
	"tiny-lsm-go/pkg/wal"
)

// TxnEngine wraps the LSM engine to provide transaction support
type TxnEngine struct {
	engine  *Engine
	manager *TransactionManager
}

// NewTxnEngine creates a new transactional engine wrapper
func NewTxnEngine(engine *Engine, config *TransactionConfig) *TxnEngine {
	manager := NewTransactionManager(engine, config)
	
	return &TxnEngine{
		engine:  engine,
		manager: manager,
	}
}

// GetManager returns the transaction manager
func (te *TxnEngine) GetManager() *TransactionManager {
	return te.manager
}

// GetEngine returns the underlying LSM engine
func (te *TxnEngine) GetEngine() *Engine {
	return te.engine
}

// Begin starts a new transaction
func (te *TxnEngine) Begin() (*Transaction, error) {
	return te.manager.Begin()
}

// BeginWithIsolation starts a new transaction with specified isolation level
func (te *TxnEngine) BeginWithIsolation(isolation IsolationLevel) (*Transaction, error) {
	return te.manager.BeginWithIsolation(isolation)
}

// Put inserts a key-value pair using transaction context
func (te *TxnEngine) Put(txn *Transaction, key, value string) error {
	return te.putWithTxn(txn, key, value)
}

// Get retrieves a value by key using transaction context
func (te *TxnEngine) Get(txn *Transaction, key string) (string, bool, error) {
	return te.getWithTxn(txn, key)
}

// Delete marks a key as deleted using transaction context
func (te *TxnEngine) Delete(txn *Transaction, key string) error {
	return te.deleteWithTxn(txn, key)
}

// putWithTxn implements transaction-aware put operation based on isolation level
func (te *TxnEngine) putWithTxn(txn *Transaction, key, value string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.state != TxnActive {
		return ErrTransactionNotActive
	}

	// Add WAL record
	txn.operations = append(txn.operations, wal.NewPutRecord(txn.id, key, value))

	switch txn.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Write directly to database
		// Save previous value for rollback
		prevValue, exists, err := te.engine.GetWithTxn(key, 0) // Get without transaction filtering
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

		return te.engine.PutWithTxn(key, value, txn.id)

	default:
		// Other isolation levels: Store in temporary map
		txn.tempMap[key] = value
		return nil
	}
}

// getWithTxn implements transaction-aware get operation based on isolation level  
func (te *TxnEngine) getWithTxn(txn *Transaction, key string) (string, bool, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

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
		return te.engine.GetWithTxn(key, 0)

	case ReadCommitted:
		// READ_COMMITTED: Read with transaction filtering
		return te.engine.GetWithTxn(key, txn.id)

	case RepeatableRead, Serializable:
		// REPEATABLE_READ/SERIALIZABLE: Check read history first
		if readRecord, exists := txn.readMap[key]; exists {
			return readRecord.Value, readRecord.Exists, nil
		}

		// First time reading this key, save the result
		value, exists, err := te.engine.GetWithTxn(key, txn.id)
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
func (te *TxnEngine) deleteWithTxn(txn *Transaction, key string) error {
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
		prevValue, exists, err := te.engine.GetWithTxn(key, 0)
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

		return te.engine.DeleteWithTxn(key, txn.id)

	default:
		// Other isolation levels: Store deletion mark in temporary map
		txn.tempMap[key] = "" // Empty string marks deletion
		return nil
	}
}

// PutBatch inserts multiple key-value pairs using transaction context
func (te *TxnEngine) PutBatch(txn *Transaction, kvs []KVPair) error {
	for _, kv := range kvs {
		if err := te.putWithTxn(txn, kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

// GetBatch retrieves multiple values by keys using transaction context
func (te *TxnEngine) GetBatch(txn *Transaction, keys []string) (map[string]string, error) {
	results := make(map[string]string)
	for _, key := range keys {
		value, found, err := te.getWithTxn(txn, key)
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
func (te *TxnEngine) DeleteBatch(txn *Transaction, keys []string) error {
	for _, key := range keys {
		if err := te.deleteWithTxn(txn, key); err != nil {
			return err
		}
	}
	return nil
}

// NewIteratorWithTxn creates a new iterator with transaction ID
func (te *TxnEngine) NewIteratorWithTxn(txnID uint64) (TransactionIterator, error) {
	iter := te.engine.NewIteratorWithTxn(txnID)
	return &TxnIteratorWrapper{iter: iter}, nil
}

// TxnIteratorWrapper wraps a regular iterator to implement TransactionIterator
type TxnIteratorWrapper struct {
	iter iterator.Iterator
}

func (w *TxnIteratorWrapper) Valid() bool {
	return w.iter.Valid()
}

func (w *TxnIteratorWrapper) Key() string {
	return w.iter.Key()
}

func (w *TxnIteratorWrapper) Value() string {
	return w.iter.Value()
}

func (w *TxnIteratorWrapper) Next() {
	w.iter.Next()
}

func (w *TxnIteratorWrapper) Seek(key string) {
	w.iter.Seek(key)
}

func (w *TxnIteratorWrapper) SeekToFirst() {
	w.iter.SeekToFirst()
}

func (w *TxnIteratorWrapper) SeekToLast() {
	w.iter.SeekToLast()
}

func (w *TxnIteratorWrapper) Close() {
	w.iter.Close()
}

// Close closes the transaction engine and its components
func (te *TxnEngine) Close() error {
	return te.engine.Close()
}
