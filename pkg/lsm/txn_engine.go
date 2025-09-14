package lsm

import (
	"tiny-lsm-go/pkg/common"
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
	// TODO: Lab 5.2
	return nil
}

// getWithTxn implements transaction-aware get operation based on isolation level
func (te *Engine) GetWithTxn(txn *Transaction, key string) (string, bool, error) {
	// TODO: Lab 5.2
	return "", false, nil
}

// deleteWithTxn implements transaction-aware delete operation based on isolation level
func (te *Engine) deleteWithTxn(txn *Transaction, key string) error {
	// TODO: Lab 5.2
	return nil
}

// PutBatch inserts multiple key-value pairs using transaction context
func (te *Engine) PutBatchWithTxn(txn *Transaction, kvs []common.KVPair) error {
	// TODO: Lab 5.2

	return nil
}

// GetBatch retrieves multiple values by keys using transaction context
func (te *Engine) GetBatchWithTxn(txn *Transaction, keys []string) (map[string]string, error) {
	// TODO: Lab 5.2

	return nil, nil
}

// DeleteBatch marks multiple keys as deleted using transaction context
func (te *Engine) DeleteBatch(txn *Transaction, keys []string) error {
	// TODO: Lab 5.2

	return nil
}
