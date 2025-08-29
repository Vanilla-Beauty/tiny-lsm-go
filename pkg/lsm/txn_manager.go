package lsm

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"tiny-lsm-go/pkg/common"
	"tiny-lsm-go/pkg/utils"
	"tiny-lsm-go/pkg/wal"
)

// IsolationLevel represents the transaction isolation level
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted prevents dirty reads
	ReadCommitted
	// RepeatableRead prevents dirty and non-repeatable reads
	RepeatableRead
	// Serializable prevents dirty reads, non-repeatable reads, and phantom reads
	Serializable
)

// TransactionState represents the state of a transaction
type TransactionState int

const (
	// TxnActive transaction is active
	TxnActive TransactionState = iota
	// TxnCommitted transaction is committed
	TxnCommitted
	// TxnAborted transaction is aborted/rolled back
	TxnAborted
)

// Transaction represents a database transaction
type Transaction struct {
	id         uint64
	state      TransactionState
	isolation  IsolationLevel
	startTime  time.Time
	commitTime time.Time
	readTxnID  uint64 // Transaction ID used for reads (for snapshot isolation)
	manager    *TransactionManager
	mu         sync.RWMutex

	// Transaction-specific data storage for non READ_UNCOMMITTED levels
	tempMap     map[string]string          // Temporary storage for uncommitted writes
	readMap     map[string]*ReadRecord     // Read history for REPEATABLE_READ and SERIALIZABLE
	rollbackMap map[string]*RollbackRecord // Rollback information for READ_UNCOMMITTED
	operations  []*wal.Record              // WAL operations for this transaction
}

// TransactionManager manages database transactions
type TransactionManager struct {
	engine        *Engine
	activeTxns    map[uint64]*Transaction
	committedTxns map[uint64]*Transaction
	mu            sync.RWMutex
	config        *TransactionConfig
}

// TransactionConfig holds transaction manager configuration
type TransactionConfig struct {
	// DefaultIsolationLevel is the default isolation level for new transactions
	DefaultIsolationLevel IsolationLevel
	// MaxActiveTxns is the maximum number of active transactions
	MaxActiveTxns int
	// TxnTimeout is the timeout for transactions
	TxnTimeout time.Duration
	// CleanupInterval is the interval for cleaning up old committed transactions
	CleanupInterval time.Duration
}

// DefaultTransactionConfig returns a default transaction configuration
func DefaultTransactionConfig() *TransactionConfig {
	return &TransactionConfig{
		DefaultIsolationLevel: ReadCommitted,
		MaxActiveTxns:         1000,
		TxnTimeout:            30 * time.Second,
		CleanupInterval:       60 * time.Second,
	}
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(engine *Engine, config *TransactionConfig) *TransactionManager {
	if config == nil {
		config = DefaultTransactionConfig()
	}

	mgr := &TransactionManager{
		engine:        engine,
		activeTxns:    make(map[uint64]*Transaction),
		committedTxns: make(map[uint64]*Transaction),
		config:        config,
	}

	mgr.loadTxnStatus()

	return mgr
}

// Begin starts a new transaction with default isolation level
func (m *TransactionManager) Begin() (*Transaction, error) {
	return m.BeginWithIsolation(m.config.DefaultIsolationLevel)
}

// BeginWithIsolation starts a new transaction with specified isolation level
func (m *TransactionManager) BeginWithIsolation(isolation IsolationLevel) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we exceed maximum active transactions
	if len(m.activeTxns) >= m.config.MaxActiveTxns {
		return nil, utils.ErrTooManyActiveTxns
	}

	txnID := atomic.AddUint64(&m.engine.metadata.NextTxnID, 1) - 1
	readTxnID := atomic.LoadUint64(&m.engine.metadata.GlobalReadTxnID)

	txn := &Transaction{
		id:        txnID,
		state:     TxnActive,
		isolation: isolation,
		startTime: time.Now(),
		readTxnID: readTxnID,
		manager:   m,
		// Initialize transaction-specific maps
		tempMap:     make(map[string]string),
		readMap:     make(map[string]*ReadRecord),
		rollbackMap: make(map[string]*RollbackRecord),
		operations:  []*wal.Record{wal.NewCreateRecord(txnID)},
	}

	m.activeTxns[txnID] = txn
	return txn, nil
}

// GetTransaction returns a transaction by ID
func (m *TransactionManager) GetTransaction(txnID uint64) (*Transaction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if txn, exists := m.activeTxns[txnID]; exists {
		return txn, true
	}

	if txn, exists := m.committedTxns[txnID]; exists {
		return txn, true
	}

	return nil, false
}

// GetActiveTransactionCount returns the number of active transactions
func (m *TransactionManager) GetActiveTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.activeTxns)
}

// GetCommittedTransactionCount returns the number of committed transactions
func (m *TransactionManager) GetCommittedTransactionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.committedTxns)
}

// GetNextTxnID returns the next transaction ID that will be assigned
func (m *TransactionManager) GetNextTxnID() uint64 {
	return atomic.LoadUint64(&m.engine.metadata.NextTxnID)
}

func (m *TransactionManager) updateFlushedTxn(txnID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove flushed transactions
	delete(m.committedTxns, txnID)
}

func (m *TransactionManager) syncTxnStatus() error {
	data, err := json.MarshalIndent(m.activeTxns, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal active transactions: %v", err)
		return err
	}

	filePath := filepath.Join(m.engine.dataDir, common.CommittedTxnFile)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write committed transactions file: %w", err)
	}
	return nil
}

func (m *TransactionManager) loadTxnStatus() error {
	data, err := os.ReadFile(filepath.Join(m.engine.dataDir, common.CommittedTxnFile))
	if err != nil {
		return fmt.Errorf("failed to read committed transactions file: %w", err)
	}
	var records map[uint64]*Transaction
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("failed to unmarshal committed transactions: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committedTxns = records
	return nil
}

func (m *TransactionManager) needRepay(txnID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exist := m.committedTxns[txnID]
	return exist
}

func (m *TransactionManager) GetactiveTxnIDs() map[uint64]struct{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	activeIDs := make(map[uint64]struct{})
	for id := range m.activeTxns {
		activeIDs[id] = struct{}{}
	}
	return activeIDs
}

func (m *TransactionManager) Close() error {
	return m.syncTxnStatus()
}

// ID returns the transaction ID
func (t *Transaction) ID() uint64 {
	return t.id
}

// State returns the transaction state
func (t *Transaction) State() TransactionState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// IsolationLevel returns the transaction isolation level
func (t *Transaction) IsolationLevel() IsolationLevel {
	return t.isolation
}

// StartTime returns the transaction start time
func (t *Transaction) StartTime() time.Time {
	return t.startTime
}

// CommitTime returns the transaction commit time (only valid for committed transactions)
func (t *Transaction) CommitTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.commitTime
}

// ReadTxnID returns the transaction ID used for reads (snapshot)
// For active transactions, use own ID to see own writes
// For committed/aborted, use original snapshot
func (t *Transaction) ReadTxnID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.state == TxnActive {
		// Active transactions can read their own writes
		return t.id
	}

	// Use snapshot for non-active transactions
	return t.readTxnID
}

// WriteTxnID returns the transaction ID used for writes
func (t *Transaction) WriteTxnID() uint64 {
	return t.id
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != TxnActive {
		return utils.ErrTransactionNotActive
	}

	// Handle different isolation levels
	switch t.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Data already written, just add commit record

	default:
		// Other isolation levels: Need conflict detection and batch apply
		if err := t.detectConflicts(); err != nil {
			// Conflict detected, abort transaction
			t.state = TxnAborted
			t.manager.mu.Lock()
			delete(t.manager.activeTxns, t.id)
			t.manager.mu.Unlock()
			return err
		}
	}

	// Add commit record
	t.operations = append(t.operations, wal.NewCommitRecord(t.id))

	// Write all operations to WAL
	if err := t.manager.engine.wal.Log(t.operations, true); err != nil {
		return fmt.Errorf("failed to write transaction operations to WAL: %w", err)
	}

	// Apply changes to database
	if err := t.applyChanges(); err != nil {
		return err
	}

	// Mark transaction as committed
	t.state = TxnCommitted
	t.commitTime = time.Now()

	// Move from active to committed transactions
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t.id)
	t.manager.committedTxns[t.id] = t

	// Update global read transaction ID for new snapshots
	atomic.StoreUint64(&t.manager.engine.metadata.GlobalReadTxnID, t.id)
	t.manager.mu.Unlock()

	// Force flush to ensure durability
	return t.manager.engine.Flush()
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != TxnActive {
		return utils.ErrTransactionNotActive
	}

	// Handle rollback based on isolation level
	if t.isolation == ReadUncommitted {
		// READ_UNCOMMITTED: Need to actively restore previous values
		if err := t.rollbackChanges(); err != nil {
			return err
		}

		// Add rollback record to WAL
		t.operations = append(t.operations, wal.NewRollbackRecord(t.id))
	}
	// For other isolation levels, data is in tempMap and will be discarded

	// Mark transaction as aborted
	t.state = TxnAborted

	// Remove from active transactions
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t.id)
	t.manager.mu.Unlock()

	return nil
}

// IsActive returns true if the transaction is active
func (t *Transaction) IsActive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnActive
}

// IsCommitted returns true if the transaction is committed
func (t *Transaction) IsCommitted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnCommitted
}

// IsAborted returns true if the transaction is aborted
func (t *Transaction) IsAborted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == TxnAborted
}

// ReadRecord represents a read operation record for isolation levels
type ReadRecord struct {
	Value  string
	TxnID  uint64
	Exists bool
}

// RollbackRecord represents rollback information for READ_UNCOMMITTED
type RollbackRecord struct {
	Value  string
	TxnID  uint64
	Exists bool
}

// detectConflicts checks for write conflicts for REPEATABLE_READ and SERIALIZABLE isolation levels
func (t *Transaction) detectConflicts() error {
	if t.isolation != RepeatableRead && t.isolation != Serializable {
		return nil // No conflict detection needed
	}

	// Check each key in tempMap for conflicts
	for key := range t.tempMap {
		// Check if a later transaction has modified this key
		_, exists, err := t.manager.engine.GetWithTxnID(key, 0) // Get without transaction filtering
		if err != nil {
			return err
		}

		if exists {
			// Get the transaction ID that last modified this key
			// This is a simplified approach - in a full implementation,
			// we would need to track the txnID for each modification
			// For now, we use a simple heuristic based on the global read transaction ID
			globalReadTxnID := atomic.LoadUint64(&t.manager.engine.metadata.GlobalReadTxnID)
			if globalReadTxnID > t.id {
				// A later transaction has committed changes, potential conflict
				return utils.ErrTransactionConflict
			}
		}
	}

	return nil
}

// applyChanges applies the temporary changes to the database
func (t *Transaction) applyChanges() error {
	if t.isolation == ReadUncommitted {
		return nil // No change application needed for other isolation levels
	}

	for key, value := range t.tempMap {
		if value == "" {
			// Empty string marks deletion
			if err := t.manager.engine.DeleteWithTxn(key, t.id); err != nil {
				return err
			}
		} else {
			// Regular put operation
			if err := t.manager.engine.PutWithTxnID(key, value, t.id); err != nil {
				return err
			}
		}
	}
	// add a marker to denote end of transaction
	if err := t.manager.engine.PutWithTxnID("", "", t.id); err != nil {
		return err
	}

	return nil
}

// rollbackChanges rolls back changes for READ_UNCOMMITTED transactions
func (t *Transaction) rollbackChanges() error {
	if t.isolation != ReadUncommitted {
		return nil // No rollback needed for other isolation levels
	}

	// Restore previous values
	for key, record := range t.rollbackMap {
		if record.Exists {
			// Restore previous value
			if err := t.manager.engine.PutWithTxnID(key, record.Value, record.TxnID); err != nil {
				return err
			}
		} else {
			// Key didn't exist before, delete it
			if err := t.manager.engine.DeleteWithTxn(key, t.id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Transaction) GetTxnID() uint64 {
	return t.id
}
