package lsm

import (
	"sync"
	"sync/atomic"
	"time"

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
	id           uint64
	state        TransactionState
	isolation    IsolationLevel
	startTime    time.Time
	commitTime   time.Time
	readTxnID    uint64 // Transaction ID used for reads (for snapshot isolation)
	manager      *TransactionManager
	mu           sync.RWMutex

	// Transaction-specific data storage for non READ_UNCOMMITTED levels
	tempMap      map[string]string            // Temporary storage for uncommitted writes
	readMap      map[string]*ReadRecord       // Read history for REPEATABLE_READ and SERIALIZABLE
	rollbackMap  map[string]*RollbackRecord   // Rollback information for READ_UNCOMMITTED
	operations   []*wal.Record               // WAL operations for this transaction
}

// TransactionManager manages database transactions
type TransactionManager struct {
	engine          *Engine
	nextTxnID       uint64
	activeTxns      map[uint64]*Transaction
	committedTxns   map[uint64]*Transaction
	globalReadTxnID uint64 // Global read transaction ID for snapshot isolation
	mu              sync.RWMutex
	config          *TransactionConfig
	persistence     *TransactionPersistence // Transaction state persistence
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
		engine:          engine,
		nextTxnID:       1,
		activeTxns:      make(map[uint64]*Transaction),
		committedTxns:   make(map[uint64]*Transaction),
		globalReadTxnID: 1,
		config:          config,
	}

	// Start cleanup goroutine
	go mgr.cleanupWorker()

	return mgr
}

// NewTransactionManagerWithPersistence creates a new transaction manager with persistence support
func NewTransactionManagerWithPersistence(engine *Engine, config *TransactionConfig, dataDir string) (*TransactionManager, error) {
	if config == nil {
		config = DefaultTransactionConfig()
	}

	mgr := &TransactionManager{
		engine:          engine,
		nextTxnID:       1,
		activeTxns:      make(map[uint64]*Transaction),
		committedTxns:   make(map[uint64]*Transaction),
		globalReadTxnID: 1,
		config:          config,
		persistence:     NewTransactionPersistence(dataDir),
	}

	// Try to recover previous transaction state
	if err := mgr.persistence.RecoverTransactionState(mgr); err != nil {
		return nil, err
	}

	// Start cleanup goroutine
	go mgr.cleanupWorker()

	return mgr, nil
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
		return nil, ErrTooManyActiveTxns
	}

	txnID := atomic.AddUint64(&m.nextTxnID, 1)
	readTxnID := atomic.LoadUint64(&m.globalReadTxnID)

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
	return atomic.LoadUint64(&m.nextTxnID)
}

// cleanupWorker runs periodically to clean up old committed transactions
func (m *TransactionManager) cleanupWorker() {
	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		m.cleanupOldTransactions()
	}
}

// cleanupOldTransactions removes old committed transactions from memory
func (m *TransactionManager) cleanupOldTransactions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-m.config.CleanupInterval * 2)
	
	for txnID, txn := range m.committedTxns {
		if txn.commitTime.Before(cutoff) {
			delete(m.committedTxns, txnID)
		}
	}
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
		return ErrTransactionNotActive
	}

	// Handle different isolation levels
	switch t.isolation {
	case ReadUncommitted:
		// READ_UNCOMMITTED: Data already written, just add commit record
		t.operations = append(t.operations, wal.NewCommitRecord(t.id))

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

		// Apply changes to database
		if err := t.applyChanges(); err != nil {
			return err
		}

		// Add commit record
		t.operations = append(t.operations, wal.NewCommitRecord(t.id))
	}

	// Mark transaction as committed
	t.state = TxnCommitted
	t.commitTime = time.Now()

	// Move from active to committed transactions
	t.manager.mu.Lock()
	delete(t.manager.activeTxns, t.id)
	t.manager.committedTxns[t.id] = t
	
	// Update global read transaction ID for new snapshots
	atomic.StoreUint64(&t.manager.globalReadTxnID, t.id)
	t.manager.mu.Unlock()

	// Force flush to ensure durability
	return t.manager.engine.Flush()
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != TxnActive {
		return ErrTransactionNotActive
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
	Value   string
	TxnID   uint64
	Exists  bool
}

// RollbackRecord represents rollback information for READ_UNCOMMITTED
type RollbackRecord struct {
	Value   string
	TxnID   uint64
	Exists  bool
}

// KVPair represents a key-value pair for batch operations
type KVPair struct {
	Key   string
	Value string
}

// detectConflicts checks for write conflicts for REPEATABLE_READ and SERIALIZABLE isolation levels
func (t *Transaction) detectConflicts() error {
	if t.isolation != RepeatableRead && t.isolation != Serializable {
		return nil // No conflict detection needed
	}

	// Check each key in tempMap for conflicts
	for key := range t.tempMap {
		// Check if a later transaction has modified this key
		_, exists, err := t.manager.engine.GetWithTxn(key, 0) // Get without transaction filtering
		if err != nil {
			return err
		}

		if exists {
			// Get the transaction ID that last modified this key
			// This is a simplified approach - in a full implementation,
			// we would need to track the txnID for each modification
			// For now, we use a simple heuristic based on the global read transaction ID
			globalReadTxnID := atomic.LoadUint64(&t.manager.globalReadTxnID)
			if globalReadTxnID > t.id {
				// A later transaction has committed changes, potential conflict
				return ErrWriteConflict
			}
		}
	}

	return nil
}

// applyChanges applies the temporary changes to the database
func (t *Transaction) applyChanges() error {
	for key, value := range t.tempMap {
		if value == "" {
			// Empty string marks deletion
			if err := t.manager.engine.DeleteWithTxn(key, t.id); err != nil {
				return err
			}
		} else {
			// Regular put operation
			if err := t.manager.engine.PutWithTxn(key, value, t.id); err != nil {
				return err
			}
		}
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
			if err := t.manager.engine.PutWithTxn(key, record.Value, record.TxnID); err != nil {
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

// TransactionIterator extends the basic iterator interface for transactions
type TransactionIterator interface {
	// Valid returns true if the iterator is pointing to a valid entry
	Valid() bool
	// Key returns the key of the current entry
	Key() string
	// Value returns the value of the current entry
	Value() string
	// Next advances the iterator to the next entry
	Next()
	// Seek positions the iterator at the first entry with key >= target
	Seek(key string)
	// SeekToFirst positions the iterator at the first entry
	SeekToFirst()
	// SeekToLast positions the iterator at the last entry
	SeekToLast()
	// Close releases any resources held by the iterator
	Close()
}
