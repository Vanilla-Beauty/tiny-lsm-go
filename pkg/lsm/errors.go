package lsm

import "errors"

var (
	// Engine errors
	ErrEngineClosed    = errors.New("LSM engine is closed")
	ErrInvalidConfig   = errors.New("invalid configuration")
	ErrRecoveryFailed  = errors.New("recovery failed")
	
	// Level management errors
	ErrInvalidLevel    = errors.New("invalid level")
	ErrLevelFull       = errors.New("level is full")
	ErrSSTNotFound     = errors.New("SST not found")
	
	// Compaction errors
	ErrCompactionFailed = errors.New("compaction failed")
	ErrNoCompactionTask = errors.New("no compaction task available")
	
	// Iterator errors
	ErrIteratorClosed   = errors.New("iterator is closed")
	ErrInvalidIterator  = errors.New("invalid iterator state")
	
	// Transaction errors
	ErrTransactionNotActive = errors.New("transaction is not active")
	ErrTransactionNotFound  = errors.New("transaction not found")
	ErrTooManyActiveTxns    = errors.New("too many active transactions")
	ErrTransactionTimeout   = errors.New("transaction timeout")
	ErrTransactionAborted   = errors.New("transaction aborted")
	ErrTransactionCommitted = errors.New("transaction already committed")
	
	// Concurrency errors
	ErrWriteConflict        = errors.New("write conflict detected")
	ErrReadConflict         = errors.New("read conflict detected")
	ErrDeadlockDetected     = errors.New("deadlock detected")
	
	// Isolation level errors
	ErrIsolationViolation   = errors.New("isolation level violation")
	ErrPhantomRead          = errors.New("phantom read detected")
	ErrNonRepeatableRead    = errors.New("non-repeatable read detected")
	ErrDirtyRead            = errors.New("dirty read detected")
)
