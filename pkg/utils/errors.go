package utils

import "errors"

// Common errors used throughout the project
var (
	// Bloom filter errors
	ErrIncompatibleBloomFilters = errors.New("incompatible bloom filters")

	// File errors
	ErrFileNotFound    = errors.New("file not found")
	ErrFileCorrupted   = errors.New("file corrupted")
	ErrInvalidChecksum = errors.New("invalid checksum")

	// Cache errors
	ErrCacheFull = errors.New("cache is full")
	ErrCacheMiss = errors.New("cache miss")

	// SST errors
	ErrSSTNotFound      = errors.New("SST not found")
	ErrSSTCorrupted     = errors.New("SST file corrupted")
	ErrInvalidSSTFile   = errors.New("invalid SST file format")
	ErrInvalidMetadata  = errors.New("invalid metadata format")
	ErrBlockNotFound    = errors.New("block not found")
	ErrEmptySST         = errors.New("cannot build empty SST")
	ErrInvalidBlockSize = errors.New("invalid block size")
	ErrCorruptedFile    = errors.New("corrupted SST file")

	// WAL errors
	ErrWALCorrupted = errors.New("WAL file corrupted")
	ErrWALClosed    = errors.New("WAL is closed")

	// Transaction errors
	ErrTransactionAborted   = errors.New("transaction aborted")
	ErrTransactionConflict  = errors.New("transaction conflict")
	ErrReadOnlyTransaction  = errors.New("read-only transaction")
	ErrTransactionNotActive = errors.New("transaction is not active")
	ErrTooManyActiveTxns    = errors.New("too many active transactions")
	ErrIsolationViolation   = errors.New("isolation level violation")

	// General errors
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidKey  = errors.New("invalid key")
	ErrInvalidData = errors.New("invalid data")
	ErrClosed      = errors.New("resource is closed")

	ErrEngineClosed = errors.New("LSM engine is closed")
)
