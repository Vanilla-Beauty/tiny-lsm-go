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
	ErrSSTNotFound     = errors.New("SST not found")
	ErrSSTCorrupted    = errors.New("SST file corrupted")
	ErrInvalidSSTFile  = errors.New("invalid SST file format")
	
	// WAL errors
	ErrWALCorrupted = errors.New("WAL file corrupted")
	ErrWALClosed    = errors.New("WAL is closed")
	
	// Transaction errors
	ErrTransactionAborted  = errors.New("transaction aborted")
	ErrTransactionConflict = errors.New("transaction conflict")
	ErrReadOnlyTransaction = errors.New("read-only transaction")
	
	// General errors
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidKey  = errors.New("invalid key")
	ErrInvalidData = errors.New("invalid data")
	ErrClosed      = errors.New("resource is closed")
)
