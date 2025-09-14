package wal

import (
	"os"
	"sync"
	"time"
)

// Config holds configuration for the WAL
type Config struct {
	// LogDir is the directory where WAL files are stored
	LogDir string
	// BufferSize is the number of records to buffer before forcing a flush
	BufferSize int
	// FileSizeLimit is the maximum size of a single WAL file in bytes
	FileSizeLimit int64
	// CleanInterval is the interval for cleaning old WAL files
	CleanInterval time.Duration
}

// DefaultConfig returns a default WAL configuration
func DefaultConfig() *Config {
	return &Config{
		LogDir:        "./wal",
		BufferSize:    128,
		FileSizeLimit: 4096, // 4KB for testing, should be larger in production
		CleanInterval: 60 * time.Second,
	}
}

// WAL manages write-ahead log files
type WAL struct {
	config      *Config
	mu          sync.Mutex
	buffer      []*Record
	currentFile *os.File
	currentSeq  int64
}

// New creates a new WAL instance
func New(config *Config, checkpointTxnID uint64) (*WAL, error) {
	// TODO: Lab 5.4
	return nil, nil
}

// Close closes the WAL and stops background tasks
func (w *WAL) Close() error {
	// TODO: Lab 5.4
	return nil
}

// Log adds records to the WAL
func (w *WAL) Log(records []*Record, forceFlush bool) error {
	// TODO: Lab 5.4
	return nil
}

// Flush forces all buffered records to be written to disk
func (w *WAL) Flush() error {
	// TODO Lab 5.4

	return nil
}

// Recover reads and returns all records from WAL files that are after the checkpoint
func Recover(logDir string, checkpointTxnID uint64) (map[uint64][]*Record, error) {
	// TODO: Lab 5.4

	return nil, nil
}

// CleanOldFiles removes WAL files that contain only committed transactions
func (w *WAL) CleanOldFiles(activeTxnIDs map[uint64]struct{}) {
	// TODO: Lab 5.4
}
