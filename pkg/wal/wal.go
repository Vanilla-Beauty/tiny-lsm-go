package wal

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	
	// Context for background cleanup
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	
	// Checkpoint transaction ID - records with TxnID <= this are safe to clean
	checkpointTxnID uint64
}

// New creates a new WAL instance
func New(config *Config, checkpointTxnID uint64) (*WAL, error) {
	if config == nil {
		config = DefaultConfig()
	}
	
	// Ensure log directory exists
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory %s: %w", config.LogDir, err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	wal := &WAL{
		config:          config,
		checkpointTxnID: checkpointTxnID,
		ctx:             ctx,
		cancelFunc:      cancel,
	}
	
	// Find the next sequence number
	if err := wal.initCurrentFile(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize WAL file: %w", err)
	}
	
	// Start background cleanup goroutine
	wal.wg.Add(1)
	go wal.cleanupLoop()
	
	return wal, nil
}

// Close closes the WAL and stops background tasks
func (w *WAL) Close() error {
	// Cancel background tasks
	w.cancelFunc()
	w.wg.Wait()
	
	// Flush remaining buffer
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if err := w.flushBuffer(); err != nil {
		log.Printf("Error flushing buffer during close: %v", err)
	}
	
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			log.Printf("Error syncing file during close: %v", err)
		}
		w.currentFile.Close()
	}
	
	return nil
}

// Log adds records to the WAL
func (w *WAL) Log(records []*Record, forceFlush bool) error {
	if len(records) == 0 && !forceFlush {
		return nil
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Add records to buffer
	w.buffer = append(w.buffer, records...)
	
	// Check if we should flush
	if len(w.buffer) >= w.config.BufferSize || forceFlush {
		return w.flushBuffer()
	}
	
	return nil
}

// Flush forces all buffered records to be written to disk
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	return w.flushBuffer()
}

// SetCheckpointTxnID updates the checkpoint transaction ID
func (w *WAL) SetCheckpointTxnID(txnID uint64) {
	w.mu.Lock()
	w.checkpointTxnID = txnID
	w.mu.Unlock()
}

// Recover reads and returns all records from WAL files that are after the checkpoint
func Recover(logDir string, checkpointTxnID uint64) (map[uint64][]*Record, error) {
	result := make(map[uint64][]*Record)
	
	// Check if log directory exists
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return result, nil
	}
	
	// Get all WAL files
	walFiles, err := getWALFiles(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	// Sort by sequence number
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].seq < walFiles[j].seq
	})
	
	// Read all records from WAL files
	for _, walFile := range walFiles {
		filePath := filepath.Join(logDir, walFile.name)
		file, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file %s: %w", filePath, err)
		}
		
		data, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL file %s: %w", filePath, err)
		}
		
		records, err := DecodeRecords(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode records from %s: %w", filePath, err)
		}
		
		// Filter records by checkpoint
		for _, record := range records {
			if record.TxnID > checkpointTxnID {
				result[record.TxnID] = append(result[record.TxnID], record)
			}
		}
	}
	
	return result, nil
}

// initCurrentFile initializes the current WAL file
func (w *WAL) initCurrentFile() error {
	walFiles, err := getWALFiles(w.config.LogDir)
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	// Find the highest sequence number
	maxSeq := int64(-1)
	for _, walFile := range walFiles {
		if walFile.seq > maxSeq {
			maxSeq = walFile.seq
		}
	}
	
	// Create new file with next sequence number
	w.currentSeq = maxSeq + 1
	return w.createNewFile()
}

// createNewFile creates a new WAL file
func (w *WAL) createNewFile() error {
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	
	filename := fmt.Sprintf("wal.%d", w.currentSeq)
	filePath := filepath.Join(w.config.LogDir, filename)
	
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file %s: %w", filePath, err)
	}
	
	w.currentFile = file
	return nil
}

// flushBuffer writes all buffered records to disk
func (w *WAL) flushBuffer() error {
	if len(w.buffer) == 0 {
		return nil
	}
	
	// Encode all records
	for _, record := range w.buffer {
		data := record.Encode()
		if _, err := w.currentFile.Write(data); err != nil {
			return fmt.Errorf("failed to write record to WAL: %w", err)
		}
	}
	
	// Sync to disk
	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	
	// Clear buffer
	w.buffer = w.buffer[:0]
	
	// Check if we need to rotate file
	if err := w.checkFileRotation(); err != nil {
		return fmt.Errorf("failed to rotate WAL file: %w", err)
	}
	
	return nil
}

// checkFileRotation rotates the WAL file if it's too large
func (w *WAL) checkFileRotation() error {
	if w.currentFile == nil {
		return nil
	}
	
	stat, err := w.currentFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}
	
	if stat.Size() >= w.config.FileSizeLimit {
		w.currentSeq++
		return w.createNewFile()
	}
	
	return nil
}

// cleanupLoop runs in a background goroutine to clean old WAL files
func (w *WAL) cleanupLoop() {
	defer w.wg.Done()
	
	ticker := time.NewTicker(w.config.CleanInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.cleanOldFiles()
		}
	}
}

// cleanOldFiles removes WAL files that contain only committed transactions
func (w *WAL) cleanOldFiles() {
	w.mu.Lock()
	checkpointTxnID := w.checkpointTxnID
	currentSeq := w.currentSeq
	w.mu.Unlock()
	
	walFiles, err := getWALFiles(w.config.LogDir)
	if err != nil {
		log.Printf("Error listing WAL files during cleanup: %v", err)
		return
	}
	
	// Keep the current file and at least one previous file
	for _, walFile := range walFiles {
		// Don't clean the current file
		if walFile.seq >= currentSeq {
			continue
		}
		
		// Check if this file contains only transactions <= checkpointTxnID
		if w.canCleanFile(walFile.name, checkpointTxnID) {
			filePath := filepath.Join(w.config.LogDir, walFile.name)
			if err := os.Remove(filePath); err != nil {
				log.Printf("Error removing WAL file %s: %v", filePath, err)
			}
		}
	}
}

// canCleanFile checks if a WAL file can be safely cleaned
func (w *WAL) canCleanFile(filename string, checkpointTxnID uint64) bool {
	filePath := filepath.Join(w.config.LogDir, filename)
	
	file, err := os.Open(filePath)
	if err != nil {
		return false
	}
	defer file.Close()
	
	data, err := io.ReadAll(file)
	if err != nil {
		return false
	}
	
	records, err := DecodeRecords(data)
	if err != nil {
		return false
	}
	
	// Check if all transactions in this file are <= checkpointTxnID
	for _, record := range records {
		if record.TxnID > checkpointTxnID {
			return false
		}
	}
	
	return true
}

// walFileInfo holds information about a WAL file
type walFileInfo struct {
	name string
	seq  int64
}

// getWALFiles returns a list of WAL files in the directory
func getWALFiles(logDir string) ([]walFileInfo, error) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	
	var walFiles []walFileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		
		name := entry.Name()
		if !strings.HasPrefix(name, "wal.") {
			continue
		}
		
		// Extract sequence number
		seqStr := strings.TrimPrefix(name, "wal.")
		seq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			continue
		}
		
		walFiles = append(walFiles, walFileInfo{
			name: name,
			seq:  seq,
		})
	}
	
	return walFiles, nil
}
