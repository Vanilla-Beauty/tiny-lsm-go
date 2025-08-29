package lsm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"tiny-lsm-go/pkg/cache"
	"tiny-lsm-go/pkg/common"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/iterator"
	"tiny-lsm-go/pkg/memtable"
	"tiny-lsm-go/pkg/sst"
	"tiny-lsm-go/pkg/utils"
	"tiny-lsm-go/pkg/wal"
)

// Engine represents the LSM-tree storage engine
type Engine struct {
	// Core components
	config      *config.Config
	dataDir     string
	memTable    *memtable.MemTable
	blockCache  *cache.BlockCache
	fileManager *utils.FileManager

	// SST management
	levels   *LevelManager
	metadata *EngineMetadata

	// WAL management
	wal *wal.WAL

	// Metadata persistence
	metadataFile string

	// Background workers
	checkCh chan struct{}

	wg sync.WaitGroup

	// Statistics
	stats *EngineStatistics

	txnManager *TransactionManager

	// State
	closed                bool
	flushAndCompactByHand bool // during test, disable background flush and compact to make it easy to debug

	// Context for controlling background goroutines
	ctx    context.Context
	cancel context.CancelFunc
}

func (e *Engine) initTxnManager(config *TransactionConfig) error {
	if config == nil {
		config = DefaultTransactionConfig()
	}
	manager := NewTransactionManager(e, config)
	e.txnManager = manager
	return nil
}

// NewEngine creates a new LSM engine
func NewEngine(cfg *config.Config, dataDir string) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Initialize file manager
	fileManager := utils.NewFileManager(dataDir)

	// Initialize block cache
	blockCache := cache.NewBlockCache(cfg.GetBlockCacheCapacity())

	// Initialize memtable
	mt := memtable.New()

	// Initialize level manager
	levels := NewLevelManager(cfg, fileManager, blockCache)

	// Initialize WAL
	walDir := filepath.Join(dataDir, "wal")
	walConfig := &wal.Config{
		LogDir:        walDir,
		BufferSize:    int(cfg.GetWALBufferSize() / 64), // Convert bytes to record count estimate
		FileSizeLimit: cfg.GetWALFileSizeLimit(),
		CleanInterval: time.Duration(cfg.GetWALCleanInterval()) * time.Second,
	}

	walInstance, err := wal.New(walConfig, 0) // Start with checkpoint 0
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	engine := &Engine{
		config:      cfg,
		dataDir:     dataDir,
		memTable:    mt,
		blockCache:  blockCache,
		fileManager: fileManager,
		levels:      levels,
		wal:         walInstance,
		metadata: &EngineMetadata{
			NextSSTID:       0,
			NextTxnID:       1,
			GlobalReadTxnID: 1,
		},

		stats:        &EngineStatistics{},
		metadataFile: filepath.Join(dataDir, "metadata"),
		closed:       false,
		checkCh:      make(chan struct{}, 1),
	}
	engine.initTxnManager(nil)

	// Create context for background workers
	engine.ctx, engine.cancel = context.WithCancel(context.Background())

	// Load metadata if exists
	if err := loadMetadata(engine); err != nil {
		fmt.Printf("failed to load metadata: %v\n", err)
	}

	// Recover from existing data if any
	if err := engine.recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	// Save initial metadata
	if err := saveMetadata(engine); err != nil {
		return nil, fmt.Errorf("failed to save initial metadata: %w", err)
	}

	// Start background workers
	engine.startBackgroundWorkers()

	return engine, nil
}

// recover recovers the engine state from disk
func (e *Engine) recover() error {
	// First, recover SST files
	if err := e.levels.LoadExistingSSTs(); err != nil {
		return fmt.Errorf("failed to load existing SST files: %w", err)
	}

	// Then, recover from WAL
	if e.wal != nil {
		if err := e.recoverFromWAL(); err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	return nil
}

// recoverFromWAL recovers uncommitted transactions from WAL logs
func (e *Engine) recoverFromWAL() error {
	// Read WAL records
	walDir := filepath.Join(e.dataDir, "wal")
	recordsByTxn, err := wal.Recover(walDir, 0) // Recover from checkpoint 0
	if err != nil {
		return fmt.Errorf("failed to read WAL records: %w", err)
	}

	if len(recordsByTxn) == 0 {
		return nil // No records to recover
	}

	fmt.Printf("🔄 Check %d transactions from WAL...\n", len(recordsByTxn))

	// Process each transaction
	hasRepayed := false
	for txnID, records := range recordsByTxn {
		if e.txnManager.needRepay(txnID) {
			if err := e.replayTransaction(txnID, records); err != nil {
				fmt.Printf("Warning: failed to replay transaction %d: %v\n", txnID, err)
				os.Exit(1)
			}
			hasRepayed = true
			fmt.Printf(" ✅ Replayed record %+v.\n", records)
		}
	}
	if hasRepayed {
		fmt.Printf("✅ WAL recovery completed. Next transaction ID: %d\n", e.metadata.NextTxnID)
	} else {
		fmt.Println("✅ WAL recovery completed. No transactions to replay.")
	}

	return nil
}

// replayTransaction replays a single transaction from WAL records
func (e *Engine) replayTransaction(txnID uint64, records []*wal.Record) error {
	if len(records) == 0 {
		return nil
	}

	// Check if transaction was committed or rolled back
	var committed, rolledBack bool
	for _, record := range records {
		switch record.OpType {
		case wal.OpCommit:
			committed = true
		case wal.OpRollback:
			rolledBack = true
		}
	}

	// If transaction was committed, replay all operations
	if committed {
		fmt.Printf("  Replaying committed transaction %d...\n", txnID)
		for _, record := range records {
			switch record.OpType {
			case wal.OpPut:
				if err := e.memTable.Put(record.Key, record.Value, txnID); err != nil {
					return fmt.Errorf("failed to replay PUT %s: %w", record.Key, err)
				}
			case wal.OpDelete:
				if err := e.memTable.Delete(record.Key, txnID); err != nil {
					return fmt.Errorf("failed to replay DELETE %s: %w", record.Key, err)
				}
			}
		}
		return nil
	}

	// If transaction was rolled back or incomplete, ignore it
	if rolledBack {
		fmt.Printf("  Skipping rolled back transaction %d\n", txnID)
	} else {
		fmt.Printf("  Skipping incomplete transaction %d\n", txnID)
	}

	return nil
}

// startBackgroundWorkers starts the background flush and compaction workers
func (e *Engine) startBackgroundWorkers() {
	// Flush worker
	e.wg.Add(3)
	go e.flushWorker()
	go e.cleanWalWorker()
	go e.syncTxnStatusWorker()

	if e.config.Compaction.EnableAutoCompaction {
		e.wg.Add(1)
		go e.compactionWorker()
	}
}

// Put inserts or updates a key-value pair
func (e *Engine) Put(key, value string) error {
	txnID := atomic.AddUint64(&e.metadata.NextTxnID, 1) - 1
	return e.PutWithTxnID(key, value, txnID)
}

// PutWithTxn inserts or updates a key-value pair with transaction ID
func (e *Engine) PutWithTxnID(key, value string, txnID uint64) error {
	if e.closed {
		return utils.ErrClosed
	}

	err := e.memTable.Put(key, value, txnID)
	if err != nil {
		return err
	}

	// Check if we need to freeze the current memtable
	if int64(e.memTable.GetCurrentSize()) >= e.config.GetPerMemSizeLimit() {
		e.freezeMemTableIfNeeded()
	}

	atomic.AddUint64(&e.stats.Writes, 1)
	atomic.StoreUint64(&e.stats.MemTableSize, uint64(e.memTable.GetCurrentSize()))
	atomic.StoreUint64(&e.stats.FrozenTableSize, uint64(e.memTable.GetFrozenSize()))

	return nil
}

// PutBatch inserts or updates multiple key-value pairs atomically
func (e *Engine) PutBatch(kvs []common.KVPair) error {
	txnID := atomic.AddUint64(&e.metadata.NextTxnID, 1) - 1
	return e.PutBatchWithTxnID(kvs, txnID)
}

// PutBatchWithTxn inserts or updates multiple key-value pairs with transaction ID
func (e *Engine) PutBatchWithTxnID(kvs []common.KVPair, txnID uint64) error {
	if e.closed {
		return utils.ErrClosed
	}

	err := e.memTable.PutBatch(kvs, txnID)
	if err != nil {
		return err
	}

	// Check if we need to freeze the current memtable
	if int64(e.memTable.GetCurrentSize()) >= e.config.GetPerMemSizeLimit() {
		e.freezeMemTableIfNeeded()
	}

	atomic.AddUint64(&e.stats.Writes, uint64(len(kvs)))
	atomic.StoreUint64(&e.stats.MemTableSize, uint64(e.memTable.GetCurrentSize()))
	atomic.StoreUint64(&e.stats.FrozenTableSize, uint64(e.memTable.GetFrozenSize()))

	return nil
}

// Get retrieves a value by key
func (e *Engine) Get(key string) (string, bool, error) {
	return e.GetWithTxnID(key, 0)
}

// GetWithTxnID retrieves a value by key with transaction ID for snapshot isolation
func (e *Engine) GetWithTxnID(key string, txnID uint64) (string, bool, error) {
	if e.closed {
		return "", false, utils.ErrClosed
	}

	atomic.AddUint64(&e.stats.Reads, 1)

	// First, check memtable (current + frozen) with transaction ID
	value, found, err := e.memTable.Get(key, txnID)
	if err != nil {
		return "", false, err
	}
	if found {
		return value, true, nil
	}

	// Then, check SST files from level 0 to highest level with transaction ID
	val, found, err := e.levels.Get(key, txnID)
	if err != nil {
		return "", false, err
	}
	if found && val == "" {
		return "", false, nil
	}
	return val, found, nil
}

// GetBatch retrieves multiple values by keys
func (e *Engine) GetBatch(keys []string) ([]memtable.GetResult, error) {
	return e.GetBatchWithTxnID(keys, 0)
}

// GetBatchWithTxn retrieves multiple values by keys with transaction ID
func (e *Engine) GetBatchWithTxnID(keys []string, txnID uint64) ([]memtable.GetResult, error) {
	if e.closed {
		return nil, utils.ErrClosed
	}

	defer atomic.AddUint64(&e.stats.Reads, uint64(len(keys)))

	results := make([]memtable.GetResult, len(keys))

	// First try to get all keys from memtable with transaction ID
	memResults, err := e.memTable.GetBatch(keys, txnID)
	if err != nil {
		return nil, err
	}

	// Copy memtable results and identify missing keys
	missingKeys := make([]string, 0)
	for i, result := range memResults {
		results[i] = result
		if !result.Found {
			missingKeys = append(missingKeys, result.Key)
		}
	}

	// Get missing keys from SST files with transaction ID
	if len(missingKeys) > 0 {
		sstResults, err := e.levels.GetBatch(missingKeys, txnID)
		if err != nil {
			return nil, err
		}

		// Merge SST results back into final results
		sstResultMap := make(map[string]memtable.GetResult)
		for _, result := range sstResults {
			sstResultMap[result.Key] = result
		}

		for i := range results {
			if !results[i].Found {
				if sstResult, found := sstResultMap[results[i].Key]; found {
					results[i] = sstResult
				}
			}
		}
	}

	return results, nil
}

func (e *Engine) Delete(key string) error {
	txnID := atomic.AddUint64(&e.metadata.NextTxnID, 1) - 1
	return e.DeleteWithTxnID(key, txnID)
}

// Delete marks a key as deleted
func (e *Engine) DeleteWithTxnID(key string, txnID uint64) error {
	if e.closed {
		return utils.ErrClosed
	}

	// Check if we need to freeze the current memtable
	if int64(e.memTable.GetCurrentSize()) >= e.config.GetPerMemSizeLimit() {
		e.freezeMemTableIfNeeded()
	}

	err := e.memTable.Delete(key, txnID)
	if err != nil {
		return err
	}

	atomic.AddUint64(&e.stats.Deletes, 1)
	atomic.StoreUint64(&e.stats.MemTableSize, uint64(e.memTable.GetCurrentSize()))
	atomic.StoreUint64(&e.stats.FrozenTableSize, uint64(e.memTable.GetFrozenSize()))

	return nil
}

// NewIterator creates a new iterator for scanning the database
func (e *Engine) NewIterator() iterator.Iterator {
	if e.closed {
		return iterator.NewEmptyIterator()
	}

	// Create merge iterator combining memtable and all SST levels
	iterators := make([]iterator.Iterator, 0)

	// Add memtable iterator
	memIter := e.memTable.NewIterator(0)
	iterators = append(iterators, memIter)

	// Add SST level iterators
	levelIters := e.levels.GetIterators(0)
	iterators = append(iterators, levelIters...)

	return iterator.NewMergeIterator(iterators)
}

// freezeMemTableIfNeeded freezes the current memtable if it's too large
func (e *Engine) freezeMemTableIfNeeded() {
	if int64(e.memTable.GetCurrentSize()) >= e.config.GetPerMemSizeLimit() {
		e.memTable.FreezeCurrentTable()
	}
}

// Flush forces a flush of frozen memtables to disk
func (e *Engine) Flush() error {
	if e.closed {
		return utils.ErrClosed
	}

	if !e.flushAndCompactByHand {
		return e.NoticeFlushCheck()
	}

	return e._doFlush()
}

func (e *Engine) updateOnTxnFlushed(entry *iterator.Entry) error {
	e.txnManager.updateFlushedTxn(entry.TxnID)
	return nil
}

// _doFlush performs the actual flush operation
// need lock outside
func (e *Engine) _doFlush() error {
	flushResult, err := e.memTable.FlushOldest()
	if err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	if flushResult == nil {
		return nil // Nothing was flushed
	}

	// Build SST from flush result
	sstID := atomic.AddUint64(&e.metadata.NextSSTID, 1) - 1
	sstPath := e.fileManager.GetSSTPath(sstID, 0) // Level 0

	builder := sst.NewSSTBuilder(e.config.GetBlockSize(), true) // Enable bloom filter

	// Add entries from flush result
	for _, entry := range flushResult.Entries {
		if entry.Key == "" && entry.Value == "" {
			e.updateOnTxnFlushed(&entry)
			// skip the transaction end marker
			continue
		}
		err := builder.Add(entry.Key, entry.Value, entry.TxnID)
		if err != nil {
			return fmt.Errorf("failed to add entry to SST builder: %w", err)
		}
	}

	// Build the SST file
	newSST, err := builder.Build(sstID, sstPath, e.blockCache)
	if err != nil {
		return fmt.Errorf("failed to build SST: %w", err)
	}

	// Add the new SST to level 0
	err = e.levels.AddSST(0, newSST)
	if err != nil {
		newSST.Close()
		newSST.Delete()
		return fmt.Errorf("failed to add SST to level manager: %w", err)
	}

	// Save metadata after updating nextSSTID
	if err := saveMetadata(e); err != nil {
		fmt.Printf("Warning: failed to save metadata after flush: %v\n", err)
	}

	// Update statistics
	atomic.AddUint64(&e.stats.Flushes, 1)
	atomic.AddUint64(&e.stats.FlushedBytes, uint64(newSST.Size()))
	atomic.StoreUint64(&e.stats.FrozenTableSize, uint64(e.memTable.GetFrozenSize()))

	return nil
}

// flushWorker runs in the background to flush memtables when needed
func (e *Engine) flushWorker() {
	defer e.wg.Done()

	if e.flushAndCompactByHand {
		return
	}

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.checkCh:
		case <-utils.After(200): // 200ms
			// Check if we need to flush
			if e.memTable.CanFlush() {
				if err := e._doFlush(); err != nil {
					// Log error but continue
					fmt.Printf("Background flush error: %v\n", err)
				}
			}
		}
	}
}

func (e *Engine) flushAtClose() error {
	for !e.memTable.Empty() {
		if err := e._doFlush(); err != nil {
			return err
		}
	}
	return nil
}

// compactionWorker runs in the background to compact SSTs when needed
func (e *Engine) compactionWorker() {
	defer e.wg.Done()

	if e.flushAndCompactByHand {
		return
	}

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-utils.After(5000): // 5 seconds
			// Check if automatic compaction is needed
			if e.levels.NeedsCompaction() {
				if err := e.doCompaction(); err != nil {
					// Log error but continue
					fmt.Printf("Background compaction error: %v\n", err)
				}
			}
		}
	}
}

// cleanupLoop runs in a background goroutine to clean old WAL files
func (e *Engine) cleanWalWorker() {
	defer e.wg.Done()
	fmt.Printf("Starting WAL cleanup loop\n")

	ticker := time.NewTicker(time.Duration(e.config.WAL.CleanInterval))
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			activeTxnIDs := e.txnManager.GetactiveTxnIDs()
			e.wal.CleanOldFiles(activeTxnIDs)
		}
	}
}
func (e *Engine) NoticeFlushCheck() error {
	if e.closed {
		return utils.ErrClosed
	}
	select {
	case e.checkCh <- struct{}{}:
	default:
		return nil
	}
	return nil
}

func (e *Engine) ForceCompact() {
	if e.closed {
		return
	}

	if !e.flushAndCompactByHand {
		return
	}
	// Manual compaction trigger
	if err := e.doCompaction(); err != nil {
		// Log error but continue
		fmt.Printf("Manual compaction error: %v\n", err)
	}
}

// doCompaction performs compaction
func (e *Engine) doCompaction() error {
	task := e.levels.PickCompactionTask()
	if task == nil {
		return nil // No compaction needed
	}

	return e.levels.ExecuteCompaction(task, e.metadata.NextSSTID, &e.metadata.NextSSTID)
}

func (e *Engine) compactAtClose() error {
	for {
		task := e.levels.PickCompactionTask()
		if task == nil {
			return nil // No compaction needed
		}
		if err := e.levels.ExecuteCompaction(task, e.metadata.NextSSTID, &e.metadata.NextSSTID); err != nil {
			return err
		}
	}
}

// ForceCompaction forces a compaction of the specified level
func (e *Engine) ForceCompaction(level int) error {
	if e.closed {
		return utils.ErrClosed
	}

	task := e.levels.CreateCompactionTask(level)
	if task == nil {
		return nil // Nothing to compact
	}

	return e.levels.ExecuteCompaction(task, e.metadata.NextSSTID, &e.metadata.NextSSTID)
}

// cleanupWorker runs periodically to clean up old committed transactions
func (e *Engine) syncTxnStatusWorker() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.txnManager.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.txnManager.syncTxnStatus()
		}
	}
}

// GetStatistics returns engine statistics
func (e *Engine) GetStatistics() EngineStatistics {
	return EngineStatistics{
		Reads:           atomic.LoadUint64(&e.stats.Reads),
		Writes:          atomic.LoadUint64(&e.stats.Writes),
		Deletes:         atomic.LoadUint64(&e.stats.Deletes),
		Flushes:         atomic.LoadUint64(&e.stats.Flushes),
		FlushedBytes:    atomic.LoadUint64(&e.stats.FlushedBytes),
		Compactions:     atomic.LoadUint64(&e.stats.Compactions),
		CompactedBytes:  atomic.LoadUint64(&e.stats.CompactedBytes),
		CompactedFiles:  atomic.LoadUint64(&e.stats.CompactedFiles),
		MemTableSize:    atomic.LoadUint64(&e.stats.MemTableSize),
		FrozenTableSize: atomic.LoadUint64(&e.stats.FrozenTableSize),
		SSTFiles:        e.levels.GetTotalSSTCount(),
		TotalSSTSize:    e.levels.GetTotalSSTSize(),
	}
}

func (e *Engine) GetMeta() *EngineMetadata {
	return e.metadata
}

// GetLevelInfo returns information about all levels
func (e *Engine) GetLevelInfo() []LevelInfo {

	return e.levels.GetLevelInfo()
}

// GetWAL returns the WAL instance
func (e *Engine) GetWAL() *wal.WAL {
	return e.wal
}

// DeleteWithTxn marks a key as deleted with transaction ID
func (e *Engine) DeleteWithTxn(key string, txnID uint64) error {
	if e.closed {
		return utils.ErrClosed
	}

	// Check if we need to freeze the current memtable
	if int64(e.memTable.GetCurrentSize()) >= e.config.GetPerMemSizeLimit() {
		e.freezeMemTableIfNeeded()
	}

	err := e.memTable.Delete(key, txnID)
	if err != nil {
		return err
	}

	atomic.AddUint64(&e.stats.Deletes, 1)
	atomic.StoreUint64(&e.stats.MemTableSize, uint64(e.memTable.GetCurrentSize()))
	atomic.StoreUint64(&e.stats.FrozenTableSize, uint64(e.memTable.GetFrozenSize()))

	return nil
}

// NewIteratorWithTxnID creates a new iterator with transaction ID for snapshot isolation
func (e *Engine) NewIteratorWithTxnID(txnID uint64) iterator.Iterator {
	if e.closed {
		return iterator.NewEmptyIterator()
	}

	// Create merge iterator combining memtable and all SST levels with transaction ID
	iterators := make([]iterator.Iterator, 0)

	// Add memtable iterator with transaction ID
	memIter := e.memTable.NewIterator(txnID)
	iterators = append(iterators, memIter)

	// Add SST level iterators with transaction ID
	levelIters := e.levels.GetIterators(txnID)
	iterators = append(iterators, levelIters...)

	return iterator.NewMergeIterator(iterators)
}

// Close closes the engine and releases resources
func (e *Engine) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true

	e.cancel()
	e.wg.Wait()

	e.flushAtClose()
	e.compactAtClose()

	// Save metadata before shutdown
	if err := saveMetadata(e); err != nil {
		fmt.Printf("Warning: failed to save metadata during shutdown: %v\n", err)
	}

	e.txnManager.Close()
	e.wal.Close()

	return nil
}
