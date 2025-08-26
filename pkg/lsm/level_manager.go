package lsm

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"tiny-lsm-go/pkg/cache"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/iterator"
	"tiny-lsm-go/pkg/memtable"
	"tiny-lsm-go/pkg/sst"
	"tiny-lsm-go/pkg/utils"
)

// LevelManager manages multiple levels of SST files
type LevelManager struct {
	config      *config.Config
	fileManager *utils.FileManager
	blockCache  *cache.BlockCache

	// Level data
	levels []Level
	mu     sync.RWMutex
}

// Level represents a single level in the LSM tree
type Level struct {
	Level   int
	SSTs    []*sst.SST
	MaxSize int64 // Maximum size for this level
}

// LevelInfo provides information about a level
type LevelInfo struct {
	Level     int
	NumFiles  int
	TotalSize int64
	MaxSize   int64
	FirstKey  string
	LastKey   string
}

// CompactionTask represents a compaction task
type CompactionTask struct {
	Level       int
	InputSSTs   []*sst.SST
	OutputLevel int
	TotalSize   int64
}

// NewLevelManager creates a new level manager
func NewLevelManager(cfg *config.Config, fileManager *utils.FileManager, blockCache *cache.BlockCache) *LevelManager {
	maxLevels := 7 // Default to 7 levels (0-6)
	levels := make([]Level, maxLevels)

	// Initialize levels with size limits
	baseSize := int64(64 * 1024 * 1024) // 64MB for level 0
	ratio := int64(cfg.GetSSTLevelRatio())

	for i := 0; i < maxLevels; i++ {
		levels[i] = Level{
			Level:   i,
			SSTs:    make([]*sst.SST, 0),
			MaxSize: baseSize,
		}
		if i > 0 {
			baseSize *= ratio
		}
	}

	return &LevelManager{
		config:      cfg,
		fileManager: fileManager,
		blockCache:  blockCache,
		levels:      levels,
	}
}

// LoadExistingSSTs loads existing SST files from disk
func (lm *LevelManager) LoadExistingSSTs() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	dataDir := lm.fileManager.GetDataDir()

	// Scan for SST files
	files, err := filepath.Glob(filepath.Join(dataDir, "*.sst"))
	if err != nil {
		return fmt.Errorf("failed to scan SST files: %w", err)
	}

	for _, filePath := range files {
		// Parse file name to get SST ID and level
		filename := filepath.Base(filePath)
		parts := strings.Split(filename, ".")
		if len(parts) != 2 || parts[1] != "sst" {
			continue // Skip invalid files
		}

		// Extract SST ID and level from filename
		// Expected format: sstid_level.sst
		nameParts := strings.Split(parts[0], "_")
		if len(nameParts) != 2 {
			continue
		}

		var sstID uint64
		var level int
		if _, err := fmt.Sscanf(nameParts[0], "%d", &sstID); err != nil {
			continue
		}
		if _, err := fmt.Sscanf(nameParts[1], "%d", &level); err != nil {
			continue
		}

		if level < 0 || level >= len(lm.levels) {
			continue // Invalid level
		}

		// Open the SST file
		sstFile, err := sst.Open(sstID, filePath, lm.blockCache)
		if err != nil {
			fmt.Printf("Warning: failed to open SST file %s: %v\n", filePath, err)
			continue
		}

		// Add to the appropriate level
		lm.levels[level].SSTs = append(lm.levels[level].SSTs, sstFile)
	}

	// Sort SSTs in each level by first key
	for i := range lm.levels {
		lm.sortLevel(i)
	}

	return nil
}

// sortLevel sorts SSTs in a level by their first key
func (lm *LevelManager) sortLevel(level int) {
	if level < 0 || level >= len(lm.levels) {
		return
	}

	sort.Slice(lm.levels[level].SSTs, func(i, j int) bool {
		return lm.levels[level].SSTs[i].FirstKey() < lm.levels[level].SSTs[j].FirstKey()
	})
}

// AddSST adds a new SST to the specified level
func (lm *LevelManager) AddSST(level int, sstFile *sst.SST) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if level < 0 || level >= len(lm.levels) {
		return fmt.Errorf("invalid level: %d", level)
	}

	lm.levels[level].SSTs = append(lm.levels[level].SSTs, sstFile)
	lm.sortLevel(level)

	return nil
}

// Get searches for a key across all levels
func (lm *LevelManager) Get(key string, txnID uint64) (string, bool, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Search levels from 0 to highest
	for i := 0; i < len(lm.levels); i++ {
		for _, sstFile := range lm.levels[i].SSTs {
			// Check if key might be in this SST
			if key < sstFile.FirstKey() || key > sstFile.LastKey() {
				continue
			}

			// Search in this SST
			iter, err := sstFile.Get(key, txnID)
			if err != nil {
				return "", false, err
			}

			if iter.Valid() && iter.Key() == key {
				value := iter.Value()
				iter.Close()
				return value, true, nil
			}
			iter.Close()

			// For levels > 0, SSTs don't overlap, so if we checked the right SST,
			// the key is not in this level
			if i > 0 {
				break
			}
		}
	}

	return "", false, nil
}

// GetBatch retrieves multiple keys across all levels
func (lm *LevelManager) GetBatch(keys []string, txnID uint64) ([]memtable.GetResult, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	results := make([]memtable.GetResult, len(keys))
	for i, key := range keys {
		results[i] = memtable.GetResult{Key: key, Found: false}
	}

	remainingKeys := make(map[string]int) // key -> index in results
	for i, key := range keys {
		remainingKeys[key] = i
	}

	// Search levels from 0 to highest
	for levelIdx := 0; levelIdx < len(lm.levels) && len(remainingKeys) > 0; levelIdx++ {
		for _, sstFile := range lm.levels[levelIdx].SSTs {
			if len(remainingKeys) == 0 {
				break
			}

			// Check which remaining keys might be in this SST
			candidateKeys := make([]string, 0)
			for key := range remainingKeys {
				if key >= sstFile.FirstKey() && key <= sstFile.LastKey() {
					candidateKeys = append(candidateKeys, key)
				}
			}

			if len(candidateKeys) == 0 {
				continue
			}

			// Search for candidate keys in this SST
			for _, key := range candidateKeys {
				iter, err := sstFile.Get(key, txnID)
				if err != nil {
					return nil, err
				}

				if iter.Valid() && iter.Key() == key && !iter.IsDeleted() {
					idx := remainingKeys[key]
					results[idx] = memtable.GetResult{
						Key:   key,
						Value: iter.Value(),
						Found: true,
					}
					delete(remainingKeys, key)
				}
				iter.Close()
			}

			// For levels > 0, SSTs don't overlap
			if levelIdx > 0 {
				// Remove keys that would be in this SST's range but weren't found
				for key := range remainingKeys {
					if key >= sstFile.FirstKey() && key <= sstFile.LastKey() {
						// Key is not in this level
						break
					}
				}
			}
		}
	}

	return results, nil
}

// GetIterators returns iterators for all levels
func (lm *LevelManager) GetIterators(txnID uint64) []iterator.Iterator {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	iterators := make([]iterator.Iterator, 0)

	for i := 0; i < len(lm.levels); i++ {
		if len(lm.levels[i].SSTs) == 0 {
			continue
		}

		if i == 0 {
			// Level 0: SSTs may overlap, so we need separate iterators for each
			for _, sstFile := range lm.levels[i].SSTs {
				iter := sstFile.NewIterator(txnID)
				iterators = append(iterators, iter)
			}
		} else {
			// Level > 0: SSTs don't overlap, so we can use a single merge iterator
			levelIters := make([]iterator.Iterator, len(lm.levels[i].SSTs))
			for j, sstFile := range lm.levels[i].SSTs {
				levelIters[j] = sstFile.NewIterator(txnID)
			}
			mergeIter := iterator.NewMergeIterator(levelIters)
			iterators = append(iterators, mergeIter)
		}
	}

	return iterators
}

// GetLevelInfo returns information about all levels
func (lm *LevelManager) GetLevelInfo() []LevelInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	info := make([]LevelInfo, len(lm.levels))

	for i, level := range lm.levels {
		info[i] = LevelInfo{
			Level:     i,
			NumFiles:  len(level.SSTs),
			TotalSize: lm.getLevelSize(i),
			MaxSize:   level.MaxSize,
		}

		if len(level.SSTs) > 0 {
			info[i].FirstKey = level.SSTs[0].FirstKey()
			info[i].LastKey = level.SSTs[len(level.SSTs)-1].LastKey()
		}
	}

	return info
}

// getLevelSize returns the total size of a level
func (lm *LevelManager) getLevelSize(level int) int64 {
	if level < 0 || level >= len(lm.levels) {
		return 0
	}

	var totalSize int64
	for _, sstFile := range lm.levels[level].SSTs {
		totalSize += sstFile.Size()
	}
	return totalSize
}

// GetTotalSSTCount returns the total number of SST files
func (lm *LevelManager) GetTotalSSTCount() uint64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var count uint64
	for _, level := range lm.levels {
		count += uint64(len(level.SSTs))
	}
	return count
}

// GetTotalSSTSize returns the total size of all SST files
func (lm *LevelManager) GetTotalSSTSize() uint64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var totalSize uint64
	for i := range lm.levels {
		totalSize += uint64(lm.getLevelSize(i))
	}
	return totalSize
}

// NeedsCompaction checks if any level needs compaction
func (lm *LevelManager) NeedsCompaction() bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	for i := 0; i < len(lm.levels)-1; i++ { // Skip last level
		if lm.getLevelSize(i) > lm.levels[i].MaxSize {
			return true
		}
	}
	return false
}

// PickCompactionTask picks a compaction task
func (lm *LevelManager) PickCompactionTask() *CompactionTask {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Find the level that most needs compaction
	for i := 0; i < len(lm.levels)-1; i++ {
		levelSize := lm.getLevelSize(i)
		if levelSize > lm.levels[i].MaxSize {
			return &CompactionTask{
				Level:       i,
				InputSSTs:   lm.levels[i].SSTs[:], // Copy slice
				OutputLevel: i + 1,
				TotalSize:   levelSize,
			}
		}
	}
	return nil
}

// CreateCompactionTask creates a compaction task for a specific level
func (lm *LevelManager) CreateCompactionTask(level int) *CompactionTask {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if level < 0 || level >= len(lm.levels)-1 || len(lm.levels[level].SSTs) == 0 {
		return nil
	}

	return &CompactionTask{
		Level:       level,
		InputSSTs:   lm.levels[level].SSTs[:], // Copy slice
		OutputLevel: level + 1,
		TotalSize:   lm.getLevelSize(level),
	}
}

// ExecuteCompaction executes a compaction task
func (lm *LevelManager) ExecuteCompaction(task *CompactionTask, nextSSTID uint64, nextSSTIDPtr *uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if task == nil {
		return nil
	}

	// Create iterators for input SSTs
	inputIters := make([]iterator.Iterator, len(task.InputSSTs))
	for i, sstFile := range task.InputSSTs {
		inputIters[i] = sstFile.NewIterator(0) // Read all versions
	}

	// Create merge iterator
	mergeIter := iterator.NewMergeIterator(inputIters)
	defer mergeIter.Close()

	// Build new SSTs for the output level
	var outputSSTs []*sst.SST
	builder := sst.NewSSTBuilder(lm.config.GetBlockSize(), true)

	currentSSTID := nextSSTID
	entriesInCurrentSST := 0
	maxEntriesPerSST := 10000 // Configurable

	for mergeIter.SeekToFirst(); mergeIter.Valid(); mergeIter.Next() {
		// Add entry to current SST
		err := builder.Add(mergeIter.Key(), mergeIter.Value(), mergeIter.TxnID())
		if err != nil {
			// Current SST is full, build it and start a new one
			if entriesInCurrentSST > 0 {
				sstPath := lm.fileManager.GetSSTPath(currentSSTID, task.OutputLevel)
				newSST, buildErr := builder.Build(currentSSTID, sstPath, lm.blockCache)
				if buildErr != nil {
					// Clean up any built SSTs
					for _, sst := range outputSSTs {
						sst.Close()
						sst.Delete()
					}
					return fmt.Errorf("failed to build SST during compaction: %w", buildErr)
				}
				outputSSTs = append(outputSSTs, newSST)

				// Start new SST
				*nextSSTIDPtr++
				currentSSTID = *nextSSTIDPtr
				builder = sst.NewSSTBuilder(lm.config.GetBlockSize(), true)
				entriesInCurrentSST = 0
			}

			// Try adding to new SST
			err = builder.Add(mergeIter.Key(), mergeIter.Value(), mergeIter.TxnID())
			if err != nil {
				// Clean up
				for _, sst := range outputSSTs {
					sst.Close()
					sst.Delete()
				}
				return fmt.Errorf("failed to add entry to new SST during compaction: %w", err)
			}
		}
		entriesInCurrentSST++

		// Check if we should start a new SST
		if entriesInCurrentSST >= maxEntriesPerSST {
			sstPath := lm.fileManager.GetSSTPath(currentSSTID, task.OutputLevel)
			newSST, buildErr := builder.Build(currentSSTID, sstPath, lm.blockCache)
			if buildErr != nil {
				// Clean up
				for _, sst := range outputSSTs {
					sst.Close()
					sst.Delete()
				}
				return fmt.Errorf("failed to build SST during compaction: %w", buildErr)
			}
			outputSSTs = append(outputSSTs, newSST)

			// Start new SST
			*nextSSTIDPtr++
			currentSSTID = *nextSSTIDPtr
			builder = sst.NewSSTBuilder(lm.config.GetBlockSize(), true)
			entriesInCurrentSST = 0
		}
	}

	// Build the final SST if it has entries
	if entriesInCurrentSST > 0 {
		sstPath := lm.fileManager.GetSSTPath(currentSSTID, task.OutputLevel)
		newSST, err := builder.Build(currentSSTID, sstPath, lm.blockCache)
		if err != nil {
			// Clean up
			for _, sst := range outputSSTs {
				sst.Close()
				sst.Delete()
			}
			return fmt.Errorf("failed to build final SST during compaction: %w", err)
		}
		outputSSTs = append(outputSSTs, newSST)
		*nextSSTIDPtr++
	}

	// Replace input SSTs with output SSTs
	// Remove input SSTs from source level
	inputLevelSSTs := make([]*sst.SST, 0)
	for _, existing := range lm.levels[task.Level].SSTs {
		found := false
		for _, input := range task.InputSSTs {
			if existing.ID() == input.ID() {
				found = true
				break
			}
		}
		if !found {
			inputLevelSSTs = append(inputLevelSSTs, existing)
		}
	}
	lm.levels[task.Level].SSTs = inputLevelSSTs

	// Add output SSTs to target level
	lm.levels[task.OutputLevel].SSTs = append(lm.levels[task.OutputLevel].SSTs, outputSSTs...)
	lm.sortLevel(task.OutputLevel)

	// Clean up input SST files
	for _, sstFile := range task.InputSSTs {
		sstFile.Close()
		sstFile.Delete()
	}

	return nil
}

// Close closes all SST files
func (lm *LevelManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for i := range lm.levels {
		for _, sstFile := range lm.levels[i].SSTs {
			if err := sstFile.Close(); err != nil {
				fmt.Printf("Error closing SST file %d: %v\n", sstFile.ID(), err)
			}
		}
		lm.levels[i].SSTs = nil
	}

	return nil
}
