package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/lsm"
	"tiny-lsm-go/pkg/memtable"
)

func main() {
	fmt.Println("🚀 Tiny-LSM-Go Engine Example")
	fmt.Println("=============================")

	// Create a temporary directory for this example
	exampleDir := "lsm_example_data"
	if err := os.MkdirAll(exampleDir, 0755); err != nil {
		log.Fatalf("Failed to create example directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(exampleDir); err != nil {
			log.Printf("Warning: Failed to clean up example directory: %v", err)
		}
	}()

	// Load configuration
	cfg := config.DefaultConfig()
	cfg.LSM.Core.PerMemSizeLimit = 1024 * 1024 // 1MB for demo
	cfg.LSM.Core.BlockSize = 4096              // 4KB blocks for demo

	// Create LSM engine
	engine, err := lsm.NewEngine(cfg, exampleDir)
	if err != nil {
		log.Fatalf("Failed to create LSM engine: %v", err)
	}
	defer engine.Close()

	fmt.Printf("✅ LSM Engine created in directory: %s\n", exampleDir)

	// Demonstrate basic operations
	demonstrateBasicOperations(engine)

	// Demonstrate batch operations
	demonstrateBatchOperations(engine)

	// Demonstrate flush and compaction
	demonstrateFlushAndCompaction(engine)

	// Demonstrate iteration
	demonstrateIteration(engine)

	// Show final statistics
	showStatistics(engine)

	fmt.Println("\n✅ All LSM engine examples completed successfully!")
}

func demonstrateBasicOperations(engine *lsm.Engine) {
	fmt.Println("\n📝 Basic Operations Demo")
	fmt.Println("------------------------")

	// Put operations
	testData := map[string]string{
		"apple":      "red",
		"banana":     "yellow",
		"cherry":     "red",
		"date":       "brown",
		"elderberry": "purple",
	}

	fmt.Println("Putting key-value pairs:")
	for key, value := range testData {
		if err := engine.Put(key, value); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
			continue
		}
		fmt.Printf("  %s -> %s\n", key, value)
	}

	// Get operations
	fmt.Println("\nRetrieving values:")
	for key := range testData {
		value, found, err := engine.Get(key)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
			continue
		}
		if found {
			fmt.Printf("  %s -> %s\n", key, value)
		} else {
			fmt.Printf("  %s -> not found\n", key)
		}
	}

	// Update operation
	fmt.Println("\nUpdating 'banana' to 'green':")
	if err := engine.Put("banana", "green"); err != nil {
		log.Printf("Failed to update banana: %v", err)
	} else {
		value, found, _ := engine.Get("banana")
		if found {
			fmt.Printf("  banana -> %s\n", value)
		}
	}

	// Delete operation
	fmt.Println("\nDeleting 'cherry':")
	if err := engine.Delete("cherry"); err != nil {
		log.Printf("Failed to delete cherry: %v", err)
	} else {
		_, found, _ := engine.Get("cherry")
		fmt.Printf("  cherry found: %v\n", found)
	}
}

func demonstrateBatchOperations(engine *lsm.Engine) {
	fmt.Println("\n📦 Batch Operations Demo")
	fmt.Println("------------------------")

	// Batch put
	batchData := []memtable.KeyValue{
		{"grape", "purple"},
		{"kiwi", "green"},
		{"mango", "orange"},
		{"orange", "orange"},
		{"pear", "yellow"},
	}

	fmt.Println("Batch putting key-value pairs:")
	if err := engine.PutBatch(batchData); err != nil {
		log.Printf("Batch put failed: %v", err)
	} else {
		for _, kv := range batchData {
			fmt.Printf("  %s -> %s\n", kv.Key, kv.Value)
		}
	}

	// Batch get
	keys := []string{"grape", "kiwi", "mango", "nonexistent"}
	fmt.Println("\nBatch getting values:")
	results, err := engine.GetBatch(keys)
	if err != nil {
		log.Printf("Batch get failed: %v", err)
	} else {
		for _, result := range results {
			if result.Found {
				fmt.Printf("  %s -> %s\n", result.Key, result.Value)
			} else {
				fmt.Printf("  %s -> not found\n", result.Key)
			}
		}
	}
}

func demonstrateFlushAndCompaction(engine *lsm.Engine) {
	fmt.Println("\n💾 Flush and Compaction Demo")
	fmt.Println("-----------------------------")

	// Add more data to trigger flush
	fmt.Println("Adding more data to trigger flush:")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		if err := engine.Put(key, value); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
			break
		}
		if i < 5 {
			fmt.Printf("  %s -> %s\n", key, value)
		} else if i == 5 {
			fmt.Printf("  ... (adding more keys)")
		}
	}

	// Force a flush
	fmt.Println("\nForcing a flush:")
	if err := engine.Flush(); err != nil {
		log.Printf("Flush failed: %v", err)
	} else {
		fmt.Println("  ✅ Flush completed")
	}

	// Show level information
	levels := engine.GetLevelInfo()
	fmt.Println("\nLevel information:")
	for _, level := range levels {
		if level.NumFiles > 0 {
			fmt.Printf("  Level %d: %d files, %.2f KB total\n", 
				level.Level, level.NumFiles, float64(level.TotalSize)/1024.0)
		}
	}

	// Sleep a bit to let background compaction work
	fmt.Println("\nWaiting for background compaction...")
	time.Sleep(2 * time.Second)
}

func demonstrateIteration(engine *lsm.Engine) {
	fmt.Println("\n🔄 Iterator Demo")
	fmt.Println("----------------")

	fmt.Println("Iterating through all data:")
	iter := engine.NewIterator()
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < 10; iter.Next() {
		fmt.Printf("  %s -> %s\n", iter.Key(), iter.Value())
		count++
	}
	
	if iter.Valid() {
		fmt.Printf("  ... (showing first 10 entries)\n")
	}
	
	// Count total entries
	totalCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		totalCount++
	}
	fmt.Printf("Total entries: %d\n", totalCount)
}

func showStatistics(engine *lsm.Engine) {
	fmt.Println("\n📊 Engine Statistics")
	fmt.Println("-------------------")

	stats := engine.GetStats()
	fmt.Printf("Reads: %d\n", stats.Reads)
	fmt.Printf("Writes: %d\n", stats.Writes)
	fmt.Printf("Deletes: %d\n", stats.Deletes)
	fmt.Printf("Flushes: %d\n", stats.Flushes)
	fmt.Printf("Flushed bytes: %.2f KB\n", float64(stats.FlushedBytes)/1024.0)
	fmt.Printf("Compactions: %d\n", stats.Compactions)
	fmt.Printf("MemTable size: %.2f KB\n", float64(stats.MemTableSize)/1024.0)
	fmt.Printf("Frozen table size: %.2f KB\n", float64(stats.FrozenTableSize)/1024.0)
	fmt.Printf("SST files: %d\n", stats.SSTFiles)
	fmt.Printf("Total SST size: %.2f KB\n", float64(stats.TotalSSTSize)/1024.0)

	// Level breakdown
	levels := engine.GetLevelInfo()
	fmt.Println("\nLevel breakdown:")
	for _, level := range levels {
		if level.NumFiles > 0 {
			fmt.Printf("  Level %d: %d files, %.2f KB (%.1f%% full)\n", 
				level.Level, level.NumFiles, 
				float64(level.TotalSize)/1024.0,
				float64(level.TotalSize)/float64(level.MaxSize)*100.0)
		}
	}
}
