package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/memtable"
	"tiny-lsm-go/pkg/skiplist"
	"tiny-lsm-go/pkg/utils"
)

func main() {
	fmt.Println("🚀 Tiny-LSM-Go Example")
	fmt.Println("=====================")

	// Create a temporary directory for this example
	exampleDir := "example_data"
	if err := os.MkdirAll(exampleDir, 0755); err != nil {
		log.Fatalf("Failed to create example directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(exampleDir); err != nil {
			log.Printf("Warning: Failed to clean up example directory: %v", err)
		}
	}()

	// Demonstrate configuration loading
	demonstrateConfig()

	// Demonstrate skip list functionality
	demonstrateSkipList()

	// Demonstrate bloom filter
	demonstrateBloomFilter()

	// Demonstrate MemTable functionality
	demonstrateMemTable()

	// Demonstrate file utilities
	demonstrateFileUtils(exampleDir)

	fmt.Println("\n✅ All examples completed successfully!")
}

func demonstrateConfig() {
	fmt.Println("\n📋 Configuration Demo")
	fmt.Println("--------------------")

	// Load default configuration
	config := config.DefaultConfig()
	fmt.Printf("Default block size: %d bytes\n", config.GetBlockSize())
	fmt.Printf("Default memory limit: %d MB\n", config.GetTotalMemSizeLimit()/(1024*1024))
	fmt.Printf("Default SST level ratio: %d\n", config.GetSSTLevelRatio())

	// Validate configuration
	if err := config.Validate(); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}
	fmt.Println("✓ Configuration validation passed")
}

func demonstrateSkipList() {
	fmt.Println("\n📚 SkipList Demo")
	fmt.Println("---------------")

	// Create a new skip list
	sl := skiplist.New()

	// Add some key-value pairs
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	values := []string{"red", "yellow", "red", "brown", "purple"}

	fmt.Println("Adding entries to SkipList:")
	for i, key := range keys {
		txnID := uint64(i + 1)
		sl.Put(key, values[i], txnID)
		fmt.Printf("  %s -> %s (txn: %d)\n", key, values[i], txnID)
	}

	fmt.Printf("\nSkipList size: %d entries, %d bytes\n", sl.Size(), sl.SizeBytes())

	// Demonstrate get operations
	fmt.Println("\nRetrieving entries:")
	for _, key := range keys {
		iter := sl.Get(key, 0) // txnID=0 means see all versions
		if iter.Valid() {
			fmt.Printf("  %s -> %s (txn: %d)\n", iter.Key(), iter.Value(), iter.TxnID())
		} else {
			fmt.Printf("  %s -> not found\n", key)
		}
		iter.Close()
	}

	// Demonstrate iterator
	fmt.Println("\nIterating through all entries:")
	iter := sl.NewIterator(0)
	defer iter.Close()
	
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s -> %s (txn: %d)\n", iter.Key(), iter.Value(), iter.TxnID())
	}

	// Demonstrate MVCC with different transaction IDs
	fmt.Println("\nUpdating 'banana' with a new transaction:")
	sl.Put("banana", "green", 10) // Higher transaction ID

	fmt.Println("Reading with different transaction snapshots:")
	for _, txnID := range []uint64{5, 15} {
		iter := sl.Get("banana", txnID)
		if iter.Valid() {
			fmt.Printf("  txn %d sees: banana -> %s (written at txn %d)\n", 
				txnID, iter.Value(), iter.TxnID())
		}
		iter.Close()
	}

	// Demonstrate deletion
	fmt.Println("\nDeleting 'cherry':")
	sl.Delete("cherry", 20)
	
	iter = sl.Get("cherry", 0)
	if iter.Valid() && iter.IsDeleted() {
		fmt.Printf("  cherry is marked as deleted (txn: %d)\n", iter.TxnID())
	}
	iter.Close()
}

func demonstrateBloomFilter() {
	fmt.Println("\n🌸 Bloom Filter Demo")
	fmt.Println("-------------------")

	// Create a bloom filter
	bf := utils.NewBloomFilter(1000, 0.01) // 1000 items, 1% false positive rate
	fmt.Printf("Created Bloom filter: %d bits, %d hash functions\n", 
		bf.Size(), bf.NumHashes())

	// Add some items
	items := []string{"key1", "key2", "key3", "key4", "key5"}
	fmt.Println("\nAdding items to Bloom filter:")
	for _, item := range items {
		bf.Add(item)
		fmt.Printf("  Added: %s\n", item)
	}

	// Test membership
	fmt.Println("\nTesting membership:")
	testItems := append(items, "key6", "key7", "nonexistent")
	for _, item := range testItems {
		exists := bf.Contains(item)
		fmt.Printf("  %s: %v\n", item, exists)
	}

	fmt.Printf("\nBloom filter stats:\n")
	fmt.Printf("  Items added: %d\n", bf.NumItems())
	fmt.Printf("  Estimated false positive rate: %.4f\n", bf.EstimateFalsePositiveRate())

	// Demonstrate serialization
	data := bf.Serialize()
	fmt.Printf("  Serialized size: %d bytes\n", len(data))

	// Deserialize and test
	bf2 := utils.DeserializeBloomFilter(data)
	if bf2 != nil {
		fmt.Println("  ✓ Successfully deserialized bloom filter")
		fmt.Printf("  Contains 'key1': %v\n", bf2.Contains("key1"))
		fmt.Printf("  Contains 'nonexistent': %v\n", bf2.Contains("nonexistent"))
	}
}

func demonstrateMemTable() {
	fmt.Println("\n🗂️  MemTable Demo")
	fmt.Println("-----------------")

	// Create a new MemTable
	mt := memtable.New()

	// Add some data
	fruits := []string{"apple", "banana", "cherry", "date", "elderberry"}
	colors := []string{"red", "yellow", "red", "brown", "purple"}

	fmt.Println("Adding entries to MemTable:")
	for i, fruit := range fruits {
		err := mt.Put(fruit, colors[i], uint64(i+1))
		if err != nil {
			log.Printf("Failed to put %s: %v", fruit, err)
			continue
		}
		fmt.Printf("  %s -> %s (txn: %d)\n", fruit, colors[i], i+1)
	}

	// Show MemTable stats
	stats := mt.GetMemTableStats()
	fmt.Printf("\nMemTable stats: %s\n", stats.String())

	// Demonstrate get operations
	fmt.Println("\nRetrieving entries:")
	for _, fruit := range fruits {
		value, found, err := mt.Get(fruit, 0)
		if err != nil {
			log.Printf("Failed to get %s: %v", fruit, err)
			continue
		}
		if found {
			fmt.Printf("  %s -> %s\n", fruit, value)
		} else {
			fmt.Printf("  %s -> not found\n", fruit)
		}
	}

	// Demonstrate batch operations
	fmt.Println("\nBatch operations:")
	batchKVs := []memtable.KeyValue{
		{"grape", "purple"},
		{"orange", "orange"},
		{"kiwi", "green"},
	}
	err := mt.PutBatch(batchKVs, 10)
	if err != nil {
		log.Printf("Batch put failed: %v", err)
	} else {
		fmt.Println("  ✓ Batch put successful")
	}

	// Batch get
	batchKeys := []string{"grape", "orange", "kiwi", "nonexistent"}
	results, err := mt.GetBatch(batchKeys, 0)
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

	// Demonstrate MVCC
	fmt.Println("\nMVCC demonstration:")
	err = mt.Put("banana", "green", 20) // Update with newer transaction
	if err != nil {
		log.Printf("Failed to update banana: %v", err)
	} else {
		fmt.Println("Updated banana to green (txn: 20)")
	}

	// Read with different transaction snapshots
	for _, txnID := range []uint64{5, 25} {
		value, found, err := mt.Get("banana", txnID)
		if err != nil {
			log.Printf("Failed to get banana at txn %d: %v", txnID, err)
			continue
		}
		if found {
			fmt.Printf("  txn %d sees: banana -> %s\n", txnID, value)
		}
	}

	// Demonstrate deletion
	fmt.Println("\nDeletion demonstration:")
	err = mt.Delete("cherry", 30)
	if err != nil {
		log.Printf("Failed to delete cherry: %v", err)
	} else {
		fmt.Println("Deleted cherry (txn: 30)")
	}

	value, found, err := mt.Get("cherry", 0)
	if err != nil {
		log.Printf("Failed to get cherry: %v", err)
	} else if !found {
		fmt.Println("  cherry is now deleted")
	}

	// But can still see old version
	value, found, err = mt.Get("cherry", 25)
	if err != nil {
		log.Printf("Failed to get cherry at txn 25: %v", err)
	} else if found {
		fmt.Printf("  txn 25 still sees: cherry -> %s\n", value)
	}

	// Demonstrate freezing and flushing
	fmt.Println("\nFreezing and flushing demonstration:")
	fmt.Printf("Current table size: %d bytes\n", mt.GetCurrentSize())
	fmt.Printf("Frozen tables: %d\n", mt.GetFrozenTableCount())

	mt.FreezeCurrentTable()
	fmt.Println("✓ Froze current table")
	fmt.Printf("Current table size: %d bytes\n", mt.GetCurrentSize())
	fmt.Printf("Frozen tables: %d\n", mt.GetFrozenTableCount())
	fmt.Printf("Frozen size: %d bytes\n", mt.GetFrozenSize())

	// Add more data to new current table
	err = mt.Put("mango", "orange", 40)
	if err != nil {
		log.Printf("Failed to put mango: %v", err)
	} else {
		fmt.Println("Added mango to new current table")
	}

	// Show we can still read all data
	fmt.Println("\nReading data from both current and frozen tables:")
	allKeys := []string{"apple", "banana", "mango"}
	for _, key := range allKeys {
		value, found, err := mt.Get(key, 0)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
			continue
		}
		if found {
			fmt.Printf("  %s -> %s\n", key, value)
		}
	}

	// Demonstrate iterator over both tables
	fmt.Println("\nIterating through all data:")
	iter := mt.NewIterator(0)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s -> %s (txn: %d)\n", iter.Key(), iter.Value(), iter.TxnID())
		count++
	}
	fmt.Printf("Total entries seen: %d\n", count)

	// Show flush capability
	if mt.CanFlush() {
		fmt.Printf("\nCan flush %d table(s)\n", mt.GetFlushableTableCount())
		result, err := mt.FlushOldest()
		if err != nil {
			log.Printf("Flush failed: %v", err)
		} else if result != nil {
			fmt.Printf("✓ Flushed table with %d entries\n", len(result.Entries))
			fmt.Printf("  First key: %s, Last key: %s\n", result.FirstKey, result.LastKey)
			fmt.Printf("  Transaction range: %d - %d\n", result.MinTxnID, result.MaxTxnID)
		}
	}
}

func demonstrateFileUtils(dataDir string) {
	fmt.Println("\n📁 File Utilities Demo")
	fmt.Println("----------------------")

	// Create file manager
	fm := utils.NewFileManager(dataDir)
	
	fmt.Printf("Data directory: %s\n", fm.GetDataDir())
	
	// Test file path generation
	sstPath := fm.GetSSTPath(1, 0)
	walPath := fm.GetWALPath("000001.wal")
	manifestPath := fm.GetManifestPath()
	
	fmt.Printf("SST file path: %s\n", sstPath)
	fmt.Printf("WAL file path: %s\n", walPath)
	fmt.Printf("Manifest path: %s\n", manifestPath)

	// Test atomic write
	testFile := filepath.Join(dataDir, "test.txt")
	testData := []byte("Hello, Tiny-LSM-Go!")
	
	fmt.Println("\nTesting atomic write:")
	if err := utils.AtomicWrite(testFile, testData); err != nil {
		log.Printf("Atomic write failed: %v", err)
	} else {
		fmt.Println("  ✓ Atomic write successful")
	}

	// Test file existence
	exists := fm.FileExists("test.txt")
	fmt.Printf("  File exists: %v\n", exists)

	// Test file size
	if size, err := fm.GetFileSize("test.txt"); err == nil {
		fmt.Printf("  File size: %d bytes\n", size)
	}

	// Test checksum operations
	fmt.Println("\nTesting checksum operations:")
	originalData := []byte("This is test data for checksum verification")
	
	// Write with checksum
	testFile2 := filepath.Join(dataDir, "test_checksum.txt")
	file, err := os.Create(testFile2)
	if err != nil {
		log.Printf("Failed to create test file: %v", err)
		return
	}

	writer := utils.NewChecksumWriter(file)
	if err := writer.WriteWithChecksum(originalData); err != nil {
		log.Printf("Failed to write with checksum: %v", err)
		file.Close()
		return
	}
	file.Close()

	// Read with checksum verification
	file, err = os.Open(testFile2)
	if err != nil {
		log.Printf("Failed to open test file: %v", err)
		return
	}
	defer file.Close()

	readData, err := utils.ReadWithChecksum(file, len(originalData))
	if err != nil {
		log.Printf("Failed to read with checksum: %v", err)
		return
	}

	if string(readData) == string(originalData) {
		fmt.Println("  ✓ Checksum verification successful")
	} else {
		fmt.Println("  ✗ Checksum verification failed")
	}

	fmt.Printf("  Original: %s\n", string(originalData))
	fmt.Printf("  Read:     %s\n", string(readData))
}
