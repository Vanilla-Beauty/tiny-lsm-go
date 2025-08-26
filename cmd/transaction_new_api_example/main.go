package main

import (
	"fmt"
	"log"
	"os"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/lsm"
)

func main() {
	fmt.Println("🔄 Tiny-LSM-Go New Transaction API Example")
	fmt.Println("==========================================")

	// Create a temporary directory for this example
	exampleDir := "new_txn_api_example_data"
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

	// Create transaction engine
	txnConfig := lsm.DefaultTransactionConfig()
	txnConfig.DefaultIsolationLevel = lsm.ReadCommitted
	txnEngine := lsm.NewTxnEngine(engine, txnConfig)
	defer txnEngine.Close()

	fmt.Printf("✅ Transaction engine created in directory: %s\n", exampleDir)

	// Test basic transaction operations with new API
	testBasicOperations(txnEngine)

	// Test different isolation levels
	testIsolationLevels(txnEngine)

	// Test batch operations
	testBatchOperations(txnEngine)

	fmt.Println("\n✅ All new API examples completed successfully!")
}

func testBasicOperations(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n🔄 Basic Operations with New API")
	fmt.Println("--------------------------------")

	// Begin transaction
	txn, err := txnEngine.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	fmt.Printf("Started transaction ID: %d\n", txn.ID())
	fmt.Printf("Transaction state: %v\n", txn.State())
	fmt.Printf("Isolation level: %v\n", txn.IsolationLevel())

	// Write data using new transaction-aware API
	err = txnEngine.Put(txn, "key1", "value1")
	if err != nil {
		log.Printf("Failed to put key1: %v", err)
		return
	}
	fmt.Println("Put: key1 -> value1")

	err = txnEngine.Put(txn, "key2", "value2")
	if err != nil {
		log.Printf("Failed to put key2: %v", err)
		return
	}
	fmt.Println("Put: key2 -> value2")

	// Read data using new transaction-aware API
	value1, found1, err := txnEngine.Get(txn, "key1")
	if err != nil {
		log.Printf("Failed to get key1: %v", err)
		return
	}
	if found1 {
		fmt.Printf("Get: key1 -> %s\n", value1)
	}

	value2, found2, err := txnEngine.Get(txn, "key2")
	if err != nil {
		log.Printf("Failed to get key2: %v", err)
		return
	}
	if found2 {
		fmt.Printf("Get: key2 -> %s\n", value2)
	}

	// Test delete
	err = txnEngine.Delete(txn, "key2")
	if err != nil {
		log.Printf("Failed to delete key2: %v", err)
		return
	}
	fmt.Println("Deleted key2")

	// Try to read deleted key
	_, found2After, err := txnEngine.Get(txn, "key2")
	if err != nil {
		log.Printf("Failed to get key2 after delete: %v", err)
		return
	}
	if found2After {
		fmt.Println("key2 still found after delete (unexpected)")
	} else {
		fmt.Println("key2 correctly not found after delete")
	}

	// Commit transaction
	err = txn.Commit()
	if err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}
	fmt.Printf("✅ Transaction %d committed successfully\n", txn.ID())

	// Verify data is visible after commit using engine directly
	committedValue, found, err := txnEngine.GetEngine().Get("key1")
	if err != nil {
		log.Printf("Failed to get committed data: %v", err)
		return
	}
	if found {
		fmt.Printf("Committed data visible: key1 -> %s\n", committedValue)
	}
}

func testIsolationLevels(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n🔒 Testing Isolation Levels")
	fmt.Println("---------------------------")

	// Test READ_UNCOMMITTED
	fmt.Println("\nTesting READ_UNCOMMITTED:")
	txnUncommitted, err := txnEngine.BeginWithIsolation(lsm.ReadUncommitted)
	if err != nil {
		log.Printf("Failed to begin READ_UNCOMMITTED transaction: %v", err)
		return
	}

	err = txnEngine.Put(txnUncommitted, "uncommitted_key", "uncommitted_value")
	if err != nil {
		log.Printf("Failed to put in READ_UNCOMMITTED: %v", err)
		return
	}
	fmt.Println("✅ READ_UNCOMMITTED transaction wrote directly to database")

	// Rollback to test rollback functionality
	err = txnUncommitted.Rollback()
	if err != nil {
		log.Printf("Failed to rollback READ uncommitted: %v", err)
		return
	}
	fmt.Println("✅ READ_UNCOMMITTED transaction rolled back successfully")

	// Test REPEATABLE_READ with conflict detection
	fmt.Println("\nTesting REPEATABLE_READ:")
	txnRepeatable, err := txnEngine.BeginWithIsolation(lsm.RepeatableRead)
	if err != nil {
		log.Printf("Failed to begin REPEATABLE_READ transaction: %v", err)
		return
	}

	err = txnEngine.Put(txnRepeatable, "repeatable_key", "repeatable_value")
	if err != nil {
		log.Printf("Failed to put in REPEATABLE_READ: %v", err)
		return
	}
	fmt.Println("✅ REPEATABLE_READ transaction stored data in temp map")

	err = txnRepeatable.Commit()
	if err != nil {
		log.Printf("Failed to commit repeatable read: %v", err)
		return
	}
	fmt.Println("✅ REPEATABLE_READ transaction committed successfully")
}

func testBatchOperations(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n📦 Testing Batch Operations")
	fmt.Println("---------------------------")

	txn, err := txnEngine.Begin()
	if err != nil {
		log.Printf("Failed to begin batch transaction: %v", err)
		return
	}

	// Batch put operations
	kvs := []lsm.KVPair{
		{Key: "batch_key1", Value: "batch_value1"},
		{Key: "batch_key2", Value: "batch_value2"},
		{Key: "batch_key3", Value: "batch_value3"},
	}

	err = txnEngine.PutBatch(txn, kvs)
	if err != nil {
		log.Printf("Failed to put batch: %v", err)
		return
	}
	fmt.Println("✅ Batch put operations completed")

	// Batch get operations
	keys := []string{"batch_key1", "batch_key2", "batch_key3", "nonexistent_key"}
	results, err := txnEngine.GetBatch(txn, keys)
	if err != nil {
		log.Printf("Failed to get batch: %v", err)
		return
	}

	fmt.Println("Batch get results:")
	for key, value := range results {
		fmt.Printf("  %s -> %s\n", key, value)
	}

	// Delete some keys
	deleteKeys := []string{"batch_key2", "batch_key3"}
	err = txnEngine.DeleteBatch(txn, deleteKeys)
	if err != nil {
		log.Printf("Failed to delete batch: %v", err)
		return
	}
	fmt.Println("✅ Batch delete operations completed")

	// Verify remaining keys
	remainingResults, err := txnEngine.GetBatch(txn, keys)
	if err != nil {
		log.Printf("Failed to get batch after delete: %v", err)
		return
	}

	fmt.Println("Batch get results after delete:")
	for key, value := range remainingResults {
		fmt.Printf("  %s -> %s\n", key, value)
	}

	err = txn.Commit()
	if err != nil {
		log.Printf("Failed to commit batch transaction: %v", err)
		return
	}
	fmt.Println("✅ Batch transaction committed successfully")
}
