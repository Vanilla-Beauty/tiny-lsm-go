package main

import (
	"fmt"
	"log"
	"os"

	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/lsm"
)

func main() {
	fmt.Println("🔄 Tiny-LSM-Go Transaction Example")
	fmt.Println("===================================")

	// Create a temporary directory for this example
	exampleDir := "txn_example_data"
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

	// Demonstrate basic transaction operations
	demonstrateBasicTransactions(txnEngine)

	// Demonstrate transaction isolation
	demonstrateTransactionIsolation(txnEngine)

	// Demonstrate concurrent transactions
	demonstrateConcurrentTransactions(txnEngine)

	// Show transaction manager statistics
	showTransactionStats(txnEngine.GetManager())

	fmt.Println("\n✅ All transaction examples completed successfully!")
}

func demonstrateBasicTransactions(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n🔄 Basic Transaction Operations")
	fmt.Println("------------------------------")

	// Start a transaction
	txn, err := txnEngine.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	fmt.Printf("Started transaction ID: %d\n", txn.ID())
	fmt.Printf("Transaction state: %v\n", txn.State())
	fmt.Printf("Isolation level: %v\n", txn.IsolationLevel())

	// Write some data using the LSM engine directly (outside transaction)
	fmt.Println("\nWriting data outside transaction:")
	if err := txnEngine.GetEngine().Put("shared_key", "initial_value"); err != nil {
		log.Printf("Failed to put shared_key: %v", err)
		return
	}
	fmt.Println("  shared_key -> initial_value")

	// Write transaction-specific data
	fmt.Println("\nWriting data within transaction:")
	if err := txnEngine.Put(txn, "txn_key1", "txn_value1"); err != nil {
		log.Printf("Failed to put txn_key1: %v", err)
		return
	}
	fmt.Println("  txn_key1 -> txn_value1")

	if err := txnEngine.Put(txn, "txn_key2", "txn_value2"); err != nil {
		log.Printf("Failed to put txn_key2: %v", err)
		return
	}
	fmt.Println("  txn_key2 -> txn_value2")

	// Update shared key within transaction
	if err := txnEngine.Put(txn, "shared_key", "updated_value"); err != nil {
		log.Printf("Failed to update shared_key: %v", err)
		return
	}
	fmt.Println("  shared_key -> updated_value (in transaction)")

	// Read data within transaction
	fmt.Println("\nReading data within transaction:")
	value, found, err := txnEngine.Get(txn, "txn_key1")
	if err != nil {
		log.Printf("Failed to get txn_key1: %v", err)
	} else if found {
		fmt.Printf("  txn_key1 -> %s\n", value)
	}

	value, found, err = txnEngine.Get(txn, "shared_key")
	if err != nil {
		log.Printf("Failed to get shared_key: %v", err)
	} else if found {
		fmt.Printf("  shared_key -> %s (transaction view)\n", value)
	}

	// Commit transaction
	fmt.Println("\nCommitting transaction...")
	if err := txn.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}
	fmt.Printf("✅ Transaction %d committed successfully\n", txn.ID())
	fmt.Printf("Transaction state: %v\n", txn.State())
}

func demonstrateTransactionIsolation(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n🔒 Transaction Isolation Demo")
	fmt.Println("-----------------------------")

	// Start two concurrent transactions
	txn1, err := txnEngine.BeginWithIsolation(lsm.ReadCommitted)
	if err != nil {
		log.Printf("Failed to begin transaction 1: %v", err)
		return
	}

	txn2, err := txnEngine.BeginWithIsolation(lsm.ReadCommitted)
	if err != nil {
		log.Printf("Failed to begin transaction 2: %v", err)
		return
	}

	fmt.Printf("Started transaction 1 ID: %d\n", txn1.ID())
	fmt.Printf("Started transaction 2 ID: %d\n", txn2.ID())

	// Transaction 1 writes data
	fmt.Println("\nTransaction 1 writes data:")
	if err := txnEngine.Put(txn1, "isolation_test", "txn1_value"); err != nil {
		log.Printf("Failed to put from txn1: %v", err)
		return
	}
	fmt.Println("  isolation_test -> txn1_value")

	// Transaction 2 tries to read the same key (should see snapshot)
	fmt.Println("\nTransaction 2 reads data (should see snapshot view):")
	value, found, err := txnEngine.Get(txn2, "isolation_test")
	if err != nil {
		log.Printf("Failed to get from txn2: %v", err)
	} else if found {
		fmt.Printf("  isolation_test -> %s (from txn2 view)\n", value)
	} else {
		fmt.Println("  isolation_test -> not found (from txn2 view)")
	}

	// Transaction 2 writes its own value
	fmt.Println("\nTransaction 2 writes its own data:")
	if err := txnEngine.Put(txn2, "isolation_test", "txn2_value"); err != nil {
		log.Printf("Failed to put from txn2: %v", err)
		return
	}
	fmt.Println("  isolation_test -> txn2_value")

	// Commit transaction 1 first
	fmt.Printf("\nCommitting transaction 1 (ID: %d)...\n", txn1.ID())
	if err := txn1.Commit(); err != nil {
		log.Printf("Failed to commit transaction 1: %v", err)
		return
	}

	// Then commit transaction 2
	fmt.Printf("Committing transaction 2 (ID: %d)...\n", txn2.ID())
	if err := txn2.Commit(); err != nil {
		log.Printf("Failed to commit transaction 2: %v", err)
		return
	}

	// Check final value
	fmt.Println("\nFinal value after both transactions committed:")
	value, found, err = txnEngine.GetEngine().Get("isolation_test")
	if err != nil {
		log.Printf("Failed to get final value: %v", err)
	} else if found {
		fmt.Printf("  isolation_test -> %s\n", value)
	}
}

func demonstrateConcurrentTransactions(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n🚀 Concurrent Transactions Demo")
	fmt.Println("-------------------------------")

	// Create multiple transactions concurrently
	numTxns := 5
	transactions := make([]*lsm.Transaction, numTxns)
	
	fmt.Printf("Creating %d concurrent transactions:\n", numTxns)
	for i := 0; i < numTxns; i++ {
		txn, err := txnEngine.Begin()
		if err != nil {
			log.Printf("Failed to begin transaction %d: %v", i, err)
			continue
		}
		transactions[i] = txn
		fmt.Printf("  Transaction %d (ID: %d)\n", i+1, txn.ID())
	}

	// Each transaction writes its own data
	fmt.Println("\nEach transaction writing its own data:")
	for i, txn := range transactions {
		if txn == nil {
			continue
		}
		key := fmt.Sprintf("concurrent_key_%d", i+1)
		value := fmt.Sprintf("value_from_txn_%d", txn.ID())
		
		if err := txnEngine.Put(txn, key, value); err != nil {
			log.Printf("Failed to put from txn %d: %v", txn.ID(), err)
			continue
		}
		fmt.Printf("  %s -> %s (txn %d)\n", key, value, txn.ID())
	}

	// Commit all transactions
	fmt.Println("\nCommitting all transactions:")
	for _, txn := range transactions {
		if txn == nil {
			continue
		}
		if err := txn.Commit(); err != nil {
			log.Printf("Failed to commit transaction %d: %v", txn.ID(), err)
			continue
		}
		fmt.Printf("  ✅ Transaction %d committed\n", txn.ID())
	}

	// Verify all data is committed
	fmt.Println("\nVerifying committed data:")
	for i := 0; i < numTxns; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i+1)
		value, found, err := txnEngine.GetEngine().Get(key)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
			continue
		}
		if found {
			fmt.Printf("  %s -> %s\n", key, value)
		}
	}
}

func demonstrateRollback(txnEngine *lsm.TxnEngine) {
	fmt.Println("\n↩️ Transaction Rollback Demo")
	fmt.Println("----------------------------")

	// Start a transaction
	txn, err := txnEngine.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	fmt.Printf("Started transaction ID: %d\n", txn.ID())

	// Write some data
	fmt.Println("\nWriting data in transaction:")
	if err := txnEngine.Put(txn, "rollback_test", "will_be_rolled_back"); err != nil {
		log.Printf("Failed to put rollback_test: %v", err)
		return
	}
	fmt.Println("  rollback_test -> will_be_rolled_back")

	// Read the data within transaction
	value, found, err := txnEngine.Get(txn, "rollback_test")
	if err != nil {
		log.Printf("Failed to get rollback_test: %v", err)
	} else if found {
		fmt.Printf("  Confirmed within txn: rollback_test -> %s\n", value)
	}

	// Rollback the transaction
	fmt.Println("\nRolling back transaction...")
	if err := txn.Rollback(); err != nil {
		log.Printf("Failed to rollback transaction: %v", err)
		return
	}
	fmt.Printf("✅ Transaction %d rolled back\n", txn.ID())
	fmt.Printf("Transaction state: %v\n", txn.State())

	// Try to read the data (should not be visible)
	fmt.Println("\nChecking if data is visible after rollback:")
	value, found, err = txnEngine.GetEngine().Get("rollback_test")
	if err != nil {
		log.Printf("Failed to get rollback_test: %v", err)
	} else if found {
		fmt.Printf("  rollback_test -> %s (unexpected!)\n", value)
	} else {
		fmt.Println("  rollback_test -> not found (expected)")
	}
}

func showTransactionStats(manager *lsm.TransactionManager) {
	fmt.Println("\n📊 Transaction Manager Statistics")
	fmt.Println("---------------------------------")
	
	fmt.Printf("Active transactions: %d\n", manager.GetActiveTransactionCount())
	fmt.Printf("Committed transactions: %d\n", manager.GetCommittedTransactionCount())
	fmt.Printf("Next transaction ID: %d\n", manager.GetNextTxnID())
}
