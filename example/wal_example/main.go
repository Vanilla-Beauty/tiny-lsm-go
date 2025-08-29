package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"tiny-lsm-go/pkg/wal"
)

func main() {
	fmt.Println("🔄 WAL (Write-Ahead Logging) Example")
	fmt.Println("=====================================")

	// Create a temporary directory for this example
	exampleDir := "wal_example_data"
	if err := os.MkdirAll(exampleDir, 0755); err != nil {
		log.Fatalf("Failed to create example directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(exampleDir); err != nil {
			log.Printf("Warning: Failed to clean up example directory: %v", err)
		}
	}()

	fmt.Printf("✅ WAL example running in directory: %s\n", exampleDir)

	// Demonstrate basic WAL operations
	demonstrateWALBasics(exampleDir)

	// Demonstrate WAL recovery
	demonstrateWALRecovery(exampleDir)

	fmt.Println("\n✅ All WAL examples completed successfully!")
}

func demonstrateWALBasics(dataDir string) {
	fmt.Println("\n🔄 Basic WAL Operations")
	fmt.Println("-----------------------")

	// Create WAL configuration
	config := &wal.Config{
		LogDir:        dataDir + "/wal",
		BufferSize:    3, // Small buffer for demonstration
		FileSizeLimit: 512, // Small file limit to trigger rotation
		CleanInterval: 5 * time.Second,
	}

	// Create WAL instance
	walInstance, err := wal.New(config, 0)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}

	fmt.Println("WAL instance created successfully")

	// Simulate transaction 1
	fmt.Println("\nSimulating Transaction 1:")
	txn1Records := []*wal.Record{
		wal.NewCreateRecord(1),
		wal.NewPutRecord(1, "user:1000", "john_doe"),
		wal.NewPutRecord(1, "balance:1000", "100.50"),
		wal.NewCommitRecord(1),
	}

	for _, record := range txn1Records {
		fmt.Printf("  %s\n", record.String())
	}

	// Write transaction 1 to WAL
	if err := walInstance.Log(txn1Records, true); err != nil {
		log.Fatalf("Failed to log transaction 1: %v", err)
	}

	// Simulate transaction 2 
	fmt.Println("\nSimulating Transaction 2:")
	txn2Records := []*wal.Record{
		wal.NewCreateRecord(2),
		wal.NewPutRecord(2, "user:1001", "jane_smith"),
		wal.NewDeleteRecord(2, "temp_key"),
		wal.NewCommitRecord(2),
	}

	for _, record := range txn2Records {
		fmt.Printf("  %s\n", record.String())
	}

	// Write transaction 2 to WAL
	if err := walInstance.Log(txn2Records, true); err != nil {
		log.Fatalf("Failed to log transaction 2: %v", err)
	}

	// Simulate a failed transaction (rollback)
	fmt.Println("\nSimulating Failed Transaction 3:")
	txn3Records := []*wal.Record{
		wal.NewCreateRecord(3),
		wal.NewPutRecord(3, "user:1002", "bob_wilson"),
		wal.NewRollbackRecord(3), // This transaction is rolled back
	}

	for _, record := range txn3Records {
		fmt.Printf("  %s\n", record.String())
	}

	// Write failed transaction to WAL
	if err := walInstance.Log(txn3Records, true); err != nil {
		log.Fatalf("Failed to log transaction 3: %v", err)
	}

	// Close WAL
	if err := walInstance.Close(); err != nil {
		log.Fatalf("Failed to close WAL: %v", err)
	}

	fmt.Println("\n✅ WAL operations completed and persisted to disk")
}

func demonstrateWALRecovery(dataDir string) {
	fmt.Println("\n🔄 WAL Recovery Demonstration")
	fmt.Println("-----------------------------")

	walDir := dataDir + "/wal"

	// Simulate recovery process (what happens on system restart)
	fmt.Println("Simulating system restart and WAL recovery...")

	// Recover transactions from WAL files
	recoveredTxns, err := wal.Recover(walDir, 0)
	if err != nil {
		log.Fatalf("Failed to recover from WAL: %v", err)
	}

	fmt.Printf("Recovery completed: found %d transactions in WAL\n", len(recoveredTxns))

	// Process each recovered transaction
	for txnID, records := range recoveredTxns {
		fmt.Printf("\nTransaction %d:\n", txnID)
		
		var committed, aborted bool
		var operations []*wal.Record

		// Analyze transaction records
		for _, record := range records {
			fmt.Printf("  %s\n", record.String())
			
			switch record.OpType {
			case wal.OpCommit:
				committed = true
			case wal.OpRollback:
				aborted = true
			case wal.OpPut, wal.OpDelete:
				operations = append(operations, record)
			}
		}

		// Determine transaction fate
		if committed && !aborted {
			fmt.Printf("  → Status: COMMITTED (would replay %d operations)\n", len(operations))
			
			// In a real system, you would apply these operations:
			for _, op := range operations {
				switch op.OpType {
				case wal.OpPut:
					fmt.Printf("    Would execute: PUT %s = %s\n", op.Key, op.Value)
				case wal.OpDelete:
					fmt.Printf("    Would execute: DELETE %s\n", op.Key)
				}
			}
		} else if aborted {
			fmt.Printf("  → Status: ABORTED (would skip replay)\n")
		} else {
			fmt.Printf("  → Status: INCOMPLETE (would skip replay)\n")
		}
	}

	fmt.Println("\n✅ WAL recovery simulation completed")
	
	// Show what a real recovery would do:
	fmt.Println("\nIn a real system, recovery would:")
	fmt.Println("1. Read all WAL files in sequence order")  
	fmt.Println("2. Group records by transaction ID")
	fmt.Println("3. Only replay transactions that have COMMIT records")
	fmt.Println("4. Skip transactions that were rolled back or incomplete")
	fmt.Println("5. Update the checkpoint transaction ID")
	fmt.Println("6. Clean up old WAL files that are no longer needed")
}
