package wal

import (
	"os"
	"testing"
	"time"
)

func TestWALBasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	
	config := &Config{
		LogDir:        tempDir,
		BufferSize:    5,
		FileSizeLimit: 1024, // 1KB
		CleanInterval: 5 * time.Second,
	}
	
	// Create WAL instance
	wal, err := New(config, 0)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	
	// Test creating records
	records := []*Record{
		NewCreateRecord(1),
		NewPutRecord(1, "key1", "value1"),
		NewPutRecord(1, "key2", "value2"), 
		NewDeleteRecord(1, "key3"),
		NewCommitRecord(1),
	}
	
	// Log records
	err = wal.Log(records, true)
	if err != nil {
		t.Fatalf("Failed to log records: %v", err)
	}
	
	// Test recovery
	recovered, err := Recover(tempDir, 0)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
	
	// Check recovered records
	if len(recovered) != 1 {
		t.Fatalf("Expected 1 transaction, got %d", len(recovered))
	}
	
	txnRecords := recovered[1]
	if len(txnRecords) != 5 {
		t.Fatalf("Expected 5 records, got %d", len(txnRecords))
	}
	
	// Verify record types
	expectedTypes := []OperationType{OpCreate, OpPut, OpPut, OpDelete, OpCommit}
	for i, record := range txnRecords {
		if record.OpType != expectedTypes[i] {
			t.Errorf("Record %d: expected type %v, got %v", i, expectedTypes[i], record.OpType)
		}
		if record.TxnID != 1 {
			t.Errorf("Record %d: expected TxnID 1, got %d", i, record.TxnID)
		}
	}
	
	// Check specific record content
	if txnRecords[1].Key != "key1" || txnRecords[1].Value != "value1" {
		t.Errorf("Put record 1: expected key1=value1, got %s=%s", txnRecords[1].Key, txnRecords[1].Value)
	}
	
	if txnRecords[3].Key != "key3" {
		t.Errorf("Delete record: expected key3, got %s", txnRecords[3].Key)
	}
}

func TestRecordEncodeDecode(t *testing.T) {
	// Test different types of records
	records := []*Record{
		NewCreateRecord(100),
		NewCommitRecord(100),
		NewRollbackRecord(100),
		NewPutRecord(100, "test_key", "test_value"),
		NewDeleteRecord(100, "delete_key"),
	}
	
	for i, original := range records {
		// Encode
		data := original.Encode()
		
		// Decode
		decoded, err := DecodeRecords(data)
		if err != nil {
			t.Fatalf("Record %d: failed to decode: %v", i, err)
		}
		
		if len(decoded) != 1 {
			t.Fatalf("Record %d: expected 1 decoded record, got %d", i, len(decoded))
		}
		
		record := decoded[0]
		
		// Compare fields
		if record.TxnID != original.TxnID {
			t.Errorf("Record %d: TxnID mismatch: expected %d, got %d", i, original.TxnID, record.TxnID)
		}
		if record.OpType != original.OpType {
			t.Errorf("Record %d: OpType mismatch: expected %v, got %v", i, original.OpType, record.OpType)
		}
		if record.Key != original.Key {
			t.Errorf("Record %d: Key mismatch: expected %s, got %s", i, original.Key, record.Key)
		}
		if record.Value != original.Value {
			t.Errorf("Record %d: Value mismatch: expected %s, got %s", i, original.Value, record.Value)
		}
	}
}

func TestWALFileRotation(t *testing.T) {
	tempDir := t.TempDir()
	
	config := &Config{
		LogDir:        tempDir,
		BufferSize:    1, // Force frequent flushes
		FileSizeLimit: 100, // Very small file limit to force rotation
		CleanInterval: 5 * time.Second,
	}
	
	wal, err := New(config, 0)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()
	
	// Add many records to trigger file rotation
	for i := 0; i < 20; i++ {
		records := []*Record{
			NewPutRecord(uint64(i), "key_with_long_name_to_make_record_larger", "value_with_long_content_to_make_record_larger"),
		}
		
		err = wal.Log(records, true)
		if err != nil {
			t.Fatalf("Failed to log records: %v", err)
		}
	}
	
	// Check that multiple WAL files were created
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp dir: %v", err)
	}
	
	walFileCount := 0
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[:4] == "wal." {
			walFileCount++
		}
	}
	
	if walFileCount < 2 {
		t.Errorf("Expected at least 2 WAL files due to rotation, got %d", walFileCount)
	}
}
