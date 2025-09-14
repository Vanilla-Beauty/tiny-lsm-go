package wal

import (
	"fmt"
	"time"
)

// OperationType represents the type of operation in a WAL record
type OperationType uint8

const (
	// OpCreate marks the creation of a transaction
	OpCreate OperationType = iota
	// OpCommit marks the commit of a transaction
	OpCommit
	// OpRollback marks the rollback of a transaction
	OpRollback
	// OpPut represents a put operation
	OpPut
	// OpDelete represents a delete operation
	OpDelete
)

// String returns the string representation of the operation type
func (op OperationType) String() string {
	switch op {
	case OpCreate:
		return "CREATE"
	case OpCommit:
		return "COMMIT"
	case OpRollback:
		return "ROLLBACK"
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", op)
	}
}

// Record represents a single WAL record
type Record struct {
	// RecordLen is the total length of this record
	RecordLen uint16
	// TxnID is the transaction ID
	TxnID uint64
	// OpType is the operation type
	OpType OperationType
	// Key is the key for PUT/DELETE operations (empty for CREATE/COMMIT/ROLLBACK)
	Key string
	// Value is the value for PUT operations (empty for others)
	Value string
	// Timestamp is when this record was created
	Timestamp time.Time
}

// NewCreateRecord creates a CREATE record
func NewCreateRecord(txnID uint64) *Record {
	// TODO: Lab 5.3
	return nil
}

// NewCommitRecord creates a COMMIT record
func NewCommitRecord(txnID uint64) *Record {
	// TODO: Lab 5.3
	return nil
}

// NewRollbackRecord creates a ROLLBACK record
func NewRollbackRecord(txnID uint64) *Record {
	// TODO: Lab 5.3
	return nil
}

// NewPutRecord creates a PUT record
func NewPutRecord(txnID uint64, key, value string) *Record {
	// TODO: Lab 5.3
	return nil
}

// NewDeleteRecord creates a DELETE record
func NewDeleteRecord(txnID uint64, key string) *Record {
	// TODO: Lab 5.3
	return nil
}

// Encode serializes the record to bytes
// Format: [RecordLen(2)] [TxnID(8)] [OpType(1)] [KeyLen(2)] [Key] [ValueLen(2)] [Value] [Timestamp(8)]
func (r *Record) Encode() []byte {
	// TODO: Lab 5.3
	return nil
}

// DecodeRecords decodes multiple records from bytes
func DecodeRecords(data []byte) ([]*Record, error) {
	// TODO: Lab 5.3
	return nil, nil
}

// String returns a string representation of the record for debugging
func (r *Record) String() string {
	switch r.OpType {
	case OpCreate, OpCommit, OpRollback:
		return fmt.Sprintf("Record{TxnID: %d, OpType: %s, Timestamp: %s}",
			r.TxnID, r.OpType, r.Timestamp.Format(time.RFC3339Nano))
	case OpPut:
		return fmt.Sprintf("Record{TxnID: %d, OpType: %s, Key: %s, Value: %s, Timestamp: %s}",
			r.TxnID, r.OpType, r.Key, r.Value, r.Timestamp.Format(time.RFC3339Nano))
	case OpDelete:
		return fmt.Sprintf("Record{TxnID: %d, OpType: %s, Key: %s, Timestamp: %s}",
			r.TxnID, r.OpType, r.Key, r.Timestamp.Format(time.RFC3339Nano))
	default:
		return fmt.Sprintf("Record{TxnID: %d, OpType: %s, Key: %s, Value: %s, Timestamp: %s}",
			r.TxnID, r.OpType, r.Key, r.Value, r.Timestamp.Format(time.RFC3339Nano))
	}
}
