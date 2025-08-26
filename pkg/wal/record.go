package wal

import (
	"encoding/binary"
	"errors"
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
	return &Record{
		TxnID:     txnID,
		OpType:    OpCreate,
		Timestamp: time.Now(),
	}
}

// NewCommitRecord creates a COMMIT record
func NewCommitRecord(txnID uint64) *Record {
	return &Record{
		TxnID:     txnID,
		OpType:    OpCommit,
		Timestamp: time.Now(),
	}
}

// NewRollbackRecord creates a ROLLBACK record
func NewRollbackRecord(txnID uint64) *Record {
	return &Record{
		TxnID:     txnID,
		OpType:    OpRollback,
		Timestamp: time.Now(),
	}
}

// NewPutRecord creates a PUT record
func NewPutRecord(txnID uint64, key, value string) *Record {
	return &Record{
		TxnID:     txnID,
		OpType:    OpPut,
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
}

// NewDeleteRecord creates a DELETE record
func NewDeleteRecord(txnID uint64, key string) *Record {
	return &Record{
		TxnID:     txnID,
		OpType:    OpDelete,
		Key:       key,
		Timestamp: time.Now(),
	}
}

// Encode serializes the record to bytes
// Format: [RecordLen(2)] [TxnID(8)] [OpType(1)] [KeyLen(2)] [Key] [ValueLen(2)] [Value] [Timestamp(8)]
func (r *Record) Encode() []byte {
	// Calculate total length
	keyLen := uint16(len(r.Key))
	valueLen := uint16(len(r.Value))
	
	// Base size: RecordLen(2) + TxnID(8) + OpType(1) + Timestamp(8) = 19 bytes
	baseSize := 19
	totalLen := baseSize + int(keyLen) + int(valueLen)
	
	// Add key length and value length fields
	if r.OpType == OpPut || r.OpType == OpDelete {
		totalLen += 2 // KeyLen field
	}
	if r.OpType == OpPut {
		totalLen += 2 // ValueLen field
	}
	
	r.RecordLen = uint16(totalLen)
	
	buf := make([]byte, totalLen)
	offset := 0
	
	// Encode RecordLen
	binary.LittleEndian.PutUint16(buf[offset:], r.RecordLen)
	offset += 2
	
	// Encode TxnID
	binary.LittleEndian.PutUint64(buf[offset:], r.TxnID)
	offset += 8
	
	// Encode OpType
	buf[offset] = uint8(r.OpType)
	offset += 1
	
	// Encode Key if present
	if r.OpType == OpPut || r.OpType == OpDelete {
		binary.LittleEndian.PutUint16(buf[offset:], keyLen)
		offset += 2
		copy(buf[offset:], []byte(r.Key))
		offset += int(keyLen)
	}
	
	// Encode Value if present
	if r.OpType == OpPut {
		binary.LittleEndian.PutUint16(buf[offset:], valueLen)
		offset += 2
		copy(buf[offset:], []byte(r.Value))
		offset += int(valueLen)
	}
	
	// Encode Timestamp (Unix timestamp in nanoseconds)
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.Timestamp.UnixNano()))
	
	return buf
}

// DecodeRecords decodes multiple records from bytes
func DecodeRecords(data []byte) ([]*Record, error) {
	if len(data) == 0 {
		return nil, nil
	}
	
	var records []*Record
	offset := 0
	
	for offset < len(data) {
		// Check if we have enough data for the record length
		if offset+2 > len(data) {
			break
		}
		
		// Read record length
		recordLen := binary.LittleEndian.Uint16(data[offset:])
		if recordLen < 19 { // Minimum record size
			return nil, fmt.Errorf("invalid record length: %d", recordLen)
		}
		
		// Check if we have enough data for the complete record
		if offset+int(recordLen) > len(data) {
			return nil, fmt.Errorf("incomplete record: need %d bytes, have %d", recordLen, len(data)-offset)
		}
		
		// Decode the record
		record, err := decodeRecord(data[offset : offset+int(recordLen)])
		if err != nil {
			return nil, fmt.Errorf("failed to decode record at offset %d: %w", offset, err)
		}
		
		records = append(records, record)
		offset += int(recordLen)
	}
	
	return records, nil
}

// decodeRecord decodes a single record from bytes
func decodeRecord(data []byte) (*Record, error) {
	if len(data) < 19 { // Minimum record size
		return nil, errors.New("record data too short")
	}
	
	record := &Record{}
	offset := 0
	
	// Decode RecordLen
	record.RecordLen = binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	
	// Decode TxnID
	record.TxnID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	
	// Decode OpType
	record.OpType = OperationType(data[offset])
	offset += 1
	
	// Decode Key if present
	if record.OpType == OpPut || record.OpType == OpDelete {
		if offset+2 > len(data) {
			return nil, errors.New("insufficient data for key length")
		}
		keyLen := binary.LittleEndian.Uint16(data[offset:])
		offset += 2
		
		if offset+int(keyLen) > len(data) {
			return nil, errors.New("insufficient data for key")
		}
		record.Key = string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)
	}
	
	// Decode Value if present
	if record.OpType == OpPut {
		if offset+2 > len(data) {
			return nil, errors.New("insufficient data for value length")
		}
		valueLen := binary.LittleEndian.Uint16(data[offset:])
		offset += 2
		
		if offset+int(valueLen) > len(data) {
			return nil, errors.New("insufficient data for value")
		}
		record.Value = string(data[offset : offset+int(valueLen)])
		offset += int(valueLen)
	}
	
	// Decode Timestamp
	if offset+8 > len(data) {
		return nil, errors.New("insufficient data for timestamp")
	}
	timestampNanos := binary.LittleEndian.Uint64(data[offset:])
	record.Timestamp = time.Unix(0, int64(timestampNanos))
	
	return record, nil
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
