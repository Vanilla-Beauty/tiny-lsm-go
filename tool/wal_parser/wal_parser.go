package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"tiny-lsm-go/pkg/wal"
)

func main() {
	var (
		logDir       = flag.String("dir", "./wal", "WAL log directory")
		showAll      = flag.Bool("all", false, "Show all records, not just uncommitted ones")
		checkpointID = flag.Uint64("checkpoint", 0, "Checkpoint transaction ID for filtering")
	)

	flag.Parse()

	if _, err := os.Stat(*logDir); os.IsNotExist(err) {
		fmt.Printf("WAL directory does not exist: %s\n", *logDir)
		os.Exit(1)
	}

	// Get all WAL files
	walFiles, err := getWALFiles(*logDir)
	if err != nil {
		fmt.Printf("Error listing WAL files: %v\n", err)
		os.Exit(1)
	}

	if len(walFiles) == 0 {
		fmt.Println("No WAL files found")
		return
	}

	// Sort by sequence number
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].seq < walFiles[j].seq
	})

	fmt.Printf("WAL Directory: %s\n", *logDir)
	fmt.Printf("Checkpoint Txn ID: %d\n", *checkpointID)
	fmt.Printf("Show All Records: %t\n", *showAll)
	fmt.Println()

	// Process each WAL file
	for _, walFile := range walFiles {
		fmt.Printf("=== WAL File: %s (Sequence: %d) ===\n", walFile.name, walFile.seq)

		filePath := filepath.Join(*logDir, walFile.name)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			fmt.Printf("Error accessing file: %v\n", err)
			continue
		}

		fmt.Printf("File Size: %d bytes\n", fileInfo.Size())

		records, err := parseWALFile(filePath)
		if err != nil {
			fmt.Printf("Error parsing WAL file: %v\n", err)
			continue
		}

		if len(records) == 0 {
			fmt.Println("No records found")
			fmt.Println()
			continue
		}

		// Filter records if not showing all
		if !*showAll && *checkpointID > 0 {
			filteredRecords := make([]*wal.Record, 0)
			for _, record := range records {
				if record.TxnID > *checkpointID {
					filteredRecords = append(filteredRecords, record)
				}
			}
			records = filteredRecords
		}

		// Group records by transaction ID
		txnRecords := make(map[uint64][]*wal.Record)
		for _, record := range records {
			txnRecords[record.TxnID] = append(txnRecords[record.TxnID], record)
		}

		// Sort transaction IDs
		txnIDs := make([]uint64, 0, len(txnRecords))
		for txnID := range txnRecords {
			txnIDs = append(txnIDs, txnID)
		}
		sort.Slice(txnIDs, func(i, j int) bool {
			return txnIDs[i] < txnIDs[j]
		})

		// Display records grouped by transaction
		for _, txnID := range txnIDs {
			fmt.Printf("\nTransaction ID: %d\n", txnID)
			fmt.Printf("Records Count: %d\n", len(txnRecords[txnID]))

			for i, record := range txnRecords[txnID] {
				fmt.Printf("  [%d] %s\n", i+1, formatRecord(record))
			}
		}

		fmt.Printf("\nTotal Records: %d\n", len(records))
		fmt.Printf("Total Transactions: %d\n\n", len(txnRecords))
	}
}

// parseWALFile reads and decodes records from a WAL file
func parseWALFile(filePath string) ([]*wal.Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL file: %w", err)
	}

	records, err := wal.DecodeRecords(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	return records, nil
}

// formatRecord formats a WAL record for display
func formatRecord(record *wal.Record) string {
	timestamp := record.Timestamp.Format(time.RFC3339Nano)

	switch record.OpType {
	case wal.OpCreate:
		return fmt.Sprintf("CREATE  | %s", timestamp)
	case wal.OpCommit:
		return fmt.Sprintf("COMMIT  | %s", timestamp)
	case wal.OpRollback:
		return fmt.Sprintf("ROLLBACK| %s", timestamp)
	case wal.OpPut:
		return fmt.Sprintf("PUT     | Key: %-20s | Value: %-20s | %s",
			truncateString(record.Key, 20),
			truncateString(record.Value, 20),
			timestamp)
	case wal.OpDelete:
		return fmt.Sprintf("DELETE  | Key: %-20s | %s",
			truncateString(record.Key, 20),
			timestamp)
	default:
		return fmt.Sprintf("UNKNOWN | OpType: %d | %s", record.OpType, timestamp)
	}
}

// truncateString truncates a string to the specified length and adds "..." if needed
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// walFileInfo holds information about a WAL file
type walFileInfo struct {
	name string
	seq  int64
}

// getWALFiles returns a list of WAL files in the directory
func getWALFiles(logDir string) ([]walFileInfo, error) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}

	var walFiles []walFileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, "wal.") {
			continue
		}

		// Extract sequence number
		seqStr := strings.TrimPrefix(name, "wal.")
		seq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			continue
		}

		walFiles = append(walFiles, walFileInfo{
			name: name,
			seq:  seq,
		})
	}

	return walFiles, nil
}
