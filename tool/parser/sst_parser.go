package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"tiny-lsm-go/pkg/sst"
	"tiny-lsm-go/pkg/utils"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <sst_file_path> [--no-bloom]\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]
	showBloom := true

	for _, arg := range os.Args[2:] {
		if arg == "--no-bloom" {
			showBloom = false
		}
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("Error accessing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("SST File: %s\n", filePath)
	fmt.Printf("File Size: %d bytes\n", fileInfo.Size())
	fmt.Println()

	// Open the SST file
	sstFile, err := sst.Open(0, filePath, nil)
	if err != nil {
		fmt.Printf("Error opening SST file: %v\n", err)
		os.Exit(1)
	}
	defer sstFile.Close()

	// Print SST header information
	fmt.Println("=== SST Header Information ===")
	fmt.Printf("SST ID: %d\n", sstFile.ID())
	fmt.Printf("First Key: %s\n", sstFile.FirstKey())
	fmt.Printf("Last Key: %s\n", sstFile.LastKey())
	fmt.Printf("Number of Blocks: %d\n", sstFile.NumBlocks())
	minTxnID, maxTxnID := sstFile.TxnRange()
	fmt.Printf("Transaction ID Range: %d - %d\n", minTxnID, maxTxnID)

	// Fix: Read file footer to get metaOffset and bloomOffset
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file for offset read: %v\n", err)
		return
	}
	defer file.Close()

	footerSize := int64(24)
	footer := make([]byte, footerSize)
	_, err = file.ReadAt(footer, fileInfo.Size()-footerSize)
	if err != nil {
		fmt.Printf("Error reading footer: %v\n", err)
		return
	}

	metaOffset := binary.LittleEndian.Uint32(footer[0:4])
	bloomOffset := binary.LittleEndian.Uint32(footer[4:8])

	fmt.Printf("Meta Offset: %+v\n", metaOffset)
	fmt.Printf("Bloom Offset: %+v\n", bloomOffset)
	fmt.Println()
	fmt.Println()

	// Print block metadata
	fmt.Println("=== Block Metadata ===")
	for i := 0; i < sstFile.NumBlocks(); i++ {
		blk, err := sstFile.ReadBlock(i)
		if err != nil {
			fmt.Printf("  Error reading block %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Block %d:\n", i)
		fmt.Printf("  First Key: %s\n", blk.FirstKey())
		fmt.Printf("  Last Key: %s\n", blk.LastKey())
		// Offset information cannot be directly obtained from Block, need to read from file
		fmt.Println()
	}

	if showBloom {
		fmt.Println("=== Bloom Filter Information ===")

		// Read bloom filter data directly from file
		bloomSize := int64(metaOffset) - int64(bloomOffset)
		if bloomSize > 0 && bloomOffset > 0 {
			bloomData := make([]byte, bloomSize)
			_, err = file.ReadAt(bloomData, int64(bloomOffset))
			if err != nil {
				fmt.Printf("Error reading bloom filter: %v\n", err)
			} else {
				bloomFilter := utils.DeserializeBloomFilter(bloomData)
				if bloomFilter != nil {
					fmt.Printf("Bit Set Size: %d bits\n", bloomFilter.Size())
					fmt.Printf("Hash Functions: %d\n", bloomFilter.NumHashes())
					fmt.Printf("Estimated Items: %d\n", bloomFilter.NumItems())
				} else {
					fmt.Println("Failed to deserialize bloom filter")
				}
			}
		} else {
			fmt.Println("No bloom filter found in SST file")
		}
		fmt.Println()
	}

	// Print block contents
	fmt.Println("=== Block Contents ===")
	for i := 0; i < sstFile.NumBlocks(); i++ {
		fmt.Printf("Block %d:\n", i)

		blk, err := sstFile.ReadBlock(i)
		if err != nil {
			fmt.Printf("  Error reading block: %v\n", err)
			continue
		}

		fmt.Printf("  Number of Entries: %d\n", blk.NumEntries())

		for j := 0; j < blk.NumEntries(); j++ {
			entry, err := blk.GetEntry(j)
			if err != nil {
				fmt.Printf("    Error reading entry %d: %v\n", j, err)
				continue
			}

			deleted := ""
			if entry.Value == "" {
				deleted = " (DELETED)"
			}

			fmt.Printf("    Entry %d: Key=%s, Value=%s, TxnID=%d%s\n",
				j, entry.Key, entry.Value, entry.TxnID, deleted)
		}
		fmt.Println()
	}

	// Print raw footer
	fmt.Println("=== Raw Footer ===")
	file, err = os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file for footer read: %v\n", err)
		return
	}
	defer file.Close()

	footerSize = int64(24)
	footer = make([]byte, footerSize)
	_, err = file.ReadAt(footer, fileInfo.Size()-footerSize)
	if err != nil {
		fmt.Printf("Error reading footer: %v\n", err)
		return
	}

	fmt.Printf("Meta Offset: %d (0x%08x)\n", binary.LittleEndian.Uint32(footer[0:4]), binary.LittleEndian.Uint32(footer[0:4]))
	fmt.Printf("Bloom Offset: %d (0x%08x)\n", binary.LittleEndian.Uint32(footer[4:8]), binary.LittleEndian.Uint32(footer[4:8]))
	fmt.Printf("Min TxnID: %d (0x%016x)\n", binary.LittleEndian.Uint64(footer[8:16]), binary.LittleEndian.Uint64(footer[8:16]))
	fmt.Printf("Max TxnID: %d (0x%016x)\n", binary.LittleEndian.Uint64(footer[16:24]), binary.LittleEndian.Uint64(footer[16:24]))
}
