
# Tiny-LSM-Go Tools

This directory contains tools for debugging and analyzing Tiny-LSM-Go database files.

## Tool List

### 1. SST Parser

Used to parse and inspect the internal structure of SST (Sorted String Table) files.

**Usage**

```bash
cd tool/sst_parser && go build
cd -
./tool/sst_parser/sst_parser <sst_file_path> [--no-bloom]
./tool/sst_parser/sst_parser ./tool/test_data/example.sst # example
```
Parameters:
- <sst_file_path>: Path to the SST file
- --no-bloom: Optional parameter to hide detailed Bloom filter information

Features
- Display SST file header information
- List all data blocks and their metadata
- Show Bloom filter contents (unless --no-bloom is specified)
- Display transaction ID range and other metadata- 

### 2. Wal Parser
Used to parse and view the contents of WAL (Write-Ahead Log) files.

**Usage**

```bash
cd tool/wal_parser && go build
cd -
./tool/wal_parser/wal_parser -dir <wal_dir>
./tool/wal_parser/wal_parser -dir ./tool/test_data/wal # example
```

Parameters:
- -dir: WAL directory path (default is "./wal")
- -all: Show all records, not just uncommitted ones
- -checkpoint: Checkpoint transaction ID for filtering records

Features
- Display list of WAL files
- Parse and display WAL records
- Filter records by transaction ID
- Show operation types (PUT/DELETE) and associated data