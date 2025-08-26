package utils

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileManager provides utilities for file operations
type FileManager struct {
	dataDir string
	mu      sync.RWMutex
}

// NewFileManager creates a new file manager
func NewFileManager(dataDir string) *FileManager {
	return &FileManager{
		dataDir: dataDir,
	}
}

// EnsureDir creates the data directory if it doesn't exist
func (fm *FileManager) EnsureDir() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	return os.MkdirAll(fm.dataDir, 0755)
}

// GetDataDir returns the data directory path
func (fm *FileManager) GetDataDir() string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.dataDir
}

// GetSSTPath returns the path for an SST file
func (fm *FileManager) GetSSTPath(sstID uint64, level int) string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return filepath.Join(fm.dataDir, fmt.Sprintf("%08d_%d.sst", sstID, level))
}

// GetWALPath returns the path for a WAL file
func (fm *FileManager) GetWALPath(walName string) string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return filepath.Join(fm.dataDir, "wal", walName)
}

// GetWALPathByID returns the path for a WAL file by ID
func (fm *FileManager) GetWALPathByID(walID uint64) string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return filepath.Join(fm.dataDir, "wal", fmt.Sprintf("%06d.wal", walID))
}

// GetManifestPath returns the path for the manifest file
func (fm *FileManager) GetManifestPath() string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return filepath.Join(fm.dataDir, "MANIFEST")
}

// GetLockPath returns the path for the lock file
func (fm *FileManager) GetLockPath() string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return filepath.Join(fm.dataDir, "LOCK")
}

// ListSSTFiles returns all SST files in the data directory
func (fm *FileManager) ListSSTFiles() ([]string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	files, err := filepath.Glob(filepath.Join(fm.dataDir, "*_*.sst"))
	if err != nil {
		return nil, err
	}

	// Return just the filenames, not full paths
	var basenames []string
	for _, file := range files {
		basenames = append(basenames, filepath.Base(file))
	}

	return basenames, nil
}

// ListWALFiles returns all WAL files in the data directory
func (fm *FileManager) ListWALFiles() ([]string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	files, err := filepath.Glob(filepath.Join(fm.dataDir, "wal_*.log"))
	if err != nil {
		return nil, err
	}

	return files, nil
}

// DeleteFile safely deletes a file
func (fm *FileManager) DeleteFile(filename string) error {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	err := os.Remove(filename)
	if os.IsNotExist(err) {
		return nil // Don't error on non-existent files
	}
	return err
}

// FileExists checks if a file exists
func (fm *FileManager) FileExists(filename string) bool {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// GetFileSize returns the size of a file
func (fm *FileManager) GetFileSize(filename string) (int64, error) {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	info, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

// SyncFile forces a sync of file contents to disk
func SyncFile(file *os.File) error {
	return file.Sync()
}

// ChecksumWriter wraps a writer to compute CRC32 checksum
type ChecksumWriter struct {
	writer   io.Writer
	checksum uint32
	hasher   hash.Hash32
}

// NewChecksumWriter creates a new checksum writer
func NewChecksumWriter(writer io.Writer) *ChecksumWriter {
	return &ChecksumWriter{
		writer: writer,
		hasher: crc32.NewIEEE(),
	}
}

// Write implements io.Writer interface
func (cw *ChecksumWriter) Write(data []byte) (int, error) {
	n, err := cw.writer.Write(data)
	if err != nil {
		return n, err
	}

	cw.hasher.Write(data[:n])
	cw.checksum = cw.hasher.Sum32()

	return n, nil
}

// Checksum returns the current checksum
func (cw *ChecksumWriter) Checksum() uint32 {
	return cw.checksum
}

// WriteWithChecksum writes data with a trailing checksum
func (cw *ChecksumWriter) WriteWithChecksum(data []byte) error {
	// Write data
	_, err := cw.Write(data)
	if err != nil {
		return err
	}

	// Write checksum
	checksumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBytes, cw.checksum)
	_, err = cw.writer.Write(checksumBytes)

	return err
}

// ChecksumReader wraps a reader to verify CRC32 checksum
type ChecksumReader struct {
	reader   io.Reader
	checksum uint32
	hasher   hash.Hash32
}

// NewChecksumReader creates a new checksum reader
func NewChecksumReader(reader io.Reader) *ChecksumReader {
	return &ChecksumReader{
		reader: reader,
		hasher: crc32.NewIEEE(),
	}
}

// Read implements io.Reader interface
func (cr *ChecksumReader) Read(data []byte) (int, error) {
	n, err := cr.reader.Read(data)
	if err != nil && err != io.EOF {
		return n, err
	}

	if n > 0 {
		cr.hasher.Write(data[:n])
		cr.checksum = cr.hasher.Sum32()
	}

	return n, err
}

// Checksum returns the current checksum
func (cr *ChecksumReader) Checksum() uint32 {
	return cr.checksum
}

// ReadWithChecksum reads data and verifies the trailing checksum
func ReadWithChecksum(reader io.Reader, dataSize int) ([]byte, error) {
	// Read data
	data := make([]byte, dataSize)
	_, err := io.ReadFull(reader, data)
	if err != nil {
		return nil, err
	}

	// Read checksum
	checksumBytes := make([]byte, 4)
	_, err = io.ReadFull(reader, checksumBytes)
	if err != nil {
		return nil, err
	}

	expectedChecksum := binary.LittleEndian.Uint32(checksumBytes)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(data)
	if actualChecksum != expectedChecksum {
		return nil, ErrInvalidChecksum
	}

	return data, nil
}

// AtomicWrite performs an atomic write operation
func AtomicWrite(filename string, data []byte) error {
	tempFile := filename + ".tmp"

	// Write to temporary file
	file, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		file.Close()
		os.Remove(tempFile)
		return err
	}

	err = file.Sync()
	if err != nil {
		file.Close()
		os.Remove(tempFile)
		return err
	}

	err = file.Close()
	if err != nil {
		os.Remove(tempFile)
		return err
	}

	// Atomically rename temporary file to target file
	return os.Rename(tempFile, filename)
}

// CopyFile copies a file from source to destination
func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return destFile.Sync()
}

// TruncateFile truncates a file to the specified size
func TruncateFile(filename string, size int64) error {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Truncate(size)
}

// EnsureWALDir ensures the WAL directory exists
func (fm *FileManager) EnsureWALDir() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	walDir := filepath.Join(fm.dataDir, "wal")
	return os.MkdirAll(walDir, 0755)
}

// CreateFile creates a new file with the given data
func (fm *FileManager) CreateFile(filename string, data []byte) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	return os.WriteFile(filename, data, 0644)
}

// ReadFile reads the contents of a file
func (fm *FileManager) ReadFile(filename string) ([]byte, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	return os.ReadFile(filename)
}

// ListFiles lists all files in the data directory
func (fm *FileManager) ListFiles() ([]string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	entries, err := os.ReadDir(fm.dataDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}

	return files, nil
}

// AppendFile appends data to a file
func (fm *FileManager) AppendFile(filename string, data []byte) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

// TruncateFile truncates a file to the specified size
func (fm *FileManager) TruncateFile(filename string, size int64) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Truncate(size)
}

// SyncFile forces a sync of file contents to disk
func (fm *FileManager) SyncFile(filename string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Sync()
}

// CleanupFiles deletes multiple files
func (fm *FileManager) CleanupFiles(filenames []string) error {
	for _, filename := range filenames {
		if err := fm.DeleteFile(filename); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// AtomicWrite performs an atomic write operation
func (fm *FileManager) AtomicWrite(filename string, data []byte) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if !filepath.IsAbs(filename) {
		filename = filepath.Join(fm.dataDir, filename)
	}

	return AtomicWrite(filename, data)
}

// After returns a channel that will receive a time value after the specified duration in milliseconds
func After(ms int) <-chan time.Time {
	return time.After(time.Duration(ms) * time.Millisecond)
}
