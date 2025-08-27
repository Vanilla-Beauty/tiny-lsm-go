package utils

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileManagerBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Test GetSSTPath
	sstPath := fm.GetSSTPath(1, 0)
	expectedPath := filepath.Join(tempDir, "00000001_0.sst")
	assert.Equal(t, expectedPath, sstPath)

	// Test different SST IDs and levels
	sstPath2 := fm.GetSSTPath(123, 5)
	expectedPath2 := filepath.Join(tempDir, "00000123_5.sst")
	assert.Equal(t, expectedPath2, sstPath2)
}

func TestFileManagerWALPath(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Test GetWALPath
	walPath := fm.GetWALPath("000001.wal")
	expectedPath := filepath.Join(tempDir, "wal", "000001.wal")
	assert.Equal(t, expectedPath, walPath)
}

func TestFileManagerManifestPath(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Test GetManifestPath
	manifestPath := fm.GetManifestPath()
	expectedPath := filepath.Join(tempDir, "MANIFEST")
	assert.Equal(t, expectedPath, manifestPath)
}

func TestFileManagerEnsureWALDir(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// WAL directory should not exist initially
	walDir := filepath.Join(tempDir, "wal")
	_, err := os.Stat(walDir)
	assert.True(t, os.IsNotExist(err))

	// EnsureWALDir should create it
	err = fm.EnsureWALDir()
	require.NoError(t, err)

	// Directory should now exist
	stat, err := os.Stat(walDir)
	require.NoError(t, err)
	assert.True(t, stat.IsDir())

	// Calling again should not error
	err = fm.EnsureWALDir()
	assert.NoError(t, err)
}

func TestFileManagerCreateFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	testData := []byte("Hello, World!")
	fileName := "test.txt"

	// Create file
	err := fm.CreateFile(fileName, testData)
	require.NoError(t, err)

	// Verify file exists and has correct content
	filePath := filepath.Join(tempDir, fileName)
	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

func TestFileManagerReadFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	testData := []byte("Test file content")
	fileName := "read_test.txt"
	filePath := filepath.Join(tempDir, fileName)

	// Create test file
	err := os.WriteFile(filePath, testData, 0644)
	require.NoError(t, err)

	// Read file using FileManager
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

func TestFileManagerReadFileNotExists(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to read non-existent file
	_, err := fm.ReadFile("nonexistent.txt")
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestFileManagerDeleteFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "delete_test.txt"
	filePath := filepath.Join(tempDir, fileName)

	// Create test file
	err := os.WriteFile(filePath, []byte("test"), 0644)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(filePath)
	require.NoError(t, err)

	// Delete file
	err = fm.DeleteFile(fileName)
	require.NoError(t, err)

	// Verify file no longer exists
	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestFileManagerDeleteNonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to delete non-existent file - should not error
	err := fm.DeleteFile("nonexistent.txt")
	assert.NoError(t, err)
}

func TestFileManagerFileExists(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "exists_test.txt"
	filePath := filepath.Join(tempDir, fileName)

	// File should not exist initially
	exists := fm.FileExists(fileName)
	assert.False(t, exists)

	// Create file
	err := os.WriteFile(filePath, []byte("test"), 0644)
	require.NoError(t, err)

	// File should now exist
	exists = fm.FileExists(fileName)
	assert.True(t, exists)
}

func TestFileManagerListFiles(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create test files
	testFiles := []string{"file1.txt", "file2.sst", "file3.log"}
	for _, fileName := range testFiles {
		err := fm.CreateFile(fileName, []byte("test"))
		require.NoError(t, err)
	}

	// List all files
	files, err := fm.ListFiles()
	require.NoError(t, err)

	// Should contain all created files
	fileSet := make(map[string]bool)
	for _, file := range files {
		fileSet[file] = true
	}

	for _, expectedFile := range testFiles {
		assert.True(t, fileSet[expectedFile], "Should contain file %s", expectedFile)
	}
}

func TestFileManagerListSSTFiles(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create mixed files
	allFiles := []string{"1_0.sst", "2_1.sst", "test.txt", "3_0.sst", "log.wal"}
	expectedSSTFiles := []string{"1_0.sst", "2_1.sst", "3_0.sst"}

	for _, fileName := range allFiles {
		err := fm.CreateFile(fileName, []byte("test"))
		require.NoError(t, err)
	}

	// List only SST files
	sstFiles, err := fm.ListSSTFiles()
	require.NoError(t, err)

	assert.Equal(t, len(expectedSSTFiles), len(sstFiles))

	sstFileSet := make(map[string]bool)
	for _, file := range sstFiles {
		sstFileSet[file] = true
	}

	for _, expectedFile := range expectedSSTFiles {
		assert.True(t, sstFileSet[expectedFile], "Should contain SST file %s", expectedFile)
	}
}

func TestFileManagerGetFileSize(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	testData := []byte("This is test data for size checking")
	fileName := "size_test.txt"

	// Create file
	err := fm.CreateFile(fileName, testData)
	require.NoError(t, err)

	// Get file size
	size, err := fm.GetFileSize(fileName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), size)
}

func TestFileManagerGetFileSizeNotExists(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to get size of non-existent file
	_, err := fm.GetFileSize("nonexistent.txt")
	assert.Error(t, err)
}

func TestFileManagerLargeFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create 1MB of random data
	largeData := make([]byte, 1024*1024)
	_, err := rand.Read(largeData)
	require.NoError(t, err)

	fileName := "large_file.dat"

	// Create large file
	err = fm.CreateFile(fileName, largeData)
	require.NoError(t, err)

	// Verify file size
	size, err := fm.GetFileSize(fileName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)), size)

	// Read and verify content
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, largeData, content)
}

func TestFileManagerConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	const numGoroutines = 10
	const filesPerGoroutine = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*filesPerGoroutine)

	// Create files concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < filesPerGoroutine; j++ {
				fileName := fmt.Sprintf("concurrent_%d_%d.txt", goroutineID, j)
				data := []byte(fmt.Sprintf("data from goroutine %d, file %d", goroutineID, j))

				if err := fm.CreateFile(fileName, data); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent file creation error: %v", err)
	}

	// Verify all files were created
	files, err := fm.ListFiles()
	require.NoError(t, err)
	assert.Equal(t, numGoroutines*filesPerGoroutine, len(files))
}

func TestFileManagerAppendFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "append_test.txt"
	initialData := []byte("Initial content\n")
	appendData := []byte("Appended content\n")

	// Create initial file
	err := fm.CreateFile(fileName, initialData)
	require.NoError(t, err)

	// Append to file
	err = fm.AppendFile(fileName, appendData)
	require.NoError(t, err)

	// Verify combined content
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)

	expectedContent := append(initialData, appendData...)
	assert.Equal(t, expectedContent, content)
}

func TestFileManagerAppendToNonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "nonexistent.txt"
	data := []byte("New file content")

	// Append to non-existent file should create it
	err := fm.AppendFile(fileName, data)
	require.NoError(t, err)

	// Verify file was created with correct content
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, data, content)
}

func TestFileManagerTruncateFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "truncate_test.txt"
	originalData := []byte("This is a long file that will be truncated")

	// Create file
	err := fm.CreateFile(fileName, originalData)
	require.NoError(t, err)

	// Truncate to 10 bytes
	err = fm.TruncateFile(fileName, 10)
	require.NoError(t, err)

	// Verify truncated content
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, originalData[:10], content)

	// Verify file size
	size, err := fm.GetFileSize(fileName)
	require.NoError(t, err)
	assert.Equal(t, int64(10), size)
}

func TestFileManagerTruncateNonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to truncate non-existent file
	err := fm.TruncateFile("nonexistent.txt", 10)
	assert.Error(t, err)
}

func TestFileManagerSyncFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "sync_test.txt"
	data := []byte("Data to sync")

	// Create file
	err := fm.CreateFile(fileName, data)
	require.NoError(t, err)

	// Sync should not error
	err = fm.SyncFile(fileName)
	assert.NoError(t, err)
}

func TestFileManagerSyncNonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Try to sync non-existent file
	err := fm.SyncFile("nonexistent.txt")
	assert.Error(t, err)
}

func TestFileManagerErrorHandling(t *testing.T) {
	// Create FileManager with non-existent directory
	fm := NewFileManager("/nonexistent/directory")

	// Operations should fail gracefully
	err := fm.CreateFile("test.txt", []byte("test"))
	assert.Error(t, err)

	_, err = fm.ReadFile("test.txt")
	assert.Error(t, err)

	_, err = fm.ListFiles()
	assert.Error(t, err)
}

func TestFileManagerCleanup(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	// Create several files
	fileNames := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, fileName := range fileNames {
		err := fm.CreateFile(fileName, []byte("test"))
		require.NoError(t, err)
	}

	// Verify files exist
	for _, fileName := range fileNames {
		assert.True(t, fm.FileExists(fileName))
	}

	// Cleanup specific files
	err := fm.CleanupFiles(fileNames[:2])
	require.NoError(t, err)

	// First two files should be gone
	assert.False(t, fm.FileExists(fileNames[0]))
	assert.False(t, fm.FileExists(fileNames[1]))
	assert.True(t, fm.FileExists(fileNames[2]))
}

func TestFileManagerAtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	fm := NewFileManager(tempDir)

	fileName := "atomic_test.txt"
	data := []byte("Atomic write test data")

	// Atomic write should create the file
	err := fm.AtomicWrite(fileName, data)
	require.NoError(t, err)

	// Verify content
	content, err := fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, data, content)

	// Atomic write should replace existing content
	newData := []byte("New atomic data")
	err = fm.AtomicWrite(fileName, newData)
	require.NoError(t, err)

	content, err = fm.ReadFile(fileName)
	require.NoError(t, err)
	assert.Equal(t, newData, content)
}

func BenchmarkFileManagerCreateFile(b *testing.B) {
	tempDir := b.TempDir()
	fm := NewFileManager(tempDir)

	data := make([]byte, 1024) // 1KB files
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileName := fmt.Sprintf("bench_file_%d.dat", i)
		err := fm.CreateFile(fileName, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileManagerReadFile(b *testing.B) {
	tempDir := b.TempDir()
	fm := NewFileManager(tempDir)

	// Create test files
	data := make([]byte, 1024)
	rand.Read(data)

	fileNames := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		fileName := fmt.Sprintf("read_bench_%d.dat", i)
		fileNames[i] = fileName
		err := fm.CreateFile(fileName, data)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fm.ReadFile(fileNames[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileManagerLargeFileOperations(b *testing.B) {
	tempDir := b.TempDir()
	fm := NewFileManager(tempDir)

	// 1MB file
	largeData := make([]byte, 1024*1024)
	rand.Read(largeData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fileName := fmt.Sprintf("large_bench_%d.dat", i)

		// Create large file
		err := fm.CreateFile(fileName, largeData)
		if err != nil {
			b.Fatal(err)
		}

		// Read it back
		_, err = fm.ReadFile(fileName)
		if err != nil {
			b.Fatal(err)
		}
	}
}
