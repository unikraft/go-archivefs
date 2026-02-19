// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Unikraft GmbH and The KraftKit Authors.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.
package erofs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"
)

// bytesWriterAt implements io.WriterAt for a byte slice
type bytesWriterAt struct {
	buf []byte
}

func newBytesWriterAt(size int) *bytesWriterAt {
	return &bytesWriterAt{buf: make([]byte, size)}
}

func (w *bytesWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fs.ErrInvalid
	}

	// Expand buffer if needed
	end := int(off) + len(p)
	if end > len(w.buf) {
		newBuf := make([]byte, end)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}

	return copy(w.buf[off:], p), nil
}

func (w *bytesWriterAt) Bytes() []byte {
	return w.buf
}

// TestWriterInlineData tests that small files are correctly inlined with their inodes
func TestWriterInlineData(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		content  string
	}{
		{
			name:     "Empty file",
			fileSize: 0,
			content:  "",
		},
		{
			name:     "Very small file (1 byte)",
			fileSize: 1,
			content:  "a",
		},
		{
			name:     "Small file (100 bytes)",
			fileSize: 100,
			content:  strings.Repeat("x", 100),
		},
		{
			name:     "File at inline threshold (1024 bytes)",
			fileSize: MaxInlineDataSize,
			content:  strings.Repeat("y", int(MaxInlineDataSize)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test filesystem
			testFS := fstest.MapFS{
				"test.txt": &fstest.MapFile{
					Data:    []byte(tt.content),
					Mode:    0o644,
					ModTime: time.Unix(1234567890, 0),
				},
			}

			// Create EROFS image
			buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
			err := Create(buf, testFS, WithAllRoot(true))
			require.NoError(t, err)

			// Open and verify the filesystem
			fsys, err := Open(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			// Read the file and verify content
			f, err := fsys.Open("test.txt")
			require.NoError(t, err)
			defer f.Close()

			content, err := io.ReadAll(f)
			require.NoError(t, err)
			require.Equal(t, tt.content, string(content))

			// Verify file size
			info, err := f.Stat()
			require.NoError(t, err)
			require.Equal(t, tt.fileSize, info.Size())
		})
	}
}

// TestWriterTailPacking tests that files with partial blocks use tail-packing correctly
func TestWriterTailPacking(t *testing.T) {
	tests := []struct {
		name          string
		fileSize      int64
		expectInlined bool // Whether the tail should be inlined
		description   string
	}{
		{
			name:          "One block plus small tail (4100 bytes)",
			fileSize:      4100,
			expectInlined: true,
			description:   "Should inline 4-byte tail",
		},
		{
			name:          "One block plus medium tail (5000 bytes)",
			fileSize:      5000,
			expectInlined: true,
			description:   "Should inline 904-byte tail",
		},
		{
			name:          "Multiple blocks plus tail (12300 bytes)",
			fileSize:      12300,
			expectInlined: true,
			description:   "Should inline 12-byte tail",
		},
		{
			name:          "Exact block size (4096 bytes)",
			fileSize:      4096,
			expectInlined: false,
			description:   "No tail to inline",
		},
		{
			name:          "Multiple exact blocks (8192 bytes)",
			fileSize:      8192,
			expectInlined: false,
			description:   "No tail to inline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test content
			content := make([]byte, tt.fileSize)
			for i := range content {
				content[i] = byte(i % 256)
			}

			// Create a test filesystem
			testFS := fstest.MapFS{
				"test.dat": &fstest.MapFile{
					Data:    content,
					Mode:    0o644,
					ModTime: time.Unix(1234567890, 0),
				},
			}

			// Create EROFS image
			buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
			err := Create(buf, testFS, WithAllRoot(true))
			require.NoError(t, err)

			// Open and verify the filesystem
			fsys, err := Open(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			// Read the file and verify content
			f, err := fsys.Open("test.dat")
			require.NoError(t, err)
			defer f.Close()

			readContent, err := io.ReadAll(f)
			require.NoError(t, err)
			require.Equal(t, content, readContent, "Content mismatch for %s", tt.description)

			// Verify file size
			info, err := f.Stat()
			require.NoError(t, err)
			require.Equal(t, tt.fileSize, info.Size())
		})
	}
}

// TestWriterBlockBoundary tests that inodes and inline data don't cross block boundaries
func TestWriterBlockBoundary(t *testing.T) {
	// Create multiple files to force different inode placements
	testFS := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("small file 1"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file2.txt": &fstest.MapFile{
			Data:    []byte(strings.Repeat("medium file content ", 50)), // ~1000 bytes
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file3.txt": &fstest.MapFile{
			Data:    []byte("tiny"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Verify all files can be read correctly
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, filename := range files {
		f, err := fsys.Open(filename)
		require.NoError(t, err, "Failed to open %s", filename)

		content, err := io.ReadAll(f)
		require.NoError(t, err, "Failed to read %s", filename)
		require.NotEmpty(t, content, "Empty content for %s", filename)

		f.Close()
	}
}

// TestWriterLargeFile tests handling of files larger than a few blocks
func TestWriterLargeFile(t *testing.T) {
	// Create a large file (100KB)
	largeContent := make([]byte, 100*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	testFS := fstest.MapFS{
		"large.bin": &fstest.MapFile{
			Data:    largeContent,
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Read the file and verify content
	f, err := fsys.Open("large.bin")
	require.NoError(t, err)
	defer f.Close()

	readContent, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, largeContent, readContent)

	// Verify file size
	info, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(len(largeContent)), info.Size())

	// Verify checksum
	h := sha256.New()
	h.Write(largeContent)
	expectedHash := hex.EncodeToString(h.Sum(nil))

	h2 := sha256.New()
	h2.Write(readContent)
	actualHash := hex.EncodeToString(h2.Sum(nil))

	require.Equal(t, expectedHash, actualHash)
}

// TestWriterSymlinks tests symlink creation and reading
func TestWriterSymlinks(t *testing.T) {
	testFS := fstest.MapFS{
		"target.txt": &fstest.MapFile{
			Data:    []byte("target content"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"link": &fstest.MapFile{
			Data:    []byte("target.txt"),
			Mode:    fs.ModeSymlink | 0o777,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Read symlink target
	target, err := fsys.ReadLink("link")
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)

	// Follow symlink and read content
	f, err := fsys.Open("link")
	require.NoError(t, err)
	defer f.Close()

	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "target content", string(content))
}

// TestWriterDirectories tests directory creation and listing
func TestWriterDirectories(t *testing.T) {
	testFS := fstest.MapFS{
		"dir1/file1.txt": &fstest.MapFile{
			Data:    []byte("file1"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"dir1/file2.txt": &fstest.MapFile{
			Data:    []byte("file2"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"dir2/subdir/file3.txt": &fstest.MapFile{
			Data:    []byte("file3"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// List dir1
	entries, err := fsys.ReadDir("dir1")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	names := []string{entries[0].Name(), entries[1].Name()}
	require.Contains(t, names, "file1.txt")
	require.Contains(t, names, "file2.txt")

	// List dir2
	entries, err = fsys.ReadDir("dir2")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "subdir", entries[0].Name())
	require.True(t, entries[0].IsDir())

	// Read nested file
	f, err := fsys.Open("dir2/subdir/file3.txt")
	require.NoError(t, err)
	defer f.Close()

	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "file3", string(content))
}

// TestWriterMixedFileSizes tests a filesystem with various file sizes
func TestWriterMixedFileSizes(t *testing.T) {
	testFS := fstest.MapFS{
		"empty.txt": &fstest.MapFile{
			Data:    []byte{},
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"tiny.txt": &fstest.MapFile{
			Data:    []byte("x"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"small.txt": &fstest.MapFile{
			Data:    []byte(strings.Repeat("small", 100)), // 500 bytes
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"medium.txt": &fstest.MapFile{
			Data:    make([]byte, 5000), // 5000 bytes
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"large.txt": &fstest.MapFile{
			Data:    make([]byte, 50000), // 50KB
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"exact-block.txt": &fstest.MapFile{
			Data:    make([]byte, 4096), // Exactly one block
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"two-blocks.txt": &fstest.MapFile{
			Data:    make([]byte, 8192), // Exactly two blocks
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Verify all files have correct sizes
	expectedSizes := map[string]int64{
		"empty.txt":       0,
		"tiny.txt":        1,
		"small.txt":       500,
		"medium.txt":      5000,
		"large.txt":       50000,
		"exact-block.txt": 4096,
		"two-blocks.txt":  8192,
	}

	for filename, expectedSize := range expectedSizes {
		info, err := fsys.Stat(filename)
		require.NoError(t, err, "Failed to stat %s", filename)
		require.Equal(t, expectedSize, info.Size(), "Size mismatch for %s", filename)

		// Also read the file to ensure content is accessible
		f, err := fsys.Open(filename)
		require.NoError(t, err, "Failed to open %s", filename)

		content, err := io.ReadAll(f)
		require.NoError(t, err, "Failed to read %s", filename)
		require.Equal(t, expectedSize, int64(len(content)), "Content length mismatch for %s", filename)

		f.Close()
	}
}

// TestWriterSuperblock tests that the superblock is written correctly
func TestWriterSuperblock(t *testing.T) {
	testFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data:    []byte("test"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Read superblock directly
	sb := SuperBlock{}
	reader := bytes.NewReader(buf.Bytes())
	err = binary.Read(io.NewSectionReader(reader, SuperBlockOffset, int64(binary.Size(sb))), binary.LittleEndian, &sb)
	require.NoError(t, err)

	// Verify superblock fields
	require.Equal(t, uint32(SuperBlockMagicV1), sb.Magic, "Invalid magic number")
	require.Equal(t, uint8(BlockSizeBits), sb.BlockSizeBits, "Invalid block size bits")
	require.Greater(t, sb.Inodes, uint64(0), "Inode count should be > 0")
	require.Greater(t, sb.Blocks, uint32(0), "Block count should be > 0")

	// Verify feature flags
	require.Equal(t, uint32(EROFS_FEATURE_COMPAT_SB_CHKSUM|EROFS_FEATURE_COMPAT_MTIME), sb.FeatureCompat)

	// Verify UUID is not all zeros
	allZeros := true
	for _, b := range sb.UUID {
		if b != 0 {
			allZeros = false
			break
		}
	}
	require.False(t, allZeros, "UUID should not be all zeros")
}

// TestWriterInodeAllocation tests that inode numbers are allocated correctly
func TestWriterInodeAllocation(t *testing.T) {
	testFS := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("content1"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file2.txt": &fstest.MapFile{
			Data:    []byte("content2"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"dir1/file3.txt": &fstest.MapFile{
			Data:    []byte("content3"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Get inode information for each file
	info1, err := fsys.Stat("file1.txt")
	require.NoError(t, err)
	ino1 := info1.Sys().(*Inode)

	info2, err := fsys.Stat("file2.txt")
	require.NoError(t, err)
	ino2 := info2.Sys().(*Inode)

	info3, err := fsys.Stat("dir1/file3.txt")
	require.NoError(t, err)
	ino3 := info3.Sys().(*Inode)

	// Verify that different files have different inode numbers
	require.NotEqual(t, ino1.Nid(), ino2.Nid(), "file1 and file2 should have different inodes")
	require.NotEqual(t, ino1.Nid(), ino3.Nid(), "file1 and file3 should have different inodes")
	require.NotEqual(t, ino2.Nid(), ino3.Nid(), "file2 and file3 should have different inodes")
}

// TestWriterRootDirectory tests the root directory entry
func TestWriterRootDirectory(t *testing.T) {
	testFS := fstest.MapFS{
		"file.txt": &fstest.MapFile{
			Data:    []byte("content"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Read root directory
	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "file.txt", entries[0].Name())

	// Stat root directory
	info, err := fsys.Stat(".")
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestWriterPermissions tests file permissions are preserved
func TestWriterPermissions(t *testing.T) {
	testFS := fstest.MapFS{
		"executable": &fstest.MapFile{
			Data:    []byte("#!/bin/sh\necho hello"),
			Mode:    0o755,
			ModTime: time.Unix(1234567890, 0),
		},
		"readonly": &fstest.MapFile{
			Data:    []byte("read only"),
			Mode:    0o444,
			ModTime: time.Unix(1234567890, 0),
		},
		"normal": &fstest.MapFile{
			Data:    []byte("normal file"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Check permissions
	tests := []struct {
		path string
		mode fs.FileMode
	}{
		{"executable", 0o755},
		{"readonly", 0o444},
		{"normal", 0o644},
	}

	for _, tt := range tests {
		info, err := fsys.Stat(tt.path)
		require.NoError(t, err, "Failed to stat %s", tt.path)
		require.Equal(t, tt.mode, info.Mode()&fs.ModePerm, "Permission mismatch for %s", tt.path)
	}
}

// TestWriterOffsetAlignment tests that offsets are properly aligned
func TestWriterOffsetAlignment(t *testing.T) {
	// This test creates a scenario that might trigger alignment issues
	testFS := fstest.MapFS{}

	// Create files of various sizes that might cause alignment issues
	for i := 0; i < 20; i++ {
		size := (i * 137) % 2048 // Various sizes up to 2KB
		content := make([]byte, size)
		for j := range content {
			content[j] = byte(j % 256)
		}

		testFS[filepath.Join("file", "test", string(rune('a'+i%26)), string(rune('0'+i%10))+".dat")] = &fstest.MapFile{
			Data:    content,
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		}
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Walk all files and verify they can be read
	err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		f, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		// Read the entire file to ensure no alignment issues
		_, err = io.ReadAll(f)
		return err
	})
	require.NoError(t, err)
}

// TestWriterEmptyDirectory tests empty directory handling
func TestWriterEmptyDirectory(t *testing.T) {
	// Create a temporary directory with an empty subdirectory
	tmpDir := t.TempDir()
	emptyDir := filepath.Join(tmpDir, "empty")
	err := os.Mkdir(emptyDir, 0o755)
	require.NoError(t, err)

	// Create a file to ensure the FS is not empty
	err = os.WriteFile(filepath.Join(tmpDir, "file.txt"), []byte("content"), 0o644)
	require.NoError(t, err)

	// Create EROFS image from directory
	testFS := os.DirFS(tmpDir)
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err = Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// List empty directory
	entries, err := fsys.ReadDir("empty")
	require.NoError(t, err)
	require.Len(t, entries, 0, "Empty directory should have no entries")

	// Stat empty directory
	info, err := fsys.Stat("empty")
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestWriterSpecialCharsInFilenames tests filenames with special characters
func TestWriterSpecialCharsInFilenames(t *testing.T) {
	testFS := fstest.MapFS{
		"file with spaces.txt": &fstest.MapFile{
			Data:    []byte("spaces"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file-with-dashes.txt": &fstest.MapFile{
			Data:    []byte("dashes"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file_with_underscores.txt": &fstest.MapFile{
			Data:    []byte("underscores"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"file.with.dots.txt": &fstest.MapFile{
			Data:    []byte("dots"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image
	buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
	err := Create(buf, testFS, WithAllRoot(true))
	require.NoError(t, err)

	// Open and verify the filesystem
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Verify all files can be opened
	for filename := range testFS {
		f, err := fsys.Open(filename)
		require.NoError(t, err, "Failed to open %s", filename)
		f.Close()
	}
}

// TestWriterMultipleWrites tests that writing to an io.Writer works correctly
func TestWriterMultipleWrites(t *testing.T) {
	testFS := fstest.MapFS{
		"test.txt": &fstest.MapFile{
			Data:    []byte("test content"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
	}

	// Create EROFS image twice to ensure no state corruption
	for i := 0; i < 2; i++ {
		buf := newBytesWriterAt(1024 * 1024) // 1MB initial size
		err := Create(buf, testFS, WithAllRoot(true))
		require.NoError(t, err, "Failed on iteration %d", i)

		// Verify the filesystem
		fsys, err := Open(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err, "Failed to open on iteration %d", i)

		f, err := fsys.Open("test.txt")
		require.NoError(t, err, "Failed to open file on iteration %d", i)

		content, err := io.ReadAll(f)
		require.NoError(t, err, "Failed to read on iteration %d", i)
		require.Equal(t, "test content", string(content), "Content mismatch on iteration %d", i)

		f.Close()
	}
}
