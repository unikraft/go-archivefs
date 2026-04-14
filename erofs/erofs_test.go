// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package erofs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/rogpeppe/go-internal/dirhash"
	"github.com/unikraft/go-archivefs/erofs"

	"github.com/stretchr/testify/require"
)

func TestEROFS(t *testing.T) {
	f, err := os.Open("testdata/toybox.img")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, f.Close())
	})

	fsys, err := erofs.Open(f)
	require.NoError(t, err)

	t.Run("Open", func(t *testing.T) {
		t.Run("File", func(t *testing.T) {
			f, err := fsys.Open("/usr/bin/toybox")
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, f.Close())
			})

			info, err := f.Stat()
			require.NoError(t, err)

			require.Equal(t, "toybox", info.Name())
			require.Equal(t, 849544, int(info.Size()))
			require.Equal(t, os.FileMode(0o555), info.Mode()&fs.ModePerm)
			require.False(t, info.IsDir())

			h := sha256.New()

			n, err := io.Copy(h, f)
			require.NoError(t, err)

			require.Equal(t, int64(849544), n)

			require.Equal(t, "31aa01d6d46f63edcadc00fd5c40f3474f0df6c22a39ed0c5751ba3efa2855ac", hex.EncodeToString(h.Sum(nil)))
		})

		t.Run("Symlink", func(t *testing.T) {
			f, err := fsys.Open("bin/sh")
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, f.Close())
			})

			info, err := f.Stat()
			require.NoError(t, err)

			require.Equal(t, "toybox", info.Name())
			require.Equal(t, os.FileMode(0o555), info.Mode()&fs.ModePerm)
			require.False(t, info.IsDir())

			h := sha256.New()

			n, err := io.Copy(h, f)
			require.NoError(t, err)

			require.Equal(t, int64(849544), n)

			require.Equal(t, "31aa01d6d46f63edcadc00fd5c40f3474f0df6c22a39ed0c5751ba3efa2855ac", hex.EncodeToString(h.Sum(nil)))
		})
	})

	t.Run("ReadDir", func(t *testing.T) {
		entries, err := fsys.ReadDir("/etc")
		require.NoError(t, err)

		require.Len(t, entries, 5)

		require.Equal(t, "group", entries[0].Name())
		require.False(t, entries[0].IsDir())
		require.Equal(t, 0o644, int(entries[0].Type()))

		require.Equal(t, "os-release", entries[1].Name())
		require.False(t, entries[1].IsDir())
		require.Equal(t, 0o644, int(entries[1].Type()))

		require.Equal(t, "passwd", entries[2].Name())
		require.False(t, entries[2].IsDir())
		require.Equal(t, 0o644, int(entries[2].Type()))

		require.Equal(t, "rc", entries[3].Name())
		require.True(t, entries[3].IsDir())
		require.True(t, entries[3].Type()&fs.ModeDir > 0)

		require.Equal(t, "resolv.conf", entries[4].Name())
		require.False(t, entries[4].IsDir())
		require.Equal(t, 0o644, int(entries[4].Type()))
	})

	t.Run("Stat", func(t *testing.T) {
		t.Run("File", func(t *testing.T) {
			info, err := fsys.Stat("/usr/bin/toybox")
			require.NoError(t, err)

			require.Equal(t, "toybox", info.Name())
			require.Equal(t, 849544, int(info.Size()))
			require.Equal(t, os.FileMode(0o555), info.Mode()&fs.ModePerm)
			require.False(t, info.IsDir())

			ino, ok := info.Sys().(*erofs.Inode)
			require.True(t, ok)

			require.Equal(t, uint64(0x327), ino.Nid())
			require.Zero(t, ino.UID())
			require.Zero(t, ino.GID())
		})

		t.Run("Dir", func(t *testing.T) {
			info, err := fsys.Stat("usr")
			require.NoError(t, err)

			require.Equal(t, "usr", info.Name())
			require.Equal(t, os.FileMode(0o755), info.Mode()&fs.ModePerm)
			require.True(t, info.IsDir())

			ino, ok := info.Sys().(*erofs.Inode)
			require.True(t, ok)

			require.Equal(t, uint64(0x9c), ino.Nid())
			require.Zero(t, ino.UID())
			require.Zero(t, ino.GID())
		})

		t.Run("Symlink", func(t *testing.T) {
			info, err := fsys.Stat("bin/sh")
			require.NoError(t, err)

			require.Equal(t, "toybox", info.Name())
			require.Equal(t, os.FileMode(0o555), info.Mode()&fs.ModePerm)
			require.False(t, info.IsDir())

			ino, ok := info.Sys().(*erofs.Inode)
			require.True(t, ok)

			require.Equal(t, uint64(0x327), ino.Nid())
			require.Zero(t, ino.UID())
			require.Zero(t, ino.GID())
		})
	})

	t.Run("Readlink", func(t *testing.T) {
		target, err := fsys.ReadLink("bin")
		require.NoError(t, err)

		require.Equal(t, "usr/bin", target)
	})

	t.Run("StatLink", func(t *testing.T) {
		info, err := fsys.StatLink("bin")
		require.NoError(t, err)

		require.Equal(t, "bin", info.Name())
		require.Equal(t, os.FileMode(0o777), info.Mode()&fs.ModePerm)
		require.False(t, info.IsDir())

		ino, ok := info.Sys().(*erofs.Inode)
		require.True(t, ok)

		require.Equal(t, uint64(0x2f), ino.Nid())
		require.Zero(t, ino.UID())
		require.Zero(t, ino.GID())
	})

	t.Run("WalkDir", func(t *testing.T) {
		var paths []string
		err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			paths = append(paths, path)

			return nil
		})
		require.NoError(t, err)

		require.Len(t, paths, 266)

		require.Equal(t, []string{
			".",
			"bin",
			"dev",
			"etc",
			"etc/group",
		}, paths[:5])
	})

	t.Run("DirHash", func(t *testing.T) {
		var files []string
		err = fs.WalkDir(fsys, ".", func(file string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
				return nil
			}

			files = append(files, filepath.ToSlash(file))
			return nil
		})
		require.NoError(t, err)

		h, err := dirhash.Hash1(files, func(name string) (io.ReadCloser, error) {
			return fsys.Open(name)
		})
		require.NoError(t, err)

		require.Equal(t, "h1:adgxkqVceeKMyJdMZMvcUIbg94TthnXUmOeufCPuzQI=", h)
	})
}

func TestEROFSCreate(t *testing.T) {
	srcFile, err := os.Open("testdata/toybox.img")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, srcFile.Close())
	})

	srcFS, err := erofs.Open(srcFile)
	require.NoError(t, err)

	dstFile, err := os.OpenFile(filepath.Join(t.TempDir()+"/toybox.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dstFile.Close())
	})

	require.NoError(t, erofs.Create(dstFile, srcFS))

	dstFS, err := erofs.Open(dstFile)
	require.NoError(t, err)

	var files []string
	err = fs.WalkDir(dstFS, ".", func(file string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		files = append(files, filepath.ToSlash(file))
		return nil
	})
	require.NoError(t, err)

	h, err := dirhash.Hash1(files, func(name string) (io.ReadCloser, error) {
		return srcFS.Open(name)
	})
	require.NoError(t, err)

	require.Equal(t, "h1:adgxkqVceeKMyJdMZMvcUIbg94TthnXUmOeufCPuzQI=", h)
}

// TestEROFSHardlinkDeduplication tests that hardlinks are properly deduplicated
// in the EROFS filesystem, ensuring that the same file data is only stored once.
// This is a regression test for a bug where hardlinks would cause the same data
// to be written multiple times, bloating the filesystem size.
func TestEROFSHardlinkDeduplication(t *testing.T) {
	// Create a temporary directory with hardlinks
	srcDir := t.TempDir()

	// Create a file with known content (1MB to make the size difference obvious)
	largeContent := make([]byte, 1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	originalFile := filepath.Join(srcDir, "original")
	require.NoError(t, os.WriteFile(originalFile, largeContent, 0o644))

	// Create multiple hardlinks to the same file
	numHardlinks := 10
	for i := range numHardlinks {
		linkPath := filepath.Join(srcDir, filepath.Join("link", string(rune('a'+i))))
		require.NoError(t, os.MkdirAll(filepath.Dir(linkPath), 0o755))
		require.NoError(t, os.Link(originalFile, linkPath))
	}

	// Verify hardlinks were created correctly
	originalStat, err := os.Stat(originalFile)
	require.NoError(t, err)

	for i := range numHardlinks {
		linkPath := filepath.Join(srcDir, filepath.Join("link", string(rune('a'+i))))
		linkStat, err := os.Stat(linkPath)
		require.NoError(t, err)

		// Verify they have the same inode (Unix-specific)
		require.Equal(t, originalStat.Sys(), linkStat.Sys(), "hardlinks should share the same inode")
	}

	// Create EROFS filesystem from the directory
	dstFile, err := os.OpenFile(filepath.Join(t.TempDir(), "hardlinks.erofs"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dstFile.Close())
	})

	require.NoError(t, erofs.Create(dstFile, os.DirFS(srcDir), erofs.WithAllRoot(true)))

	// Get the size of the EROFS filesystem
	stat, err := dstFile.Stat()
	require.NoError(t, err)
	erofsSize := stat.Size()

	// The EROFS filesystem should be much smaller than if each hardlink stored a copy
	// Expected size: ~1MB (for one copy of the data) + metadata overhead
	// Without deduplication: ~11MB (11 copies of the 1MB file) + metadata

	// Allow some overhead for metadata, but the size should be closer to 1MB than 11MB
	// We'll use 2MB as the upper bound to allow for metadata and block alignment
	maxExpectedSize := int64(2 * 1024 * 1024)

	// Without deduplication, it would be at least 10MB
	minBuggySize := int64(10 * 1024 * 1024)

	t.Logf("EROFS filesystem size: %d bytes (%.2f MB)", erofsSize, float64(erofsSize)/(1024*1024))

	require.Less(t, erofsSize, maxExpectedSize,
		"EROFS size should be close to single file size, not total of all hardlinks")
	require.Greater(t, erofsSize, int64(0),
		"EROFS size should be non-zero")

	// If this fails, the hardlink deduplication is not working
	if erofsSize > minBuggySize {
		t.Fatalf("EROFS size (%d bytes) suggests hardlinks are not deduplicated (would be >%d bytes without fix)",
			erofsSize, minBuggySize)
	}

	// Verify the filesystem is valid and readable
	fsys, err := erofs.Open(dstFile)
	require.NoError(t, err)

	// Verify we can read the original file
	f, err := fsys.Open("original")
	require.NoError(t, err)
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, largeContent, content, "original file content should match")

	// Verify all hardlinks are readable and have the same content
	for i := range numHardlinks {
		linkPath := filepath.Join("link", string(rune('a'+i)))
		f, err := fsys.Open(linkPath)
		require.NoError(t, err)
		content, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		require.Equal(t, largeContent, content, "hardlink %s content should match original", linkPath)
	}
}
