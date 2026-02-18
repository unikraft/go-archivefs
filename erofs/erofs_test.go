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
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/unikraft/go-archivefs/erofs"
	"github.com/unikraft/go-archivefs/memfs"
	"github.com/unikraft/go-archivefs/tarfs"
	"github.com/rogpeppe/go-internal/dirhash"

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

func TestEROFSFilenamesSortingBeforeDot(t *testing.T) {
	// EROFS requires directory entries in strict alphabetical order for
	// binary search lookup. The '.' and '..' entries must be sorted among
	// all other entries, not hardcoded at the front. All printable ASCII
	// characters before '.' (0x2E) would be affected:
	//   ! " # $ % & ' ( ) * + , -
	// (0x21 through 0x2D)
	prefixes := []string{"!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-"}

	srcFS := memfs.New()
	require.NoError(t, srcFS.MkdirAll("dir", 0o755))

	for _, p := range prefixes {
		name := p + "file.txt"
		require.NoError(t, srcFS.WriteFile(filepath.Join("dir", name), []byte("content-"+p), 0o644))
	}
	// Also add a file that sorts after '.' for good measure.
	require.NoError(t, srcFS.WriteFile("dir/normal.txt", []byte("normal"), 0o644))

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "sort.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, imgFile.Close())
	})

	require.NoError(t, erofs.Create(imgFile, srcFS))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	// Stat (which uses Lookup/binary search) must find every file.
	for _, p := range prefixes {
		name := p + "file.txt"
		t.Run(name, func(t *testing.T) {
			info, err := fsys.Stat(filepath.Join("dir", name))
			require.NoError(t, err)
			require.Equal(t, name, info.Name())

			f, err := fsys.Open(filepath.Join("dir", name))
			require.NoError(t, err)
			data, err := io.ReadAll(f)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			require.Equal(t, "content-"+p, string(data))
		})
	}

	// Normal file should still work too.
	info, err := fsys.Stat("dir/normal.txt")
	require.NoError(t, err)
	require.Equal(t, "normal.txt", info.Name())
}

func TestEROFSFilenamesSortingBeforeDotRoot(t *testing.T) {
	// Same as above but with files at the root level, where only '.' exists
	// (no '..' entry). Verifies the root directory case is also sorted correctly.
	prefixes := []string{"!", "#", "-"}

	srcFS := memfs.New()
	for _, p := range prefixes {
		name := p + "root.txt"
		require.NoError(t, srcFS.WriteFile(name, []byte("root-"+p), 0o644))
	}
	require.NoError(t, srcFS.WriteFile("zebra.txt", []byte("zebra"), 0o644))

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "rootsort.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, imgFile.Close())
	})

	require.NoError(t, erofs.Create(imgFile, srcFS))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	for _, p := range prefixes {
		name := p + "root.txt"
		t.Run(name, func(t *testing.T) {
			info, err := fsys.Stat(name)
			require.NoError(t, err)
			require.Equal(t, name, info.Name())

			f, err := fsys.Open(name)
			require.NoError(t, err)
			data, err := io.ReadAll(f)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			require.Equal(t, "root-"+p, string(data))
		})
	}

	info, err := fsys.Stat("zebra.txt")
	require.NoError(t, err)
	require.Equal(t, "zebra.txt", info.Name())
}

func TestEROFSSymlinkCycleDetection(t *testing.T) {
	// Create a tar archive with circular symlinks: a -> b, b -> a.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "a",
		Typeflag: tar.TypeSymlink,
		Linkname: "b",
	}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "b",
		Typeflag: tar.TypeSymlink,
		Linkname: "a",
	}))
	require.NoError(t, tw.Close())

	// Open as tarfs (which implements ReadLinkFS).
	srcFS, err := tarfs.Open(bytes.NewReader(tarBuf.Bytes()))
	require.NoError(t, err)

	// Create an EROFS image containing the cycle.
	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "cycle.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	// Resolving "a" follows a -> b -> a -> b -> ... and must return an
	// error rather than overflowing the stack.
	_, err = fsys.Stat("a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many levels of symbolic links")

	_, err = fsys.Open("b")
	require.Error(t, err)
}

func TestEROFSCorruptedInodeNoPanic(t *testing.T) {
	// Create a valid image with a file.
	srcFS := memfs.New()
	require.NoError(t, srcFS.WriteFile("file.txt", []byte("data"), 0o644))

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "corrupt.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	// Read the full image into memory.
	stat, err := imgFile.Stat()
	require.NoError(t, err)
	data := make([]byte, stat.Size())
	_, err = imgFile.ReadAt(data, 0)
	require.NoError(t, err)

	// Truncate the image so that the superblock and root inode are intact
	// but child inodes are cut off. Metadata starts at block 1 (offset 4096).
	// Keep 96 bytes of metadata: root inode (32 bytes) + inline dir data.
	truncated := data[:4096+96]

	// Previously getInode would panic on error. Verify that operations on a
	// corrupted image return errors gracefully.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic on corrupted image: %v", r)
		}
	}()

	fsys, err := erofs.Open(bytes.NewReader(truncated))
	if err != nil {
		return // Open failing is acceptable
	}

	// Stat on the file should hit the corrupted/missing inode and return
	// an error rather than panicking.
	_, _ = fsys.Stat("file.txt")
	_, _ = fsys.Open("file.txt")
	_, _ = fsys.ReadDir(".")
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
