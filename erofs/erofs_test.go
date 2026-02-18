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
	"sync"
	"testing"
	"time"

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
			f, err := fsys.Open("usr/bin/toybox")
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
		entries, err := fsys.ReadDir("etc")
		require.NoError(t, err)

		require.Len(t, entries, 5)

		require.Equal(t, "group", entries[0].Name())
		require.False(t, entries[0].IsDir())
		require.Equal(t, fs.FileMode(0), entries[0].Type())

		require.Equal(t, "os-release", entries[1].Name())
		require.False(t, entries[1].IsDir())
		require.Equal(t, fs.FileMode(0), entries[1].Type())

		require.Equal(t, "passwd", entries[2].Name())
		require.False(t, entries[2].IsDir())
		require.Equal(t, fs.FileMode(0), entries[2].Type())

		require.Equal(t, "rc", entries[3].Name())
		require.True(t, entries[3].IsDir())
		require.Equal(t, fs.ModeDir, entries[3].Type())

		require.Equal(t, "resolv.conf", entries[4].Name())
		require.False(t, entries[4].IsDir())
		require.Equal(t, fs.FileMode(0), entries[4].Type())
	})

	t.Run("Stat", func(t *testing.T) {
		t.Run("File", func(t *testing.T) {
			info, err := fsys.Stat("usr/bin/toybox")
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

func TestEROFSStatLink(t *testing.T) {
	// Build a tar with a symlink and its target.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "target.txt",
		Size:     5,
		Mode:     0o644,
	}))
	_, err := tw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeSymlink,
		Name:     "link",
		Linkname: "target.txt",
		Mode:     0o777,
	}))
	require.NoError(t, tw.Close())

	srcFS, err := tarfs.Open(bytes.NewReader(tarBuf.Bytes()))
	require.NoError(t, err)

	imgFile, err := os.CreateTemp(t.TempDir(), "statlink-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { imgFile.Close() })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	// Stat follows the symlink, returning the target's info.
	statFI, err := fsys.Stat("link")
	require.NoError(t, err)
	require.Equal(t, "target.txt", statFI.Name())
	require.False(t, statFI.Mode()&fs.ModeSymlink != 0, "Stat should follow symlinks")

	// StatLink does not follow the symlink.
	linkFI, err := fsys.StatLink("link")
	require.NoError(t, err)
	require.Equal(t, "link", linkFI.Name())
	require.True(t, linkFI.Mode()&fs.ModeSymlink != 0, "StatLink should not follow symlinks")

	// Both should return a valid *erofs.Inode via Sys().
	require.NotNil(t, statFI.Sys())
	require.NotNil(t, linkFI.Sys())
	_, ok := statFI.Sys().(*erofs.Inode)
	require.True(t, ok, "Stat Sys() should return *erofs.Inode")
	_, ok = linkFI.Sys().(*erofs.Inode)
	require.True(t, ok, "StatLink Sys() should return *erofs.Inode")
}

func TestEROFSSuperBlockChecksum(t *testing.T) {
	// Create a valid image.
	srcFS := memfs.New()
	require.NoError(t, srcFS.WriteFile("hello.txt", []byte("hello"), 0o644))

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "checksum.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	// Valid image should open without error.
	_, err = erofs.Open(imgFile)
	require.NoError(t, err)

	// Corrupt a byte in the superblock (the Inodes field at offset 1024+16).
	stat, err := imgFile.Stat()
	require.NoError(t, err)
	data := make([]byte, stat.Size())
	_, err = imgFile.ReadAt(data, 0)
	require.NoError(t, err)

	data[1024+16] ^= 0xFF // flip a byte in the superblock body

	// Opening the corrupted image should fail checksum verification.
	_, err = erofs.Open(bytes.NewReader(data))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid checksum")
}

func TestEROFSRootNidSet(t *testing.T) {
	// Verify the writer explicitly sets RootNid in the superblock.
	srcFS := memfs.New()
	require.NoError(t, srcFS.MkdirAll("subdir", 0o755))
	require.NoError(t, srcFS.WriteFile("subdir/file.txt", []byte("data"), 0o644))

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "rootnid.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	img, err := erofs.OpenImage(imgFile)
	require.NoError(t, err)

	// The root inode must be reachable via the superblock's RootNid.
	rootIno, err := img.Inode(img.RootNid())
	require.NoError(t, err)
	require.True(t, rootIno.IsDir(), "root inode must be a directory")
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

func TestEROFSModTimeNanoseconds(t *testing.T) {
	// Extended inodes store nanosecond-precision modification times.
	// Verify that ModTime() includes the nanosecond component.
	modTime := time.Date(2024, 6, 15, 12, 30, 45, 123456789, time.UTC)

	// Use PAX format explicitly to preserve nanosecond precision in tar.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	content := []byte("hello")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Format:   tar.FormatPAX,
		Name:     "file.txt",
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     int64(len(content)),
		ModTime:  modTime,
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	srcFS, err := tarfs.Open(bytes.NewReader(tarBuf.Bytes()))
	require.NoError(t, err)

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "nsec.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, srcFS))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	info, err := fsys.Stat("file.txt")
	require.NoError(t, err)

	// The nanosecond component should be preserved.
	require.Equal(t, modTime.Unix(), info.ModTime().Unix())
	require.Equal(t, modTime.Nanosecond(), info.ModTime().Nanosecond())
}

func TestEROFSZeroModTimeCompactInode(t *testing.T) {
	// Files with zero modtime should use compact inodes (32 bytes) rather
	// than extended (64 bytes). The Go zero time time.Time{} has a nil
	// Location, but some FS implementations return the equivalent zero
	// time with a UTC Location (e.g. time.Time{}.UTC()). These are both
	// IsZero() but not == time.Time{}. Previously the writer used == which
	// would miss the UTC variant and produce an unnecessarily large inode.
	srcFS := memfs.New()
	require.NoError(t, srcFS.WriteFile("file.txt", []byte("hello"), 0o644))

	// Wrap to override ModTime with the UTC-located zero time.
	wrapped := &zeroUTCModTimeFS{FS: srcFS}

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "compact.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, wrapped))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	info, err := fsys.Stat("file.txt")
	require.NoError(t, err)

	ino := info.Sys().(*erofs.Inode)

	// Layout 0 = compact (32 bytes), layout 1 = extended (64 bytes).
	require.Equal(t, uint16(0), ino.Layout(), "zero-modtime file should use compact inode layout")
}

// zeroUTCModTimeFS wraps an fs.FS and overrides ModTime to return the zero
// time with a non-nil UTC Location, which is IsZero() but != time.Time{}.
type zeroUTCModTimeFS struct {
	fs.FS
}

func (z *zeroUTCModTimeFS) Open(name string) (fs.File, error) {
	f, err := z.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &zeroUTCModTimeFile{File: f}, nil
}

type zeroUTCModTimeFile struct {
	fs.File
}

func (f *zeroUTCModTimeFile) Stat() (fs.FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return &zeroUTCFileInfo{FileInfo: info}, nil
}

func (f *zeroUTCModTimeFile) ReadDir(n int) ([]fs.DirEntry, error) {
	rdf, ok := f.File.(fs.ReadDirFile)
	if !ok {
		return nil, fs.ErrInvalid
	}
	entries, err := rdf.ReadDir(n)
	if err != nil {
		return nil, err
	}
	wrapped := make([]fs.DirEntry, len(entries))
	for i, e := range entries {
		wrapped[i] = &zeroUTCDirEntry{DirEntry: e}
	}
	return wrapped, nil
}

type zeroUTCDirEntry struct {
	fs.DirEntry
}

func (e *zeroUTCDirEntry) Info() (fs.FileInfo, error) {
	info, err := e.DirEntry.Info()
	if err != nil {
		return nil, err
	}
	return &zeroUTCFileInfo{FileInfo: info}, nil
}

type zeroUTCFileInfo struct {
	fs.FileInfo
}

func (fi *zeroUTCFileInfo) ModTime() time.Time {
	return time.Time{}.UTC() // IsZero() == true, but != time.Time{}
}

// countingFS wraps an fs.FS and counts how many times each path is opened.
type countingFS struct {
	fs.FS
	mu     sync.Mutex
	counts map[string]int
}

func (c *countingFS) Open(name string) (fs.File, error) {
	c.mu.Lock()
	c.counts[name]++
	c.mu.Unlock()
	return c.FS.Open(name)
}

func TestEROFSFileOpenedOnce(t *testing.T) {
	// Verify that regular files are only opened once during Create,
	// not twice (firstPass used to open files just to get their size).
	srcFS := memfs.New()
	require.NoError(t, srcFS.MkdirAll("dir", 0o755))
	require.NoError(t, srcFS.WriteFile("dir/a.txt", []byte("aaa"), 0o644))
	require.NoError(t, srcFS.WriteFile("dir/b.txt", []byte("bbb"), 0o644))
	require.NoError(t, srcFS.WriteFile("big.txt", make([]byte, 8192), 0o644))

	cfs := &countingFS{FS: srcFS, counts: make(map[string]int)}

	imgFile, err := os.OpenFile(filepath.Join(t.TempDir(), "counting.img"), os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, imgFile.Close()) })

	require.NoError(t, erofs.Create(imgFile, cfs))

	// Each regular file should be opened exactly once (in the write phase).
	// Before the fix, firstPass would also open each file to get its size.
	for _, name := range []string{"dir/a.txt", "dir/b.txt", "big.txt"} {
		require.Equal(t, 1, cfs.counts[name], "file %q should be opened exactly once", name)
	}

	// Verify the image is still correct.
	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	data, err := fs.ReadFile(fsys, "dir/a.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("aaa"), data)

	data, err = fs.ReadFile(fsys, "big.txt")
	require.NoError(t, err)
	require.Len(t, data, 8192)
}

func TestEROFSDirEntryTypeReturnsBits(t *testing.T) {
	// Build a tar archive with a directory, regular file, and symlink.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir,
		Name:     "subdir/",
		Mode:     0o755,
	}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "subdir/file.txt",
		Size:     5,
		Mode:     0o644,
	}))
	_, err := tw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeSymlink,
		Name:     "link",
		Linkname: "subdir/file.txt",
		Mode:     0o777,
	}))
	require.NoError(t, tw.Close())

	tfs, err := tarfs.Open(bytes.NewReader(tarBuf.Bytes()))
	require.NoError(t, err)

	imgFile, err := os.CreateTemp(t.TempDir(), "type-test-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { imgFile.Close() })

	require.NoError(t, erofs.Create(imgFile, tfs))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	entries, err := fs.ReadDir(fsys, ".")
	require.NoError(t, err)

	typeMap := make(map[string]fs.FileMode)
	for _, e := range entries {
		typeMap[e.Name()] = e.Type()
	}

	// Type() must return only the type bits, not permission bits.
	require.Equal(t, fs.ModeDir, typeMap["subdir"], "directory should have ModeDir type")
	require.Equal(t, fs.ModeSymlink, typeMap["link"], "symlink should have ModeSymlink type")

	subEntries, err := fs.ReadDir(fsys, "subdir")
	require.NoError(t, err)
	require.Len(t, subEntries, 1)
	// Regular file: Type() should be exactly 0 (no type bits set).
	require.Equal(t, fs.FileMode(0), subEntries[0].Type(), "regular file should have zero type bits")
}

func TestEROFSRoundTripPreservesOwnership(t *testing.T) {
	// Create a tar with a specific UID/GID.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "owned.txt",
		Size:     3,
		Mode:     0o644,
		Uid:      1234,
		Gid:      5678,
	}))
	_, err := tw.Write([]byte("hey"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	srcFS, err := tarfs.Open(bytes.NewReader(tarBuf.Bytes()))
	require.NoError(t, err)

	// Write first EROFS image from tarfs.
	img1, err := os.CreateTemp(t.TempDir(), "owner1-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { img1.Close() })

	require.NoError(t, erofs.Create(img1, srcFS))

	fsys1, err := erofs.Open(img1)
	require.NoError(t, err)

	fi1, err := fsys1.Stat("owned.txt")
	require.NoError(t, err)
	ino1 := fi1.Sys().(*erofs.Inode)
	require.Equal(t, uint32(1234), ino1.UID())
	require.Equal(t, uint32(5678), ino1.GID())

	// Round-trip: write second EROFS image from the first.
	img2, err := os.CreateTemp(t.TempDir(), "owner2-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { img2.Close() })

	require.NoError(t, erofs.Create(img2, fsys1))

	fsys2, err := erofs.Open(img2)
	require.NoError(t, err)

	fi2, err := fsys2.Stat("owned.txt")
	require.NoError(t, err)
	ino2 := fi2.Sys().(*erofs.Inode)
	require.Equal(t, uint32(1234), ino2.UID(), "UID should survive EROFS round-trip")
	require.Equal(t, uint32(5678), ino2.GID(), "GID should survive EROFS round-trip")
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
