// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH and The Unikraft CLI Authors.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package erofs_test

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs/erofs"
)

// imageBytes builds an EROFS image from src and returns its raw bytes as a
// ReaderAt-capable reader.
func imageBytes(t *testing.T, src fs.FS) *bytes.Reader {
	t.Helper()
	imgFile, err := os.CreateTemp(t.TempDir(), "erofs-extract-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { imgFile.Close() })

	require.NoError(t, erofs.Create(imgFile, src))

	_, err = imgFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	data, err := io.ReadAll(imgFile)
	require.NoError(t, err)
	return bytes.NewReader(data)
}

func TestUnpack(t *testing.T) {
	modTime := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755, ModTime: modTime},
		{Typeflag: tar.TypeReg, Name: "dir/hello.txt", Mode: 0o644, ModTime: modTime},
		{Typeflag: tar.TypeDir, Name: "dir/sub/", Mode: 0o755, ModTime: modTime},
		{Typeflag: tar.TypeReg, Name: "dir/sub/nested.txt", Mode: 0o600, ModTime: modTime},
		{Typeflag: tar.TypeSymlink, Name: "dir/link.txt", Linkname: "sub/nested.txt", ModTime: modTime},
	}
	fileData := map[string][]byte{
		"dir/hello.txt":      []byte("hello world\n"),
		"dir/sub/nested.txt": []byte("nested\n"),
	}
	tfs := createTarFS(t, headers, fileData)
	reader := imageBytes(t, tfs)

	dest := t.TempDir()
	require.NoError(t, erofs.Unpack(reader, dest))

	// Regular file content round-trips intact.
	got, err := os.ReadFile(filepath.Join(dest, "dir/hello.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello world\n", string(got))

	gotNested, err := os.ReadFile(filepath.Join(dest, "dir/sub/nested.txt"))
	require.NoError(t, err)
	require.Equal(t, "nested\n", string(gotNested))

	// Nested directory was created.
	fi, err := os.Stat(filepath.Join(dest, "dir/sub"))
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	// File mode (perm bits) is preserved.
	require.Equal(t, os.FileMode(0o755), fi.Mode().Perm())
	fi, err = os.Stat(filepath.Join(dest, "dir/sub/nested.txt"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	// mtime is preserved on regular files.
	require.Equal(t, modTime.Unix(), fi.ModTime().Unix())

	// Symlink target is preserved.
	gotLink, err := os.Readlink(filepath.Join(dest, "dir/link.txt"))
	require.NoError(t, err)
	require.Equal(t, "sub/nested.txt", gotLink)
}

func TestUnpack_empty(t *testing.T) {
	// A trivial image with only the root directory.
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "./", Mode: 0o755},
	}
	tfs := createTarFS(t, headers, nil)
	reader := imageBytes(t, tfs)

	dest := t.TempDir()
	require.NoError(t, erofs.Unpack(reader, dest))

	// dest exists and is empty.
	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestUnpack_setuidSticky verifies that special mode bits (setuid/setgid/
// sticky) survive extraction, not just the permission bits.
func TestUnpack_setuidSticky(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: setuid bit enforcement differs")
	}
	headers := []tar.Header{
		{Typeflag: tar.TypeReg, Name: "suid", Mode: 0o4755},
		{Typeflag: tar.TypeReg, Name: "sgid", Mode: 0o2755},
		{Typeflag: tar.TypeDir, Name: "sticky/", Mode: 0o1755},
	}
	fileData := map[string][]byte{
		"suid": []byte("s"),
		"sgid": []byte("g"),
	}
	tfs := createTarFS(t, headers, fileData)
	reader := imageBytes(t, tfs)

	dest := t.TempDir()
	require.NoError(t, erofs.Unpack(reader, dest))

	fi, err := os.Stat(filepath.Join(dest, "suid"))
	require.NoError(t, err)
	require.NotZero(t, fi.Mode()&os.ModeSetuid, "setuid bit not preserved")

	fi, err = os.Stat(filepath.Join(dest, "sgid"))
	require.NoError(t, err)
	require.NotZero(t, fi.Mode()&os.ModeSetgid, "setgid bit not preserved")

	fi, err = os.Stat(filepath.Join(dest, "sticky"))
	require.NoError(t, err)
	require.NotZero(t, fi.Mode()&os.ModeSticky, "sticky bit not preserved")
}

// TestUnpack_symlinkTraversalEscape verifies that Unpack refuses to write an
// entry through a pre-existing symlinked directory component in dest, which
// would otherwise escape dest on disk.
func TestUnpack_symlinkTraversalEscape(t *testing.T) {
	target := t.TempDir() // outside dest; must stay untouched
	targetFile := filepath.Join(target, "smuggled")

	// Image contains only a regular file at "out/smuggled".
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "out/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "out/smuggled", Mode: 0o644},
	}
	fileData := map[string][]byte{
		"out/smuggled": []byte("escaped"),
	}
	tfs := createTarFS(t, headers, fileData)
	reader := imageBytes(t, tfs)

	dest := t.TempDir()
	// Replace the "out" directory with a symlink pointing outside dest.
	require.NoError(t, os.RemoveAll(filepath.Join(dest, "out")))
	require.NoError(t, os.Symlink(target, filepath.Join(dest, "out")))

	err := erofs.Unpack(reader, dest)
	require.Error(t, err, "Unpack should reject symlink-traversal escape")

	_, statErr := os.Stat(targetFile)
	require.True(t, os.IsNotExist(statErr), "file was written outside dest via symlink traversal")
}
