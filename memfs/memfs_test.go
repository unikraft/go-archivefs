// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions of this file are based on code originally from:
 * github.com/psanford/memfs
 *
 * Copyright (c) 2021 The memfs Authors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 * * Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package memfs_test

import (
	"fmt"
	"io"
	"io/fs"
	"slices"
	"testing"

	"github.com/unikraft/go-archivefs/memfs"

	"github.com/stretchr/testify/require"
)

func TestMemFS(t *testing.T) {
	rootFS := memfs.New()

	err := rootFS.MkdirAll("foo/bar", 0777)
	require.NoError(t, err)

	var gotPaths []string

	err = fs.WalkDir(rootFS, ".", func(path string, d fs.DirEntry, err error) error {
		gotPaths = append(gotPaths, path)
		if !d.IsDir() {
			return fmt.Errorf("%s is not a directory", path)
		}
		return nil
	})
	require.NoError(t, err)

	expectPaths := []string{
		".",
		"foo",
		"foo/bar",
	}
	require.Equal(t, expectPaths, gotPaths)

	err = rootFS.WriteFile("foo/baz/buz.txt", []byte("buz"), 0777)
	require.ErrorIs(t, err, fs.ErrNotExist)

	_, err = fs.ReadFile(rootFS, "foo/baz/buz.txt")
	require.ErrorIs(t, err, fs.ErrNotExist)

	body := []byte("baz")
	err = rootFS.WriteFile("foo/bar/baz.txt", body, 0777)
	require.NoError(t, err)

	gotBody, err := fs.ReadFile(rootFS, "foo/bar/baz.txt")
	require.NoError(t, err)

	require.Equal(t, body, gotBody)

	subFS, err := rootFS.Sub("foo/bar")
	require.NoError(t, err)

	gotSubBody, err := fs.ReadFile(subFS, "baz.txt")
	require.NoError(t, err)

	require.Equal(t, body, gotSubBody)

	body = []byte("top_level_file")
	err = rootFS.WriteFile("top_level_file.txt", body, 0777)
	require.NoError(t, err)

	gotBody, err = fs.ReadFile(rootFS, "top_level_file.txt")
	require.NoError(t, err)

	require.Equal(t, body, gotBody)
}

func TestMemFSRootStatName(t *testing.T) {
	rootFS := memfs.New()

	// Open "." and verify Stat returns name ".".
	f, err := rootFS.Open(".")
	require.NoError(t, err)

	fi, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, ".", fi.Name())
	require.True(t, fi.IsDir())
	require.NoError(t, f.Close())

	// Sub-FS root should also return "." for Stat name.
	require.NoError(t, rootFS.MkdirAll("subdir", 0o755))

	subFS, err := rootFS.Sub("subdir")
	require.NoError(t, err)

	sf, err := subFS.Open(".")
	require.NoError(t, err)

	sfi, err := sf.Stat()
	require.NoError(t, err)
	require.Equal(t, ".", sfi.Name(), "Sub-FS root Stat().Name() should be \".\"")
	require.True(t, sfi.IsDir())
	require.NoError(t, sf.Close())
}

func TestMemFSDirEntryType(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.MkdirAll("subdir", 0o755))
	require.NoError(t, rootFS.WriteFile("file.txt", []byte("hi"), 0o644))

	entries, err := fs.ReadDir(rootFS, ".")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	typeMap := make(map[string]fs.FileMode)
	for _, e := range entries {
		typeMap[e.Name()] = e.Type()
	}

	// Type() must return only type bits, not permission bits.
	require.Equal(t, fs.FileMode(0), typeMap["file.txt"], "regular file Type() should be 0")
	require.Equal(t, fs.ModeDir, typeMap["subdir"], "directory Type() should be exactly ModeDir")
}

func TestMemFSReadDirSorted(t *testing.T) {
	rootFS := memfs.New()

	// Create files in non-alphabetical order.
	for _, name := range []string{"cherry.txt", "apple.txt", "banana.txt"} {
		require.NoError(t, rootFS.WriteFile(name, []byte(name), 0o644))
	}
	require.NoError(t, rootFS.MkdirAll("delta", 0o755))

	// fs.ReadDir must return entries sorted by name.
	entries, err := fs.ReadDir(rootFS, ".")
	require.NoError(t, err)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}

	require.True(t, slices.IsSorted(names), "ReadDir must return sorted entries, got: %v", names)
	require.Equal(t, []string{"apple.txt", "banana.txt", "cherry.txt", "delta"}, names)
}

func TestMemFSReadDirPagination(t *testing.T) {
	rootFS := memfs.New()

	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		require.NoError(t, rootFS.WriteFile(name, []byte(name), 0o644))
	}

	f, err := rootFS.Open(".")
	require.NoError(t, err)

	dirFile, ok := f.(fs.ReadDirFile)
	require.True(t, ok)

	// Read first 2 entries.
	batch1, err := dirFile.ReadDir(2)
	require.NoError(t, err)
	require.Len(t, batch1, 2)
	require.Equal(t, "a.txt", batch1[0].Name())
	require.Equal(t, "b.txt", batch1[1].Name())

	// Read next 2 entries.
	batch2, err := dirFile.ReadDir(2)
	require.NoError(t, err)
	require.Len(t, batch2, 2)
	require.Equal(t, "c.txt", batch2[0].Name())
	require.Equal(t, "d.txt", batch2[1].Name())

	// Read remaining - should get 1 entry.
	batch3, err := dirFile.ReadDir(2)
	require.NoError(t, err)
	require.Len(t, batch3, 1)
	require.Equal(t, "e.txt", batch3[0].Name())

	// Reading past end with n>0 must return io.EOF per fs.ReadDirFile contract.
	batch4, err := dirFile.ReadDir(2)
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, batch4)

	require.NoError(t, f.Close())
}

func TestMemFSStatSizeAfterRead(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.WriteFile("file.txt", []byte("hello world"), 0o644))

	f, err := rootFS.Open("file.txt")
	require.NoError(t, err)

	// Stat before any reads should report the full size.
	fi, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(11), fi.Size())

	// Partial read.
	buf := make([]byte, 5)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	// Stat after partial read must still report the full file size.
	fi, err = f.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(11), fi.Size(), "Size() should not decrease after Read()")

	require.NoError(t, f.Close())
}
