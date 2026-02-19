// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package erofs

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
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

// TestWriterDaxSafeInlining checks that, like mkfs.erofs -E noinline_data, only directories and symlinks have inline data.
func TestWriterDaxSafeInlining(t *testing.T) {
	testFS := fstest.MapFS{
		"small.txt": &fstest.MapFile{
			Data:    []byte("small file data"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"tail.bin": &fstest.MapFile{
			Data:    make([]byte, 5000), // one block + tail
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"dir/file.txt": &fstest.MapFile{
			Data:    []byte("nested"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		},
		"link": &fstest.MapFile{
			Data:    []byte("dir/file.txt"),
			Mode:    fs.ModeSymlink | 0o777,
			ModTime: time.Unix(1234567890, 0),
		},
	}
	// A directory with more than one block of entries, which has full blocks and an inline tail.
	for i := 0; i < 400; i++ {
		name := fmt.Sprintf("bigdir/entry-%04d.txt", i)
		testFS[name] = &fstest.MapFile{
			Data:    []byte("x"),
			Mode:    0o644,
			ModTime: time.Unix(1234567890, 0),
		}
	}

	buf := newBytesWriterAt(1024 * 1024)
	require.NoError(t, Create(buf, testFS, WithAllRoot(true)))

	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	layoutOf := func(path string) uint16 {
		t.Helper()
		info, err := fsys.Lstat(path)
		require.NoError(t, err)
		return info.Sys().(*Inode).DataLayout()
	}

	// Regular files must never carry inline data (DAX).
	require.Equal(t, uint16(InodeDataLayoutFlatPlain), layoutOf("small.txt"))
	require.Equal(t, uint16(InodeDataLayoutFlatPlain), layoutOf("tail.bin"))

	// Small directories and symlinks are fully inlined.
	require.Equal(t, uint16(InodeDataLayoutFlatInline), layoutOf("dir"))
	require.Equal(t, uint16(InodeDataLayoutFlatInline), layoutOf("link"))

	// A large directory has its full blocks in the data area, and its tail inline.
	bigdirInfo, err := fsys.Stat("bigdir")
	require.NoError(t, err)
	bigdirIno := bigdirInfo.Sys().(*Inode)
	require.Greater(t, bigdirIno.Size(), uint64(BlockSize), "bigdir should span multiple blocks")
	require.Equal(t, uint16(InodeDataLayoutFlatInline), bigdirIno.DataLayout())

	// All entries of the large directory can be read, and the symlink resolves.
	entries, err := fsys.ReadDir("bigdir")
	require.NoError(t, err)
	require.Len(t, entries, 400)

	f, err := fsys.Open("bigdir/entry-0399.txt")
	require.NoError(t, err)
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "x", string(content))
	require.NoError(t, f.Close())

	target, err := fsys.ReadLink("link")
	require.NoError(t, err)
	require.Equal(t, "dir/file.txt", target)
}

func TestWriterLongSymlink(t *testing.T) {
	target := strings.Repeat("t", BlockSize+100)
	testFS := fstest.MapFS{"link": &fstest.MapFile{Data: []byte(target), Mode: fs.ModeSymlink | 0o777}}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Like Linux, the reader returns at most one block minus the terminating NUL.
	got, err := fsys.ReadLink("link")
	require.NoError(t, err)
	require.Equal(t, target[:BlockSize-1], got)
}
