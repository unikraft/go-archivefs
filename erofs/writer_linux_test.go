// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Unikraft GmbH and The KraftKit Authors.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.
package erofs

import (
	"bytes"
	"io/fs"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestWriterInodeNumberOnOtherDevice(t *testing.T) {
	// Only paths on one device with more than one link are hard links. Other files have one link.
	chr := fs.ModeDevice | fs.ModeCharDevice | 0o666
	testFS := fstest.MapFS{
		"a":    &fstest.MapFile{Data: []byte("a"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 7, Nlink: 2}},
		"a2":   &fstest.MapFile{Data: []byte("a"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 7, Nlink: 2}},
		"b":    &fstest.MapFile{Data: []byte("b"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 2, Ino: 7, Nlink: 2}},
		"c":    &fstest.MapFile{Data: []byte("c"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 8, Nlink: 1}},
		"d":    &fstest.MapFile{Data: []byte("d"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 8, Nlink: 1}},
		"e":    &fstest.MapFile{Data: []byte("e"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 10, Nlink: 0}},
		"null": &fstest.MapFile{Mode: chr, Sys: &syscall.Stat_t{Dev: 1, Ino: 9, Nlink: 2, Rdev: 1<<8 | 3}},
		"zero": &fstest.MapFile{Mode: chr, Sys: &syscall.Stat_t{Dev: 2, Ino: 9, Nlink: 2, Rdev: 1<<8 | 5}},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	inode := func(name string) *Inode {
		fi, err := fsys.Stat(name)
		require.NoError(t, err, name)
		return fi.Sys().(*Inode)
	}
	for _, name := range []string{"a", "a2", "b", "c", "d", "e"} {
		got, err := fs.ReadFile(fsys, name)
		require.NoError(t, err, name)
		require.Equal(t, name[:1], string(got), name)
	}

	require.Equal(t, inode("a").Nid(), inode("a2").Nid())
	require.Equal(t, uint32(2), inode("a").Nlink())
	require.NotEqual(t, inode("a").Nid(), inode("b").Nid())
	require.Equal(t, uint32(1), inode("b").Nlink())
	require.NotEqual(t, inode("c").Nid(), inode("d").Nid())
	require.Equal(t, uint32(1), inode("c").Nlink())
	require.Equal(t, uint32(1), inode("e").Nlink())

	require.NotEqual(t, inode("null").Nid(), inode("zero").Nid())
	require.Equal(t, [2]uint32{1, 3}, [2]uint32{inode("null").GetDevMajor(), inode("null").GetDevMinor()})
	require.Equal(t, [2]uint32{1, 5}, [2]uint32{inode("zero").GetDevMajor(), inode("zero").GetDevMinor()})
}
