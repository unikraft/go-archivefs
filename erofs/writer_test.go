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
	"encoding/binary"
	"fmt"
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

func TestWriterDirectoryNlink(t *testing.T) {
	testFS := fstest.MapFS{
		"a/b/f":  &fstest.MapFile{Data: []byte("f"), Mode: 0o644},
		"a/c/f":  &fstest.MapFile{Data: []byte("f"), Mode: 0o644},
		"a/f":    &fstest.MapFile{Data: []byte("f"), Mode: 0o644},
		"a/link": &fstest.MapFile{Data: []byte("b"), Mode: fs.ModeSymlink | 0o777},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS, WithAllRoot(true)))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	for name, want := range map[string]uint32{".": 3, "a": 4, "a/b": 2, "a/c": 2} {
		fi, err := fsys.Stat(name)
		require.NoError(t, err)
		require.Equal(t, want, fi.Sys().(*Inode).Nlink(), name)
	}
}

// inodeSys is a FileInfo.Sys() value with an inode number and a link count.
type inodeSys struct{ ino, nlink uint64 }

func (s inodeSys) GetIno() uint64   { return s.ino }
func (s inodeSys) GetNlink() uint64 { return s.nlink }

func TestWriterHardLinkLayout(t *testing.T) {
	// The links get inodes of different sizes, and "aa" is written between them.
	target := strings.Repeat("t", 4040)
	data := bytes.Repeat([]byte("d"), BlockSize)
	testFS := fstest.MapFS{
		"a":  &fstest.MapFile{Data: []byte(target), Mode: fs.ModeSymlink | 0o777, Sys: inodeSys{7, 2}},
		"aa": &fstest.MapFile{Data: data, Mode: 0o644},
		"b":  &fstest.MapFile{Data: []byte(target), Mode: fs.ModeSymlink | 0o777, Sys: inodeSys{7, 2}, ModTime: time.Unix(1, 0)},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	for _, name := range []string{"a", "b"} {
		got, err := fsys.ReadLink(name)
		require.NoError(t, err)
		require.Equal(t, target, got, name)
	}
	got, err := fs.ReadFile(fsys, "aa")
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestWriterSpecialFileHardLinks(t *testing.T) {
	testFS := fstest.MapFS{
		"fifo1": &fstest.MapFile{Mode: fs.ModeNamedPipe | 0o644, Sys: inodeSys{9, 2}},
		"fifo2": &fstest.MapFile{Mode: fs.ModeNamedPipe | 0o644, Sys: inodeSys{9, 2}},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var nids []uint64
	for _, name := range []string{"fifo1", "fifo2"} {
		fi, err := fsys.Stat(name)
		require.NoError(t, err)
		require.Equal(t, uint32(2), fi.Sys().(*Inode).Nlink(), name)
		nids = append(nids, fi.Sys().(*Inode).Nid())
	}
	require.Equal(t, nids[0], nids[1])
}

func TestWriterInodeNumberCollision(t *testing.T) {
	// Files on different devices can have the same inode number, but only files of one type are links.
	data := bytes.Repeat([]byte("c"), 3*BlockSize)
	zdata := bytes.Repeat([]byte("z"), 3*BlockSize+5)
	testFS := fstest.MapFS{
		"a":    &fstest.MapFile{Data: []byte("target"), Mode: fs.ModeSymlink | 0o777, Sys: inodeSys{7, 2}},
		"c":    &fstest.MapFile{Data: data, Mode: 0o644},
		"fifo": &fstest.MapFile{Mode: fs.ModeNamedPipe | 0o644, Sys: inodeSys{8, 2}},
		"r":    &fstest.MapFile{Data: []byte("r"), Mode: 0o644, Sys: inodeSys{8, 2}},
		"z":    &fstest.MapFile{Data: zdata, Mode: 0o644, Sys: inodeSys{7, 2}},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, testFS))
	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	for name, want := range map[string][]byte{"c": data, "r": []byte("r"), "z": zdata} {
		got, err := fs.ReadFile(fsys, name)
		require.NoError(t, err, name)
		require.Equal(t, want, got, name)
	}
	target, err := fsys.ReadLink("a")
	require.NoError(t, err)
	require.Equal(t, "target", target)
	fi, err := fsys.Lstat("fifo")
	require.NoError(t, err)
	require.Equal(t, fs.ModeNamedPipe, fi.Mode().Type())
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

func TestWriterDevice(t *testing.T) {
	dev, err := os.OpenFile("/dev/null", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("no /dev/null")
	}
	defer dev.Close()

	testFS := fstest.MapFS{"f": &fstest.MapFile{Data: []byte("f"), Mode: 0o644}}
	require.NoError(t, Create(dev, testFS))
}

// changedFS is a file system in which "f" has other data when it is opened than when it is walked.
type changedFS struct {
	fstest.MapFS
	changed fstest.MapFS
}

func (c changedFS) Open(name string) (fs.File, error) {
	if name == "f" {
		return c.changed.Open(name)
	}
	return c.MapFS.Open(name)
}

func TestWriterFileGrows(t *testing.T) {
	src := changedFS{
		MapFS:   fstest.MapFS{"f": &fstest.MapFile{Data: bytes.Repeat([]byte("g"), BlockSize), Mode: 0o644}},
		changed: fstest.MapFS{"f": &fstest.MapFile{Data: bytes.Repeat([]byte("G"), 3*BlockSize), Mode: 0o644}},
	}

	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, src))
	var sb SuperBlock
	require.NoError(t, binary.Read(bytes.NewReader(buf.Bytes()[SuperBlockOffset:]), binary.LittleEndian, &sb))
	require.Equal(t, int64(sb.Blocks)*BlockSize, int64(len(buf.Bytes())))

	fsys, err := Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	got, err := fs.ReadFile(fsys, "f")
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("G"), BlockSize), got)
}

func TestWriterFileShrinks(t *testing.T) {
	src := changedFS{
		MapFS:   fstest.MapFS{"f": &fstest.MapFile{Data: bytes.Repeat([]byte("g"), 3*BlockSize), Mode: 0o644}},
		changed: fstest.MapFS{"f": &fstest.MapFile{Data: []byte("short"), Mode: 0o644}},
	}

	require.ErrorContains(t, Create(newBytesWriterAt(0), src), "became shorter")
}

func TestWriterReusedDestination(t *testing.T) {
	// The files end in the middle of a block, so the image has gaps between the writes.
	testFS := fstest.MapFS{
		"a":   &fstest.MapFile{Data: []byte("a"), Mode: 0o644},
		"b":   &fstest.MapFile{Data: bytes.Repeat([]byte("b"), BlockSize+1), Mode: 0o644},
		"d/c": &fstest.MapFile{Data: []byte("c"), Mode: 0o644},
	}
	opts := []ErofsCreateOption{WithUUID([16]byte{1}), WithBuildTime(time.Unix(1, 0))}
	want := newBytesWriterAt(0)
	require.NoError(t, Create(want, testFS, opts...))

	f, err := os.Create(filepath.Join(t.TempDir(), "img"))
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(bytes.Repeat([]byte{0xaa}, 1<<20))
	require.NoError(t, err)
	require.NoError(t, Create(f, testFS, opts...))
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)

	require.Len(t, got, len(want.Bytes()))
	stale := 0
	for i := range got {
		if got[i] != want.Bytes()[i] {
			stale++
		}
	}
	require.Zero(t, stale, "bytes that differ from an image in an empty destination")
}
