// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/unikraft/go-archivefs/cpio"
)

// tree returns a source file system with all the file types and link layouts.
// The Sys values have no mode, and their inodes start at 100.
func tree() fstest.MapFS {
	hosts := &cpio.Header{Uid: 1000, Guid: 100, Inode: 100, Links: 3}
	sh := &cpio.Header{Inode: 103, Links: 2}
	mtime := time.Unix(1700000000, 0)

	return fstest.MapFS{
		"TRAILER!!!": {Mode: 0o644, Data: []byte("t")},
		"bin":        {Mode: fs.ModeDir | 0o755},
		"bin/su":     {Mode: fs.ModeSetuid | 0o755, Data: []byte("su")},
		"dev":        {Mode: fs.ModeDir | 0o755},
		"dev/fifo":   {Mode: fs.ModeNamedPipe | 0o644},
		"dev/null":   {Mode: fs.ModeDevice | fs.ModeCharDevice | 0o666, Sys: &cpio.Header{RdevMajor: 1, RdevMinor: 3, DevMajor: 8}},
		"dev/sda1":   {Mode: fs.ModeDevice | 0o660, Sys: &cpio.Header{RdevMajor: 8, RdevMinor: 1}},
		"dev/sock":   {Mode: fs.ModeSocket | 0o755},
		"etc":        {Mode: fs.ModeDir | 0o755},
		"etc/future": {Mode: 0o644, Data: []byte("future"), ModTime: time.Unix(1<<33, 0)},
		"etc/hosts":  {Mode: 0o644, Data: []byte("hosts"), ModTime: mtime, Sys: hosts},
		"etc/lone":   {Mode: 0o644, Data: []byte("lone"), Sys: &cpio.Header{Inode: 101, Links: 2}},
		"etc/old":    {Mode: 0o644, Data: []byte("old"), ModTime: time.Unix(-1, 0)},
		"home":       {Mode: fs.ModeDir | 0o755},
		"home/user":  {Mode: fs.ModeDir | 0o700, Sys: &cpio.Header{Uid: 1000, Guid: 1000, Inode: 104, Links: 2}},
		"link":       {Mode: fs.ModeDir | 0o755},
		"link/a":     {Mode: 0o644, Data: []byte("hosts"), ModTime: mtime, Sys: hosts},
		"link/b":     {Mode: 0o644, Data: []byte("hosts"), ModTime: mtime, Sys: hosts},
		"link/sh1":   {Mode: fs.ModeSymlink | 0o777, Data: []byte("../bin/su"), Sys: sh},
		"link/sh2":   {Mode: fs.ModeSymlink | 0o777, Data: []byte("../bin/su"), Sys: sh},
	}
}

func TestCreate(t *testing.T) {
	var buf bytes.Buffer
	if err := cpio.Create(&buf, tree()); err != nil {
		t.Fatalf("Create: %v", err)
	}

	type entry struct {
		name         string
		mode         cpio.FileMode
		uid, gid     int
		ino          int64
		links        int
		rdev         [2]int
		mtime        int64
		target, body string
	}
	want := []entry{
		{name: "./TRAILER!!!", mode: cpio.TypeRegular | 0o644, ino: 1, links: 1, body: "t"},
		{name: "./bin", mode: cpio.TypeDir | 0o755, ino: 2, links: 2},
		{name: "./bin/su", mode: cpio.TypeRegular | cpio.ModeSetuid | 0o755, ino: 3, links: 1, body: "su"},
		{name: "./dev", mode: cpio.TypeDir | 0o755, ino: 4, links: 2},
		{name: "./dev/fifo", mode: cpio.TypeNamedPipe | 0o644, ino: 5, links: 1},
		{name: "./dev/null", mode: cpio.TypeCharDevice | 0o666, ino: 6, links: 1, rdev: [2]int{1, 3}},
		{name: "./dev/sda1", mode: cpio.TypeDevice | 0o660, ino: 7, links: 1, rdev: [2]int{8, 1}},
		{name: "./dev/sock", mode: cpio.TypeSocket | 0o755, ino: 8, links: 1},
		{name: "./etc", mode: cpio.TypeDir | 0o755, ino: 9, links: 2},
		{name: "./etc/future", mode: cpio.TypeRegular | 0o644, ino: 10, links: 1, mtime: 1<<32 - 1, body: "future"},
		{name: "./etc/hosts", mode: cpio.TypeRegular | 0o644, uid: 1000, gid: 100, ino: 11, links: 3, mtime: 1700000000, body: "hosts"},
		{name: "./etc/lone", mode: cpio.TypeRegular | 0o644, ino: 12, links: 1, body: "lone"},
		{name: "./etc/old", mode: cpio.TypeRegular | 0o644, ino: 13, links: 1, body: "old"},
		{name: "./home", mode: cpio.TypeDir | 0o755, ino: 14, links: 3},
		{name: "./home/user", mode: cpio.TypeDir | 0o700, uid: 1000, gid: 1000, ino: 15, links: 2},
		{name: "./link", mode: cpio.TypeDir | 0o755, ino: 16, links: 2},
		{name: "./link/a", mode: cpio.TypeRegular | 0o644, uid: 1000, gid: 100, ino: 11, links: 3, mtime: 1700000000},
		{name: "./link/b", mode: cpio.TypeRegular | 0o644, uid: 1000, gid: 100, ino: 11, links: 3, mtime: 1700000000},
		{name: "./link/sh1", mode: cpio.TypeSymlink | 0o777, ino: 17, links: 2, target: "../bin/su"},
		{name: "./link/sh2", mode: cpio.TypeSymlink | 0o777, ino: 17, links: 2, target: "../bin/su"},
	}

	var got []entry
	r := cpio.NewReader(&buf)
	for {
		hdr, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		body, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("reading %s: %v", hdr.Name, err)
		}
		if hdr.DevMajor != 0 || hdr.DevMinor != 0 {
			t.Errorf("%s: device %d:%d, want 0:0", hdr.Name, hdr.DevMajor, hdr.DevMinor)
		}
		got = append(got, entry{
			name:   hdr.Name,
			mode:   hdr.Mode,
			uid:    hdr.Uid,
			gid:    hdr.Guid,
			ino:    hdr.Inode,
			links:  hdr.Links,
			rdev:   [2]int{hdr.RdevMajor, hdr.RdevMinor},
			mtime:  hdr.ModTime.Unix(),
			target: hdr.Linkname,
			body:   string(body),
		})
	}

	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d:\n%+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

func TestCreateErrors(t *testing.T) {
	// a-big fills the write buffer before the bad entry. Thus an entry check
	// after the first write puts bytes in dst.
	big := &fstest.MapFile{Mode: 0o644, Data: bytes.Repeat([]byte("a"), 1<<17)}

	tests := []struct {
		desc string
		src  fs.FS
		want error
	}{
		{
			desc: "symlink without ReadLinkFS",
			src: struct{ fs.FS }{fstest.MapFS{
				"a-big":  big,
				"z-link": {Mode: fs.ModeSymlink | 0o777, Data: []byte("a-big")},
			}},
			want: fs.ErrInvalid,
		},
		{
			desc: "irregular file",
			src: fstest.MapFS{
				"a-big":       big,
				"z-irregular": {Mode: fs.ModeIrregular | 0o644},
			},
		},
		{
			desc: "hard links with different sizes",
			src: fstest.MapFS{
				"a-big": big,
				"z-1":   {Mode: 0o644, Data: []byte("one"), Sys: &cpio.Header{Inode: 7, Links: 2}},
				"z-2":   {Mode: 0o644, Data: []byte("two!"), Sys: &cpio.Header{Inode: 7, Links: 2}},
			},
		},
		{
			desc: "empty symlink target",
			src: fstest.MapFS{
				"a-big":  big,
				"z-link": {Mode: fs.ModeSymlink | 0o777},
			},
			want: cpio.ErrHeader,
		},
	}

	for _, tt := range tests {
		var buf bytes.Buffer
		err := cpio.Create(&buf, tt.src)
		if err == nil || tt.want != nil && !errors.Is(err, tt.want) {
			t.Errorf("%s: Create error = %v, want %v", tt.desc, err, tt.want)
		}
		if buf.Len() != 0 {
			t.Errorf("%s: Create wrote %d bytes, want 0", tt.desc, buf.Len())
		}
	}
}
