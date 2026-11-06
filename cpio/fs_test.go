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
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/unikraft/go-archivefs/cpio"
)

type entry struct {
	hdr  cpio.Header
	body string
}

func file(name string, mode cpio.FileMode, body string) entry {
	return entry{hdr: cpio.Header{Name: name, Mode: cpio.TypeRegular | mode}, body: body}
}

func dir(name string, mode cpio.FileMode) entry {
	return entry{hdr: cpio.Header{Name: name, Mode: cpio.TypeDir | mode}}
}

func symlink(name, target string) entry {
	return entry{hdr: cpio.Header{Name: name, Mode: cpio.TypeSymlink | 0o777}, body: target}
}

func node(name string, mode cpio.FileMode, major, minor int) entry {
	return entry{hdr: cpio.Header{Name: name, Mode: mode, RdevMajor: major, RdevMinor: minor}}
}

func hardlink(name string, ino int64, mode cpio.FileMode, body string) entry {
	return entry{hdr: cpio.Header{Name: name, Mode: cpio.TypeRegular | mode, Inode: ino, Links: 2}, body: body}
}

// archive writes the entries to a CPIO archive. It writes the trailer only if
// trailer is true.
func archive(t *testing.T, trailer bool, entries ...entry) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := cpio.NewWriter(&buf)
	for _, e := range entries {
		hdr := e.hdr
		hdr.Size = int64(len(e.body))
		if err := w.WriteHeader(&hdr); err != nil {
			t.Fatalf("WriteHeader(%q): %v", hdr.Name, err)
		}
		if _, err := w.Write([]byte(e.body)); err != nil {
			t.Fatalf("Write(%q): %v", hdr.Name, err)
		}
	}

	var err error
	if trailer {
		err = w.Close()
	} else {
		err = w.Flush()
	}
	if err != nil {
		t.Fatalf("closing archive: %v", err)
	}

	return buf.Bytes()
}

func openFS(t *testing.T, entries ...entry) *cpio.FS {
	t.Helper()

	fsys, err := cpio.OpenFS(bytes.NewReader(archive(t, true, entries...)))
	if err != nil {
		t.Fatalf("OpenFS: %v", err)
	}

	return fsys
}

// rootfs returns a file system with all the file types and link layouts.
func rootfs(t *testing.T) *cpio.FS {
	t.Helper()

	return openFS(t,
		dir(".", 0o700),
		dir("./bin", 0o755),
		file("./bin/sh", 0o755, "#!/bin/sh\n"),
		file("bin/su", cpio.ModeSetuid|0o755, "su"),
		symlink("bin/ash", "sh"),
		symlink("bin", "usr/bin"),
		file("usr/lib/libc.so", 0o644, "libc"),
		symlink("usr/lib/libc.so.6", "libc.so"),
		dir("usr", 0o750),
		symlink("lib", "usr/lib"),
		dir("lib/.", 0o700),
		symlink("bin/lib", "/usr/lib"),
		file("lib/extra.so", 0o644, "extra"),
		file("lib/../os-release", 0o644, "cpio"),
		symlink("etc/ld.so", "../usr/lib/libc.so"),
		symlink("etc/os-release", "../lib/../os-release"),
		hardlink("/etc/hosts", 100, 0o644, "first"),
		hardlink("link/a", 100, 0o644, ""),
		hardlink("link/b", 100, 0o644, ""),
		hardlink("link/c", 200, 0o644, ""),
		hardlink("link/d", 200, 0o600, "last"),
		hardlink("link/e", 300, 0o644, "kept"),
		hardlink("link/f", 300, 0o644, ""),
		symlink("link/e", "f"),
		node("dev/null", cpio.TypeCharDevice|0o666, 1, 3),
		node("dev/sda", cpio.TypeDevice|0o660, 8, 0),
		node("dev/fifo", cpio.TypeNamedPipe|0o644, 0, 0),
		node("dev/sock", cpio.TypeSocket|0o755, 0, 0),
		file("etc/hosts", 0o600, "new"),
	)
}

func TestFS(t *testing.T) {
	if err := fstest.TestFS(rootfs(t),
		"bin/sh", "bin/su", "bin/ash", "usr/lib/libc.so", "usr/lib/libc.so.6", "usr/lib/extra.so",
		"usr/os-release", "lib", "bin/lib", "etc/ld.so", "etc/os-release", "etc/hosts", "link/a", "link/b",
		"link/c", "link/d", "link/e", "link/f", "dev/null", "dev/sda", "dev/fifo", "dev/sock",
	); err != nil {
		t.Fatal(err)
	}
}

func TestFSLstat(t *testing.T) {
	fsys := rootfs(t)

	tests := []struct {
		name         string
		mode         fs.FileMode
		size         int64
		major, minor uint32
	}{
		{name: ".", mode: fs.ModeDir | 0o700},
		{name: "etc", mode: fs.ModeDir | 0o755},
		{name: "usr", mode: fs.ModeDir | 0o750},
		{name: "usr/lib", mode: fs.ModeDir | 0o700},
		{name: "bin/su", mode: fs.ModeSetuid | 0o755, size: 2},
		{name: "link/a", mode: 0o600, size: 3},
		{name: "dev/null", mode: fs.ModeDevice | fs.ModeCharDevice | 0o666, major: 1, minor: 3},
		{name: "dev/sda", mode: fs.ModeDevice | 0o660, major: 8},
		{name: "dev/fifo", mode: fs.ModeNamedPipe | 0o644},
		{name: "dev/sock", mode: fs.ModeSocket | 0o755},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := fsys.Lstat(tt.name)
			if err != nil {
				t.Fatalf("Lstat: %v", err)
			}
			if info.Mode() != tt.mode {
				t.Errorf("Mode() = %v, want %v", info.Mode(), tt.mode)
			}
			if info.Size() != tt.size {
				t.Errorf("Size() = %d, want %d", info.Size(), tt.size)
			}

			hdr := info.Sys().(*cpio.Header)
			if hdr.GetDevMajor() != tt.major || hdr.GetDevMinor() != tt.minor {
				t.Errorf("device = %d:%d, want %d:%d", hdr.GetDevMajor(), hdr.GetDevMinor(), tt.major, tt.minor)
			}
		})
	}
}

func TestFSReadFile(t *testing.T) {
	fsys := rootfs(t)

	tests := []struct {
		name, want string
	}{
		{"bin/ash", "#!/bin/sh\n"},
		{"lib/libc.so", "libc"},
		{"bin/lib/libc.so", "libc"},
		{"etc/ld.so", "libc"},
		{"etc/os-release", "cpio"},
		{"etc/hosts", "new"},
		{"link/b", "new"},
		{"link/c", "last"},
		{"link/f", "kept"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := fs.ReadFile(fsys, tt.name)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("ReadFile = %q, want %q", b, tt.want)
			}
		})
	}
}

func TestFSReadLink(t *testing.T) {
	fsys := rootfs(t)

	tests := map[string]string{
		"bin/lib":       "/usr/lib",
		"etc/ld.so":     "../usr/lib/libc.so",
		"lib/libc.so.6": "libc.so",
	}

	for name, want := range tests {
		got, err := fs.ReadLink(fsys, name)
		if err != nil {
			t.Errorf("ReadLink(%q): %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("ReadLink(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestFSInodes(t *testing.T) {
	fsys := rootfs(t)

	nlink := map[string]uint64{
		".":         7,
		"usr/lib":   2,
		"etc/hosts": 3,
		"link/f":    1,
	}
	links := [][]string{
		{"etc/hosts", "link/a", "link/b"},
		{"link/c", "link/d"},
	}

	names := map[uint64][]string{}
	if err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}

		hdr := info.Sys().(*cpio.Header)
		names[hdr.GetIno()] = append(names[hdr.GetIno()], name)
		if want, ok := nlink[name]; ok && hdr.GetNlink() != want {
			t.Errorf("%s: GetNlink() = %d, want %d", name, hdr.GetNlink(), want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var shared [][]string
	for ino, list := range names {
		if ino == 0 {
			t.Errorf("%v: inode 0", list)
		}
		if len(list) > 1 {
			shared = append(shared, list)
		}
	}
	slices.SortFunc(shared, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	if !slices.EqualFunc(shared, links, slices.Equal) {
		t.Errorf("files with one inode = %v, want %v", shared, links)
	}
}

func TestFSErrors(t *testing.T) {
	fsys := openFS(t,
		file("file", 0o644, "data"),
		file("file/x", 0o644, "data"),
		symlink("dangling", "missing"),
		file("dangling/x", 0o644, "data"),
		symlink("loop1", "loop2"),
		symlink("loop2", "loop1"),
	)

	tests := []struct {
		desc string
		fn   func() error
		want error
	}{
		{"invalid path", func() error { _, err := fsys.Open("../file"); return err }, fs.ErrInvalid},
		{"missing file", func() error { _, err := fsys.Open("missing"); return err }, fs.ErrNotExist},
		{"entry below a dangling symlink", func() error { _, err := fsys.Open("dangling/x"); return err }, fs.ErrNotExist},
		{"readlink on file", func() error { _, err := fsys.ReadLink("file"); return err }, fs.ErrInvalid},
		{"symlink loop", func() error { _, err := fsys.Open("loop1"); return err }, nil},
		{"readdir on file", func() error { _, err := fsys.ReadDir("file"); return err }, nil},
		{"readfile on dir", func() error { _, err := fsys.ReadFile("."); return err }, nil},
	}

	for _, tt := range tests {
		err := tt.fn()
		var pathErr *fs.PathError
		if !errors.As(err, &pathErr) {
			t.Errorf("%s: error = %v, want *fs.PathError", tt.desc, err)
			continue
		}
		if tt.want != nil && !errors.Is(err, tt.want) {
			t.Errorf("%s: error = %v, want %v", tt.desc, err, tt.want)
		}
	}
}

func TestOpenFSInvalid(t *testing.T) {
	negative := archive(t, true, file("file", 0o644, ""))
	copy(negative[54:62], "-0000001")

	tests := []struct {
		desc string
		data []byte
		want error
	}{
		{"no trailer", archive(t, false, file("file", 0o644, "data")), io.ErrUnexpectedEOF},
		{"bad magic", bytes.Repeat([]byte("x"), 512), cpio.ErrHeader},
		{"negative size", negative, cpio.ErrHeader},
	}

	for _, tt := range tests {
		if _, err := cpio.OpenFS(bytes.NewReader(tt.data)); !errors.Is(err, tt.want) {
			t.Errorf("%s: OpenFS error = %v, want %v", tt.desc, err, tt.want)
		}
	}
}
