// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package erofs_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs"
	"github.com/unikraft/go-archivefs/erofs"
	"github.com/unikraft/go-archivefs/tarfs"
)

// capNetBindService is a security.capability value that gives CAP_NET_BIND_SERVICE.
var capNetBindService = string([]byte{
	0x01, 0x00, 0x00, 0x02, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
})

type tarEntry struct {
	hdr  tar.Header
	body string
}

func xattrTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := e.hdr
		hdr.Format = tar.FormatPAX
		hdr.Size = int64(len(e.body))
		if hdr.Typeflag != tar.TypeReg {
			hdr.Size = 0
		}
		require.NoError(t, tw.WriteHeader(&hdr))
		if hdr.Size > 0 {
			_, err := tw.Write([]byte(e.body))
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func paxXattrs(xattrs map[string]string) map[string]string {
	records := map[string]string{}
	for k, v := range xattrs {
		records["SCHILY.xattr."+k] = v
	}
	return records
}

// xattrEntries returns a tree with xattrs on regular files, directories, symlinks, a device, hard links and next to inline data.
func xattrEntries() ([]tarEntry, map[string]map[string]string) {
	capXattrs := map[string]string{
		"security.capability":     capNetBindService,
		"user.a":                  "x",
		"system.posix_acl_access": "\x02\x00\x00\x00\x01\x00\x06\x00\xff\xff\xff\xff",
	}
	want := map[string]map[string]string{
		"d":        {"user.dir": "1"},
		"d/cap":    capXattrs,
		"d/z-link": capXattrs,
		"a-link":   capXattrs,
		"s":        {"trusted.t": "v"},
		"big":      {"user.big": strings.Repeat("b", 3000)},
		"bigdir":   {"user.big": strings.Repeat("c", 3500)},
		"dev":      {"security.selinux": "system_u:object_r:null_device_t:s0"},
	}

	entries := []tarEntry{
		{hdr: tar.Header{Typeflag: tar.TypeDir, Name: "d/", Mode: 0o755, PAXRecords: paxXattrs(want["d"])}},
		{hdr: tar.Header{Typeflag: tar.TypeReg, Name: "d/cap", Mode: 0o755, PAXRecords: paxXattrs(capXattrs)}, body: "cap"},
		{hdr: tar.Header{Typeflag: tar.TypeLink, Name: "d/z-link", Linkname: "d/cap"}},
		{hdr: tar.Header{Typeflag: tar.TypeLink, Name: "a-link", Linkname: "d/cap"}},
		{hdr: tar.Header{Typeflag: tar.TypeSymlink, Name: "s", Linkname: "d/cap", Mode: 0o777, PAXRecords: paxXattrs(want["s"])}},
		{hdr: tar.Header{Typeflag: tar.TypeReg, Name: "big", Mode: 0o644, PAXRecords: paxXattrs(want["big"])}, body: strings.Repeat("x", 5000)},
		{hdr: tar.Header{Typeflag: tar.TypeDir, Name: "bigdir/", Mode: 0o755, PAXRecords: paxXattrs(want["bigdir"])}},
		{hdr: tar.Header{Typeflag: tar.TypeReg, Name: "plain", Mode: 0o644}, body: "plain"},
		{hdr: tar.Header{Typeflag: tar.TypeChar, Name: "dev", Mode: 0o666, Devmajor: 1, Devminor: 3, PAXRecords: paxXattrs(want["dev"])}},
	}
	for i := range 30 {
		entries = append(entries, tarEntry{
			hdr:  tar.Header{Typeflag: tar.TypeReg, Name: fmt.Sprintf("bigdir/f%02d", i), Mode: 0o644},
			body: "f",
		})
	}

	return entries, want
}

// checkXattrs checks the xattrs whose names start with prefix.
func checkXattrs(t *testing.T, fsys fs.FS, want map[string]map[string]string, prefix string) {
	t.Helper()

	err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		info, err := d.Info()
		require.NoError(t, err)
		got, err := info.Sys().(archivefs.XattrInfo).GetXattrs()
		require.NoError(t, err, name)
		maps.DeleteFunc(got, func(k, _ string) bool { return !strings.HasPrefix(k, prefix) })
		if len(want[name]) == 0 {
			require.Empty(t, got, name)
		} else {
			require.Equal(t, want[name], got, name)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestXattrs(t *testing.T) {
	entries, want := xattrEntries()
	srcFS, err := tarfs.Open(bytes.NewReader(xattrTar(t, entries)))
	require.NoError(t, err)

	imgPath := filepath.Join(t.TempDir(), "xattr.img")
	img, err := os.Create(imgPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, img.Close()) })
	require.NoError(t, erofs.Create(img, srcFS, erofs.WithAllRoot(true)))

	fsys, err := erofs.Open(img)
	require.NoError(t, err)
	checkXattrs(t, fsys, want, "")

	for name, body := range map[string]string{"d/cap": "cap", "a-link": "cap", "big": strings.Repeat("x", 5000), "plain": "plain"} {
		got, err := fs.ReadFile(fsys, name)
		require.NoError(t, err)
		require.Equal(t, body, string(got), name)
	}
	target, err := fsys.ReadLink("s")
	require.NoError(t, err)
	require.Equal(t, "d/cap", target)
	dirents, err := fs.ReadDir(fsys, "bigdir")
	require.NoError(t, err)
	require.Len(t, dirents, 30)

	if fsck, err := exec.LookPath("fsck.erofs"); err == nil {
		out, err := exec.Command(fsck, imgPath).CombinedOutput()
		require.NoError(t, err, string(out))
	}
}

func TestXattrsFromMkfs(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skip("mkfs.erofs is not installed")
	}
	if help, _ := exec.Command(mkfs, "--help").CombinedOutput(); !bytes.Contains(help, []byte("--tar")) {
		t.Skip("mkfs.erofs does not support --tar")
	}

	entries, want := xattrEntries()

	dir := t.TempDir()
	tarPath := filepath.Join(dir, "xattr.tar")
	imgPath := filepath.Join(dir, "xattr.img")
	require.NoError(t, os.WriteFile(tarPath, xattrTar(t, entries), 0o644))
	out, err := exec.Command(mkfs, "--tar=f", imgPath, tarPath).CombinedOutput()
	require.NoError(t, err, string(out))

	img, err := os.Open(imgPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, img.Close()) })

	fsys, err := erofs.Open(img)
	require.NoError(t, err)
	checkXattrs(t, fsys, want, "")
}

type xattrSys map[string]string

func (x xattrSys) GetXattrs() (map[string]string, error) { return x, nil }

func TestXattrsUnsupported(t *testing.T) {
	src := fstest.MapFS{"f": &fstest.MapFile{Data: []byte("f"), Mode: 0o644, Sys: xattrSys{
		"com.apple.quarantine":             "x",
		"user.":                            "x",
		"user." + strings.Repeat("n", 251): "x",
		"user.kept":                        "x",
	}}}
	fsys := createImage(t, src, erofs.WithAllRoot(true))
	checkXattrs(t, fsys, map[string]map[string]string{"f": {"user.kept": "x"}}, "")
}

func TestXattrsValueTooLong(t *testing.T) {
	src := fstest.MapFS{"f": &fstest.MapFile{Data: []byte("f"), Mode: 0o644, Sys: xattrSys{"user.v": strings.Repeat("v", 1<<16)}}}
	img, err := os.Create(filepath.Join(t.TempDir(), "img"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, img.Close()) })
	require.ErrorContains(t, erofs.Create(img, src, erofs.WithAllRoot(true)), "longer than")
}
