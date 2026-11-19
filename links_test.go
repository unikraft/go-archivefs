// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package archivefs_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs"
)

type inodeSys struct{ ino, nlink uint64 }

func (s inodeSys) GetIno() uint64   { return s.ino }
func (s inodeSys) GetNlink() uint64 { return s.nlink }

func addLinks(t *testing.T, fsys fstest.MapFS, names ...string) []string {
	t.Helper()

	var links archivefs.HardLinks
	var firsts []string
	for _, name := range names {
		fi, err := fsys.Lstat(name)
		require.NoError(t, err)
		first, err := links.Add(fsys, name, fi)
		require.NoError(t, err, name)
		firsts = append(firsts, first)
	}
	return firsts
}

func TestHardLinks(t *testing.T) {
	fsys := fstest.MapFS{
		"a":     {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{7, 3}},
		"b":     {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{7, 3}},
		"c":     {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{7, 3}},
		"one":   {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{8, 1}},
		"one2":  {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{8, 1}},
		"zero":  {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{0, 2}},
		"zero2": {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{0, 2}},
		"fifo":  {Mode: fs.ModeNamedPipe | 0o644, Sys: inodeSys{7, 2}},
		"d":     {Mode: fs.ModeDir | 0o755, Sys: inodeSys{9, 2}},
		"d2":    {Mode: fs.ModeDir | 0o755, Sys: inodeSys{9, 2}},
		"s":     {Data: []byte("t"), Mode: fs.ModeSymlink | 0o777, Sys: inodeSys{10, 2}},
		"s2":    {Data: []byte("t"), Mode: fs.ModeSymlink | 0o777, Sys: inodeSys{10, 2}},
	}

	got := addLinks(t, fsys, "a", "b", "one", "one2", "zero", "zero2", "fifo", "d", "d2", "s", "s2", "c")
	require.Equal(t, []string{"a", "a", "", "", "", "", "fifo", "", "", "s", "s", "a"}, got)
}

func TestHardLinksMismatch(t *testing.T) {
	sys := inodeSys{7, 2}
	for name, files := range map[string][2]*fstest.MapFile{
		"mode":   {{Mode: 0o644, Data: []byte("x"), Sys: sys}, {Mode: 0o600, Data: []byte("x"), Sys: sys}},
		"size":   {{Mode: 0o644, Data: []byte("x"), Sys: sys}, {Mode: 0o644, Data: []byte("xy"), Sys: sys}},
		"target": {{Mode: fs.ModeSymlink | 0o777, Data: []byte("x"), Sys: sys}, {Mode: fs.ModeSymlink | 0o777, Data: []byte("y"), Sys: sys}},
	} {
		t.Run(name, func(t *testing.T) {
			fsys := fstest.MapFS{"a": files[0], "b": files[1]}
			var links archivefs.HardLinks
			for _, name := range []string{"a", "b"} {
				fi, err := fsys.Lstat(name)
				require.NoError(t, err)
				first, err := links.Add(fsys, name, fi)
				if name == "a" {
					require.NoError(t, err)
					require.Equal(t, "a", first)
					continue
				}
				require.ErrorContains(t, err, "inode of a")
			}
		})
	}
}

func TestHardLinksSamePath(t *testing.T) {
	fsys := fstest.MapFS{"a": {Data: []byte("x"), Mode: 0o644, Sys: inodeSys{7, 2}}}
	fi, err := fsys.Lstat("a")
	require.NoError(t, err)

	var links archivefs.HardLinks
	first, err := links.Add(fsys, "a", fi)
	require.NoError(t, err)
	require.Equal(t, "a", first)
	_, err = links.Add(fsys, "a", fi)
	require.ErrorContains(t, err, "a listed twice")
}
