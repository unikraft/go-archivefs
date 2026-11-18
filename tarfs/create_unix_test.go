//go:build unix

// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package tarfs_test

import (
	"archive/tar"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs/tarfs"
)

func TestTarFSCreateDirFS(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "d", "f"), []byte("data"), 0o644))
	require.NoError(t, os.Link(filepath.Join(dir, "d", "f"), filepath.Join(dir, "g")))
	sock, err := net.Listen("unix", filepath.Join(dir, "sock"))
	if err != nil {
		t.Skipf("cannot make a unix socket: %v", err)
	}
	t.Cleanup(func() { require.NoError(t, sock.Close()) })

	var dst bytes.Buffer
	require.NoError(t, tarfs.Create(&dst, os.DirFS(dir)))

	type entry struct {
		Name, Linkname string
		Typeflag       byte
	}
	var got []entry
	for _, e := range readTar(t, dst.Bytes()) {
		got = append(got, entry{e.Name, e.Linkname, e.Typeflag})
	}
	require.Equal(t, []entry{
		{Name: "d", Typeflag: tar.TypeDir},
		{Name: "d/f", Typeflag: tar.TypeReg},
		{Name: "g", Linkname: "d/f", Typeflag: tar.TypeLink},
	}, got)
}
