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
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestOpenBlockSize(t *testing.T) {
	buf := newBytesWriterAt(0)
	require.NoError(t, Create(buf, fstest.MapFS{"f": &fstest.MapFile{Data: []byte("f"), Mode: 0o644}}))
	withBlockSizeBits := func(bits uint8) []byte {
		img := bytes.Clone(buf.Bytes())
		var sb SuperBlock
		require.NoError(t, binary.Read(bytes.NewReader(img[SuperBlockOffset:]), binary.LittleEndian, &sb))
		sb.BlockSizeBits = bits
		var out bytes.Buffer
		require.NoError(t, binary.Write(&out, binary.LittleEndian, &sb))
		copy(img[SuperBlockOffset:], out.Bytes())
		return img
	}

	for _, bits := range []uint8{0, 8, 17, 40} {
		_, err := Open(bytes.NewReader(withBlockSizeBits(bits)))
		require.ErrorContains(t, err, "unsupported block size", "block size bits %d", bits)
	}

	// 1024 byte blocks are valid, and Open must refuse the image without a panic.
	_, err := Open(bytes.NewReader(withBlockSizeBits(10)))
	require.ErrorContains(t, err, "invalid checksum")
}

func TestOpenSmallBlocksFromMkfs(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skip("mkfs.erofs not found")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	data := bytes.Repeat([]byte("0123456789"), 300)
	require.NoError(t, os.Mkdir(src, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "f"), data, 0o644))
	targets := map[string]string{}
	for _, n := range []int{300, 1000, 2000, 4095} {
		name := fmt.Sprintf("l%d", n)
		targets[name] = strings.Repeat("t", n)
		require.NoError(t, os.Symlink(targets[name], filepath.Join(src, name)))
	}

	for _, size := range []int{512, 1024, 2048} {
		img := filepath.Join(dir, fmt.Sprintf("%d.erofs", size))
		if out, err := exec.Command(mkfs, fmt.Sprintf("-b%d", size), img, src).CombinedOutput(); err != nil {
			t.Skipf("mkfs.erofs -b%d: %v: %s", size, err, out)
		}

		f, err := os.Open(img)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		fsys, err := Open(f)
		require.NoError(t, err, "block size %d", size)
		require.Equal(t, uint32(size), fsys.image.BlockSize())
		require.NotZero(t, fsys.image.sb.FeatureCompat&EROFS_FEATURE_COMPAT_SB_CHKSUM, "block size %d", size)

		got, err := fs.ReadFile(fsys, "f")
		require.NoError(t, err, "block size %d", size)
		require.Equal(t, data, got, "block size %d", size)
		for name, target := range targets {
			got, err := fsys.ReadLink(name)
			require.NoError(t, err, "block size %d, %s", size, name)
			require.Equal(t, target, got, "block size %d, %s", size, name)
		}
	}
}
