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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func ibodyHeader(shared byte) []byte { return []byte{0, 0, 0, 0, shared, 0, 0, 0, 0, 0, 0, 0} }

func xattrEntry(index byte, name, value string) []byte {
	e := append([]byte{byte(len(name)), index, byte(len(value)), 0}, name+value...)
	return append(e, make([]byte, int(roundUp(int64(len(e)), xattrEntrySize))-len(e))...)
}

func concat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestReadXattrs(t *testing.T) {
	for _, tt := range []struct {
		name string
		area []byte
		want map[string]string
		err  string
	}{
		{"header only", ibodyHeader(0), nil, "not supported"},
		{"entries", concat(ibodyHeader(0), xattrEntry(XattrIndexUser, "a", "x"), xattrEntry(XattrIndexSecurity, "capability", "c")),
			map[string]string{"user.a": "x", "security.capability": "c"}, ""},
		{"unknown and long prefix indexes", concat(ibodyHeader(0), xattrEntry(5, "a", "x"), xattrEntry(0x81, "b", "y"), xattrEntry(XattrIndexUser, "c", "z")),
			map[string]string{"user.c": "z"}, ""},
		{"too many shared xattrs", concat(ibodyHeader(3), xattrEntry(XattrIndexUser, "a", "x")), nil, "shared xattrs do not fit"},
		{"entry past the area", concat(ibodyHeader(0), xattrEntry(XattrIndexUser, "a", "0123456789"))[:xattrIbodyHeaderSize+8], nil, "ends after the xattr area"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			size := int64(len(tt.area))
			if r := size % xattrEntrySize; r != 0 {
				tt.area = append(tt.area, make([]byte, xattrEntrySize-r)...)
				size += xattrEntrySize - r
			}
			img := &Image{src: bytes.NewReader(tt.area)}
			got, err := img.readXattrs(0, size)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReadSharedXattrs(t *testing.T) {
	// The shared xattr area starts at block 1, and the inode uses the shared xattr with id 1.
	area := concat(ibodyHeader(1), binary.LittleEndian.AppendUint32(nil, 1), xattrEntry(XattrIndexUser, "a", "x"))
	shared := concat(make([]byte, xattrEntrySize), xattrEntry(XattrIndexTrusted, "s", "y"))
	src := concat(area, make([]byte, BlockSize-len(area)), shared)

	img := &Image{src: bytes.NewReader(src), sb: SuperBlock{BlockSizeBits: BlockSizeBits, XattrBlockAddr: 1}}
	got, err := img.readXattrs(0, int64(len(area)))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"user.a": "x", "trusted.s": "y"}, got)
}

func TestXattrsInlineDataBoundary(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "img"))
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, Create(f, fstest.MapFS{}))
	img, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	image, err := OpenImage(bytes.NewReader(img))
	require.NoError(t, err)
	metaOff := image.sb.NidToOffset(0)

	// place writes a symlink inode with xattrs and an inline target in a new block, and returns its nid.
	xattrs := concat(ibodyHeader(0), xattrEntry(XattrIndexUser, "a", "x"))
	place := func(target string) uint64 {
		ino := InodeExtended{
			Format: setBits(setBits(0, InodeLayoutExtended, InodeLayoutBit, InodeLayoutBits),
				InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits),
			XattrCount: xattrCount(xattrs),
			Mode:       S_IFLNK | 0o777,
			Size:       uint64(len(target)),
			Nlink:      1,
		}
		var b bytes.Buffer
		require.NoError(t, binary.Write(&b, binary.LittleEndian, &ino))
		b.Write(xattrs)
		b.WriteString(target)
		pos := int64(len(img))
		img = append(img, make([]byte, roundUp(int64(b.Len()), BlockSize))...)
		copy(img[pos:], b.Bytes())
		return uint64(pos-metaOff) / InodeSlotSize
	}
	// The target follows the inode and its xattrs, and ends at the end of the block or after it.
	tailSize := BlockSize - binary.Size(InodeExtended{}) - len(xattrs)
	fits := place(strings.Repeat("c", tailSize))
	crosses := place(strings.Repeat("d", tailSize+1))

	image, err = OpenImage(bytes.NewReader(img))
	require.NoError(t, err)
	ino, err := image.Inode(fits)
	require.NoError(t, err)
	target, err := ino.Readlink()
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("c", tailSize), target)
	got, err := ino.GetXattrs()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"user.a": "x"}, got)

	_, err = image.Inode(crosses)
	require.ErrorContains(t, err, "cross block boundary")
}
