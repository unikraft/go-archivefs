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

func TestInlineDataBoundary(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "img"))
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, Create(f, fstest.MapFS{}))
	img, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	image, err := OpenImage(bytes.NewReader(img))
	require.NoError(t, err)
	metaOff := image.sb.NidToOffset(0)

	// place writes a symlink inode with xattrs and an inline target at off in a new block, and returns its nid.
	place := func(target string, xattrs []byte, off int64) uint64 {
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
		pos := int64(len(img)) + off
		img = append(img, make([]byte, roundUp(off+int64(b.Len()), BlockSize))...)
		copy(img[pos:], b.Bytes())
		return uint64(pos-metaOff) / InodeSlotSize
	}
	xattrs := concat(ibodyHeader(0), xattrEntry(XattrIndexUser, "a", "x"))
	tailSize := BlockSize - binary.Size(InodeExtended{}) - len(xattrs)
	cases := []struct {
		name    string
		target  string
		xattrs  []byte
		off     int64
		crosses bool
	}{
		{"inode ends in the next block", strings.Repeat("a", BlockSize-32), nil, BlockSize - InodeSlotSize, false},
		{"inode in the middle of a block", strings.Repeat("b", BlockSize/2), nil, BlockSize / 2, true},
		{"after the xattrs, to the end of the block", strings.Repeat("c", tailSize), xattrs, 0, false},
		{"after the xattrs, after the end of the block", strings.Repeat("d", tailSize+1), xattrs, 0, true},
		{"empty", "", nil, 0, false},
	}
	nids := make([]uint64, len(cases))
	for i, tc := range cases {
		nids[i] = place(tc.target, tc.xattrs, tc.off)
	}

	image, err = OpenImage(bytes.NewReader(img))
	require.NoError(t, err)
	for i, tc := range cases {
		ino, err := image.Inode(nids[i])
		if tc.crosses {
			require.ErrorContains(t, err, "cross block boundary", tc.name)
			continue
		}
		require.NoError(t, err, tc.name)
		target, err := ino.Readlink()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.target, target, tc.name)
		got, err := ino.GetXattrs()
		require.NoError(t, err, tc.name)
		if tc.xattrs != nil {
			require.Equal(t, map[string]string{"user.a": "x"}, got, tc.name)
		}
	}
}
