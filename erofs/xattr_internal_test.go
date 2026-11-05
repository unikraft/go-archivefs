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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadXattrs(t *testing.T) {
	header := func(shared byte) []byte { return []byte{0, 0, 0, 0, shared, 0, 0, 0, 0, 0, 0, 0} }
	entry := func(index byte, name, value string) []byte {
		e := append([]byte{byte(len(name)), index, byte(len(value)), 0}, name+value...)
		return append(e, make([]byte, int(roundUp(int64(len(e)), xattrEntrySize))-len(e))...)
	}
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

	for _, tt := range []struct {
		name string
		area []byte
		want map[string]string
		err  string
	}{
		{"header only", header(0), nil, "not supported"},
		{"entries", cat(header(0), entry(XattrIndexUser, "a", "x"), entry(XattrIndexSecurity, "capability", "c")),
			map[string]string{"user.a": "x", "security.capability": "c"}, ""},
		{"unknown and long prefix indexes", cat(header(0), entry(5, "a", "x"), entry(0x81, "b", "y"), entry(XattrIndexUser, "c", "z")),
			map[string]string{"user.c": "z"}, ""},
		{"too many shared xattrs", cat(header(3), entry(XattrIndexUser, "a", "x")), nil, "shared xattrs do not fit"},
		{"entry past the area", cat(header(0), entry(XattrIndexUser, "a", "0123456789"))[:xattrIbodyHeaderSize+8], nil, "ends after the xattr area"},
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
