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
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
)

// Name indexes of the extended attribute name prefixes that Linux supports.
const (
	XattrIndexUser            = 1
	XattrIndexPosixACLAccess  = 2
	XattrIndexPosixACLDefault = 3
	XattrIndexTrusted         = 4
	XattrIndexSecurity        = 6
)

const (
	// xattrIbodyHeaderSize is the size of the header of the inline xattr area.
	xattrIbodyHeaderSize = 12

	// xattrEntrySize is the size of an entry header and the alignment of entries.
	xattrEntrySize = 4

	// xattrNameMax is the maximum length of a full xattr name in Linux.
	xattrNameMax = 255
)

// xattrPrefix is the name prefix of a name index. An exact prefix is a full name.
type xattrPrefix struct {
	index  uint8
	prefix string
	exact  bool
}

var xattrPrefixes = []xattrPrefix{
	{XattrIndexUser, "user.", false},
	{XattrIndexPosixACLAccess, "system.posix_acl_access", true},
	{XattrIndexPosixACLDefault, "system.posix_acl_default", true},
	{XattrIndexTrusted, "trusted.", false},
	{XattrIndexSecurity, "security.", false},
}

// splitXattrName returns the name index and the rest of a full xattr name that Linux supports.
func splitXattrName(name string) (uint8, string, bool) {
	if len(name) > xattrNameMax {
		return 0, "", false
	}
	for _, p := range xattrPrefixes {
		if suffix, ok := strings.CutPrefix(name, p.prefix); ok && (suffix == "") == p.exact {
			return p.index, suffix, true
		}
	}
	return 0, "", false
}

// xattrPrefixOf returns the name prefix of a name index.
func xattrPrefixOf(index uint8) (string, bool) {
	for _, p := range xattrPrefixes {
		if p.index == index {
			return p.prefix, true
		}
	}
	return "", false
}

// encodeXattrs returns the inline xattr area for the xattrs that Linux supports, or nil if there are none.
func encodeXattrs(xattrs map[string]string) ([]byte, error) {
	type entry struct {
		index       uint8
		name, value string
	}
	var entries []entry
	for name, value := range xattrs {
		// Linux cannot show other names, so the image does not keep them.
		index, suffix, ok := splitXattrName(name)
		if !ok {
			continue
		}
		if len(value) > math.MaxUint16 {
			return nil, fmt.Errorf("value of xattr %q is longer than %d bytes", name, math.MaxUint16)
		}
		entries = append(entries, entry{index, suffix, value})
	}
	if len(entries) == 0 {
		return nil, nil
	}
	slices.SortFunc(entries, func(a, b entry) int {
		return cmp.Or(cmp.Compare(a.index, b.index), strings.Compare(a.name, b.name))
	})

	// The header has no shared xattrs and a zero name filter, which marks all names as present.
	buf := make([]byte, xattrIbodyHeaderSize)
	for _, e := range entries {
		buf = append(buf, uint8(len(e.name)), e.index)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(e.value)))
		buf = append(buf, e.name...)
		buf = append(buf, e.value...)
		buf = append(buf, make([]byte, roundUp(int64(len(buf)), xattrEntrySize)-int64(len(buf)))...)
	}

	if int64(len(buf)) > xattrIbodySize(math.MaxUint16) {
		return nil, fmt.Errorf("xattrs take %d bytes, more than an inode can hold", len(buf))
	}

	return buf, nil
}

// xattrCount returns the xattr count field of an inode for its inline xattr area.
func xattrCount(ibody []byte) uint16 {
	if len(ibody) == 0 {
		return 0
	}
	return uint16((len(ibody)-xattrIbodyHeaderSize)/xattrEntrySize + 1)
}

// xattrIbodySize returns the size of the inline xattr area for an xattr count.
func xattrIbodySize(count uint16) int64 {
	if count == 0 {
		return 0
	}
	return xattrIbodyHeaderSize + xattrEntrySize*(int64(count)-1)
}

// readXattrs reads the inline xattr area of size bytes at off, and the shared xattrs it refers to.
func (i *Image) readXattrs(off, size int64) (map[string]string, error) {
	// Like Linux, refuse an area with only the header.
	if size == xattrIbodyHeaderSize {
		return nil, fmt.Errorf("xattr area of %d bytes is not supported", size)
	}
	buf, err := i.bytesAt(off, size)
	if err != nil {
		return nil, err
	}

	xattrs := map[string]string{}

	pos := int64(xattrIbodyHeaderSize)
	shared := int64(buf[4])
	if pos+shared*xattrEntrySize > size {
		return nil, fmt.Errorf("%d shared xattrs do not fit in the xattr area", shared)
	}
	for range shared {
		id := binary.LittleEndian.Uint32(buf[pos:])
		pos += xattrEntrySize

		eoff := i.sb.BlockAddrToOffset(i.sb.XattrBlockAddr) + int64(id)*xattrEntrySize
		hdr, err := i.bytesAt(eoff, xattrEntrySize)
		if err != nil {
			return nil, fmt.Errorf("shared xattr %d: %w", id, err)
		}
		nameLen, valueLen := int64(hdr[0]), int64(binary.LittleEndian.Uint16(hdr[2:]))
		body, err := i.bytesAt(eoff+xattrEntrySize, nameLen+valueLen)
		if err != nil {
			return nil, fmt.Errorf("shared xattr %d: %w", id, err)
		}
		addXattr(xattrs, hdr[1], body[:nameLen], body[nameLen:])
	}

	for pos+xattrEntrySize <= size {
		nameLen, valueLen := int64(buf[pos]), int64(binary.LittleEndian.Uint16(buf[pos+2:]))
		start := pos + xattrEntrySize
		end := start + nameLen + valueLen
		if end > size {
			return nil, fmt.Errorf("xattr entry at %d ends after the xattr area", pos)
		}
		addXattr(xattrs, buf[pos+1], buf[start:start+nameLen], buf[start+nameLen:end])
		pos = roundUp(end, xattrEntrySize)
	}

	return xattrs, nil
}

// addXattr adds an xattr entry to xattrs. Like Linux, it skips unknown name indexes.
func addXattr(xattrs map[string]string, index uint8, name, value []byte) {
	if prefix, ok := xattrPrefixOf(index); ok {
		xattrs[prefix+string(name)] = string(value)
	}
}
