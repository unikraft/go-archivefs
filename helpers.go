// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// Package archivefs provides common interfaces and helpers for working
// with filesystem metadata across different archive and filesystem types.
package archivefs

import (
	"archive/tar"
	"maps"
	"slices"
	"strings"
)

// InodeInfo is implemented by FileInfo.Sys() values that expose inode
// and hard-link metadata.
type InodeInfo interface {
	// GetIno returns the inode number.
	GetIno() uint64
	// GetNlink returns the number of hard links.
	GetNlink() uint64
}

// OwnerInfo is implemented by FileInfo.Sys() values that expose file
// ownership metadata.
type OwnerInfo interface {
	// GetUID returns the user ID of the file owner.
	GetUID() int
	// GetGID returns the group ID of the file owner.
	GetGID() int
}

// DevInfo is implemented by FileInfo.Sys() values that expose device numbers
// for special files (block/character devices).
type DevInfo interface {
	// GetDevMajor returns the major device number.
	GetDevMajor() uint32
	// GetDevMinor returns the minor device number.
	GetDevMinor() uint32
}

// XattrInfo is implemented by FileInfo.Sys() values that expose extended attributes.
type XattrInfo interface {
	// GetXattrs returns the raw values of the extended attributes by full name.
	GetXattrs() (map[string]string, error)
}

// paxXattrPrefix is the prefix of the PAX records that hold extended attributes.
const paxXattrPrefix = "SCHILY.xattr."

// GetXattrs returns the extended attributes of a *tar.Header or an [XattrInfo].
func GetXattrs(sys any) (map[string]string, error) {
	switch v := sys.(type) {
	case XattrInfo:
		return v.GetXattrs()
	case *tar.Header:
		return tarXattrs(v), nil
	}
	return nil, nil
}

// tarXattrs returns the extended attributes in the PAX records and the deprecated Xattrs of hdr.
func tarXattrs(hdr *tar.Header) map[string]string {
	// Like in archive/tar, Xattrs[k] is the same as PAXRecords["SCHILY.xattr."+k], and Xattrs take precedence.
	raw := map[string]string{}
	for k, v := range hdr.PAXRecords {
		if name, ok := strings.CutPrefix(k, paxXattrPrefix); ok {
			raw[name] = v
		}
	}
	maps.Copy(raw, hdr.Xattrs) //nolint:staticcheck // archive/tar still writes the deprecated Xattrs.
	if len(raw) == 0 {
		return nil
	}

	// The keys are sorted, so that keys that decode to the same name always give the same value.
	xattrs := make(map[string]string, len(raw))
	for _, k := range slices.Sorted(maps.Keys(raw)) {
		xattrs[xattrNameUnescaper.Replace(k)] = raw[k]
	}
	return xattrs
}

// SetTarXattrs replaces the extended attributes of hdr with xattrs, as PAX records with names encoded like GNU tar.
func SetTarXattrs(hdr *tar.Header, xattrs map[string]string) {
	hdr.Xattrs = nil //nolint:staticcheck // The PAX records replace the deprecated Xattrs.
	maps.DeleteFunc(hdr.PAXRecords, func(k, _ string) bool { return strings.HasPrefix(k, paxXattrPrefix) })
	for name, value := range xattrs {
		if hdr.PAXRecords == nil {
			hdr.PAXRecords = map[string]string{}
		}
		hdr.PAXRecords[paxXattrPrefix+xattrNameEscaper.Replace(name)] = value
	}
}

// xattrNameEscaper encodes the "%" and "=" that a PAX key cannot hold, like GNU tar.
var xattrNameEscaper = strings.NewReplacer("%", "%25", "=", "%3D")

// xattrNameUnescaper decodes the escapes of xattrNameEscaper, like GNU tar. Other tools, such as archive/tar, do not escape names.
var xattrNameUnescaper = strings.NewReplacer("%25", "%", "%3D", "=")
