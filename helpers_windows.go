//go:build windows

// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package archivefs

import "archive/tar"

// GetIno returns the inode number from a FileInfo.Sys() value.
// It supports *tar.Header (always 0) and any type implementing [InodeInfo].
func GetIno(sys any) uint64 {
	switch v := sys.(type) {
	case InodeInfo:
		return v.GetIno()
	case *tar.Header:
		return 0
	}
	return 0
}

// GetNlink returns the hard-link count from a FileInfo.Sys() value.
// It supports *tar.Header (always 1) and any type implementing [InodeInfo].
func GetNlink(sys any) uint64 {
	switch v := sys.(type) {
	case InodeInfo:
		return v.GetNlink()
	case *tar.Header:
		return 1
	}
	return 1
}

// GetUID returns the owner user ID from a FileInfo.Sys() value.
// It supports *tar.Header and any type implementing [OwnerInfo].
func GetUID(sys any) int {
	switch v := sys.(type) {
	case OwnerInfo:
		return v.GetUID()
	case *tar.Header:
		return v.Uid
	}
	return 0
}

// GetGID returns the owner group ID from a FileInfo.Sys() value.
// It supports *tar.Header and any type implementing [OwnerInfo].
func GetGID(sys any) int {
	switch v := sys.(type) {
	case OwnerInfo:
		return v.GetGID()
	case *tar.Header:
		return v.Gid
	}
	return 0
}

// GetDevMajor returns the major device number from a FileInfo.Sys() value.
// It supports *tar.Header and any type implementing [DevInfo].
// Returns 0 for non-device files.
func GetDevMajor(sys any) uint32 {
	switch v := sys.(type) {
	case DevInfo:
		return v.GetDevMajor()
	case *tar.Header:
		if v.Devmajor < 0 {
			return 0
		}
		return uint32(v.Devmajor)
	}
	return 0
}

// GetDevMinor returns the minor device number from a FileInfo.Sys() value.
// It supports *tar.Header and any type implementing [DevInfo].
// Returns 0 for non-device files.
func GetDevMinor(sys any) uint32 {
	switch v := sys.(type) {
	case DevInfo:
		return v.GetDevMinor()
	case *tar.Header:
		if v.Devminor < 0 {
			return 0
		}
		return uint32(v.Devminor)
	}
	return 0
}
