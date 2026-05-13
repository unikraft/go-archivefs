//go:build linux

// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package archivefs

import (
	"archive/tar"
	"syscall"
)

// GetDevMajor returns the major device number from a FileInfo.Sys() value.
// It supports *syscall.Stat_t (Linux dev_t encoding), *tar.Header, and any
// type implementing [DevInfo]. Returns 0 for non-device files.
func GetDevMajor(sys any) uint32 {
	switch v := sys.(type) {
	case *syscall.Stat_t:
		// Linux dev_t encoding (glibc <sys/sysmacros.h>):
		// major = bits[8:20] | bits[32:]
		rdev := v.Rdev
		return uint32(((rdev >> 8) & 0xfff) | ((rdev >> 32) &^ uint64(0xfff)))
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
// It supports *syscall.Stat_t (Linux dev_t encoding), *tar.Header, and any
// type implementing [DevInfo]. Returns 0 for non-device files.
func GetDevMinor(sys any) uint32 {
	switch v := sys.(type) {
	case *syscall.Stat_t:
		// Linux dev_t encoding (glibc <sys/sysmacros.h>):
		// minor = bits[0:8] | bits[20:32]
		rdev := v.Rdev
		return uint32((rdev & 0xff) | ((rdev >> 12) &^ uint64(0xff)))
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
