//go:build !windows && !linux

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

// GetDevMajor returns the major device number from a FileInfo.Sys() value.
// It supports *tar.Header and any type implementing [DevInfo].
// Returns 0 for non-device files.
//
// Note: *syscall.Stat_t is not handled here because the dev_t bit layout
// differs between Unix platforms (e.g. Darwin, FreeBSD). Add a
// platform-specific file (e.g. helpers_darwin.go) to support Stat_t on
// additional targets.
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
//
// Note: *syscall.Stat_t is not handled here because the dev_t bit layout
// differs between Unix platforms (e.g. Darwin, FreeBSD). Add a
// platform-specific file (e.g. helpers_darwin.go) to support Stat_t on
// additional targets.
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
