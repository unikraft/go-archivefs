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
