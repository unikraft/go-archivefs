//go:build windows
// +build windows

// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package erofs

import (
	"archive/tar"
	"io/fs"
)

func getOwner(fi fs.FileInfo) (uid, gid int) {
	switch fi.Sys().(type) {
	case *tar.Header:
		hdr := fi.Sys().(*tar.Header)

		uid = hdr.Uid
		gid = hdr.Gid
	}

	return
}

func getNLinks(fi fs.FileInfo) int {
	switch fi.Sys().(type) {
	case *Inode:
		inode := fi.Sys().(*Inode)

		return int(inode.Nlink())
	}

	return 1
}

func getIno(fi fs.FileInfo) uint64 {
	switch fi.Sys().(type) {
	case *Inode:
		inode := fi.Sys().(*Inode)

		return inode.Nid()
	}

	return 0
}
