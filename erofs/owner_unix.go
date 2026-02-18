//go:build !windows
// +build !windows

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
	"syscall"
)

func getOwner(fi fs.FileInfo) (uid, gid int) {
	switch sys := fi.Sys().(type) {
	case *syscall.Stat_t:
		uid = int(sys.Uid)
		gid = int(sys.Gid)
	case *tar.Header:
		uid = sys.Uid
		gid = sys.Gid
	case *Inode:
		uid = int(sys.UID())
		gid = int(sys.GID())
	}

	return
}

func getNLinks(fi fs.FileInfo) int {
	switch fi.Sys().(type) {
	case *syscall.Stat_t:
		stat := fi.Sys().(*syscall.Stat_t)

		return int(stat.Nlink)
	case *Inode:
		inode := fi.Sys().(*Inode)

		return int(inode.Nlink())
	}

	return 1
}

func getIno(fi fs.FileInfo) uint64 {
	switch fi.Sys().(type) {
	case *syscall.Stat_t:
		stat := fi.Sys().(*syscall.Stat_t)

		return stat.Ino
	case *Inode:
		inode := fi.Sys().(*Inode)

		return inode.Nid()
	}

	return 0
}
