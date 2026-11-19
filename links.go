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
	"fmt"
	"io/fs"
)

// HardLinks finds the paths of a file system that are hard links to the same
// inode. Like the Linux initramfs unpacker, it matches files that are not
// directories and have more than one link by device, inode number and type.
// The zero value is ready to use.
type HardLinks struct {
	first map[linkKey]hardLink
}

type linkKey struct {
	dev, ino uint64
	typ      fs.FileMode
}

type hardLink struct {
	name   string
	mode   fs.FileMode
	size   int64
	target string
}

// Add adds the path name of fsys, with the file info fi. It returns the first
// path that was added with the inode of name, which is name itself for the
// first one, or "" if name is not a hard link. It returns an error if name and
// the first path differ in mode, size or symbolic link target.
func (h *HardLinks) Add(fsys fs.FS, name string, fi fs.FileInfo) (string, error) {
	sys := fi.Sys()
	ino := GetIno(sys)
	if fi.IsDir() || ino == 0 || GetNlink(sys) < 2 {
		return "", nil
	}

	link := hardLink{name: name, mode: fi.Mode(), size: fi.Size()}
	if fi.Mode()&fs.ModeSymlink != 0 {
		var err error
		if link.target, err = fs.ReadLink(fsys, name); err != nil {
			return "", err
		}
	}

	key := linkKey{GetDev(sys), ino, fi.Mode().Type()}
	first, ok := h.first[key]
	if !ok {
		if h.first == nil {
			h.first = map[linkKey]hardLink{}
		}
		h.first[key] = link
		return name, nil
	}

	if name == first.name {
		return "", fmt.Errorf("%s listed twice", name)
	}
	// A source that merges file systems can give one inode number to different files.
	if link.mode != first.mode || link.size != first.size || link.target != first.target {
		return "", fmt.Errorf("inode of %s, but a different mode, size or target", first.name)
	}

	return first.name, nil
}
