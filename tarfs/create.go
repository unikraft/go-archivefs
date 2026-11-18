// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package tarfs

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/unikraft/go-archivefs"
)

// CreateOption is an option for Create.
type CreateOption func(*createOptions)

type createOptions struct {
	root bool
}

// WithRootDir writes the root directory as the entry "./", with its owner,
// mode, modification time and extended attributes.
func WithRootDir(root bool) CreateOption {
	return func(o *createOptions) {
		o.root = root
	}
}

// linkKey identifies the inode of a hard link by device, inode number and type.
type linkKey struct {
	dev, ino uint64
	typ      fs.FileMode
}

// Create writes src to dst as a tar archive with owners, devices, xattrs and
// hard links, without the root and sockets.
func Create(dst io.Writer, src fs.FS, opts ...CreateOption) error {
	var o createOptions
	for _, opt := range opts {
		opt(&o)
	}

	tw := tar.NewWriter(dst)
	links := map[linkKey]*tar.Header{}

	err := fs.WalkDir(src, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// A tar archive has an implicit root directory, and a root entry can confuse unarchivers.
		if path == "." && !o.root {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			return err
		}
		if fi.Mode()&fs.ModeSocket != 0 {
			return nil
		}

		var link string
		if d.Type()&fs.ModeSymlink != 0 {
			linkFS, ok := src.(fs.ReadLinkFS)
			if !ok {
				return errors.New("source FS does not support symlinks")
			}

			link, err = linkFS.ReadLink(path)
			if err != nil {
				return err
			}
		}

		hdr, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		hdr.Name = path
		if path == "." {
			hdr.Name = "./"
		}

		sys := fi.Sys()
		hdr.Uid, hdr.Gid = archivefs.GetUID(sys), archivefs.GetGID(sys)
		// FileInfoHeader sets the device numbers of a *syscall.Stat_t on more systems than archivefs.
		if (hdr.Typeflag == tar.TypeChar || hdr.Typeflag == tar.TypeBlock) && hdr.Devmajor == 0 && hdr.Devminor == 0 {
			hdr.Devmajor, hdr.Devminor = int64(archivefs.GetDevMajor(sys)), int64(archivefs.GetDevMinor(sys))
		}
		xattrs, err := archivefs.GetXattrs(sys)
		if err != nil {
			return err
		}
		archivefs.SetTarXattrs(hdr, xattrs)

		// The first path of an inode with more than one link holds the file. The other paths are links to it.
		if ino := archivefs.GetIno(sys); !d.IsDir() && ino != 0 && archivefs.GetNlink(sys) > 1 {
			key := linkKey{archivefs.GetDev(sys), ino, fi.Mode().Type()}
			if first, ok := links[key]; ok {
				// A source that merges file systems can give one inode number to different files.
				if hdr.Mode != first.Mode || hdr.Size != first.Size || hdr.Linkname != first.Linkname {
					return &fs.PathError{Op: "create", Path: path, Err: fmt.Errorf("inode of %s, but a different mode, size or target", first.Name)}
				}
				hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeLink, first.Name, 0
				return tw.WriteHeader(hdr)
			}
			links[key] = hdr
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if !d.Type().IsRegular() {
			return nil
		}

		f, err := src.Open(path)
		if err != nil {
			return err
		}

		_, err = io.Copy(tw, f)
		_ = f.Close()
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return tw.Close()
}
