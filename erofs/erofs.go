// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions of this file are based on code originally from: github.com/google/gvisor
 *
 * Copyright 2023 The gVisor Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package erofs

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	_ fs.FS         = (*Filesystem)(nil)
	_ fs.ReadDirFS  = (*Filesystem)(nil)
	_ fs.StatFS     = (*Filesystem)(nil)
	_ fs.ReadLinkFS = (*Filesystem)(nil)
)

type Filesystem struct {
	image *Image
	root  *dirEntry
}

func Open(src io.ReaderAt) (*Filesystem, error) {
	image := &Image{src: src}

	if err := image.initSuperBlock(); err != nil {
		return nil, err
	}

	return &Filesystem{
		image: image,
		root: &dirEntry{
			image: image,
			nid:   image.RootNid(),
			typ:   FT_DIR,
		},
	}, nil
}

func (fsys *Filesystem) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}

	de, err := fsys.resolve(name, false)
	if err != nil {
		return nil, err
	}

	return &file{
		de: de,
	}, nil
}

func (fsys *Filesystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}

	de, err := fsys.resolve(name, false)
	if err != nil {
		return nil, err
	}

	if !de.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: errors.New("not a directory")}
	}

	ino := de.getInode()
	if de.inodeErr != nil {
		return nil, de.inodeErr
	}

	var dirents []fs.DirEntry
	err = ino.IterDirents(func(name string, typ uint8, nid uint64) error {
		// Skip "." and ".." entries.
		if name == "." || name == ".." {
			return nil
		}

		dirents = append(dirents, &dirEntry{
			image: de.image,
			name:  name,
			nid:   nid,
			typ:   typ,
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return dirents, nil
}

func (fsys *Filesystem) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}

	de, err := fsys.resolve(name, false)
	if err != nil {
		return nil, err
	}

	ino := de.getInode()
	if de.inodeErr != nil {
		return nil, de.inodeErr
	}

	return &fileInfo{
		image: de.image,
		name:  de.name,
		inode: ino,
	}, nil
}

// ReadLink returns the destination of the named symbolic link.
// Experimental implementation of: https://github.com/golang/go/issues/49580
func (fsys *Filesystem) ReadLink(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}

	de, err := fsys.resolve(name, true)
	if err != nil {
		return "", err
	}

	ino := de.getInode()
	if de.inodeErr != nil {
		return "", de.inodeErr
	}

	return ino.Readlink()
}

func (fsys *Filesystem) Lstat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrInvalid}
	}

	de, err := fsys.resolve(name, true)
	if err != nil {
		return nil, err
	}

	ino := de.getInode()
	if de.inodeErr != nil {
		return nil, de.inodeErr
	}

	return &fileInfo{
		image: de.image,
		name:  de.name,
		inode: ino,
	}, nil
}

// StatLink returns a FileInfo describing the file without following any symbolic links.
// Experimental implementation of: https://github.com/golang/go/issues/49580
func (fsys *Filesystem) StatLink(name string) (fs.FileInfo, error) {
	return fsys.Lstat(name)
}

// maxSymlinks is the maximum number of symlink resolutions allowed during
// a single path resolution, matching the Linux kernel limit (MAXSYMLINKS).
const maxSymlinks = 40

func (fsys *Filesystem) resolve(name string, noResolveLastSymlink bool) (*dirEntry, error) {
	return fsys.resolveDepth(name, noResolveLastSymlink, maxSymlinks)
}

func (fsys *Filesystem) resolveDepth(name string, noResolveLastSymlink bool, remaining int) (*dirEntry, error) {
	de := fsys.root

	components := splitPath(name)
	for i, comp := range components {
		child, err := de.lookup(comp)
		if err != nil {
			return nil, err
		}

		ino := child.getInode()
		if child.inodeErr != nil {
			return nil, child.inodeErr
		}

		if ino.IsSymlink() && !(noResolveLastSymlink && i == len(components)-1) {
			if remaining <= 0 {
				return nil, errors.New("too many levels of symbolic links")
			}

			link, err := ino.Readlink()
			if err != nil {
				return nil, err
			}
			link = filepath.Clean(link)

			if strings.HasPrefix(link, "/") {
				link = strings.TrimPrefix(link, "/")
			} else {
				link = filepath.Join(strings.Join(components[:i], "/"), link)
			}

			child, err = fsys.resolveDepth(link, noResolveLastSymlink, remaining-1)
			if err != nil {
				return nil, err
			}
		}

		de = child
	}
	return de, nil
}

type file struct {
	de *dirEntry
	r  io.Reader
}

func (f *file) Read(p []byte) (int, error) {
	if f.r == nil {
		ino := f.de.getInode()
		if f.de.inodeErr != nil {
			return 0, f.de.inodeErr
		}

		var err error
		f.r, err = ino.Data()
		if err != nil {
			return 0, err
		}
	}

	return f.r.Read(p)
}

func (f *file) Close() error {
	return nil
}

func (f *file) Stat() (fs.FileInfo, error) {
	return f.de.Info()
}

type dirEntry struct {
	image         *Image
	name          string
	typ           uint8
	nid           uint64
	readInodeOnce sync.Once
	inode         *Inode
	inodeErr      error
}

func (de *dirEntry) Name() string {
	return de.name
}

func (de *dirEntry) IsDir() bool {
	return de.typ == FT_DIR
}

func (de *dirEntry) Type() fs.FileMode {
	switch de.typ {
	case FT_DIR:
		return fs.ModeDir
	case FT_SYMLINK:
		return fs.ModeSymlink
	case FT_CHRDEV:
		return fs.ModeDevice | fs.ModeCharDevice
	case FT_BLKDEV:
		return fs.ModeDevice
	case FT_FIFO:
		return fs.ModeNamedPipe
	case FT_SOCK:
		return fs.ModeSocket
	default:
		return 0
	}
}

func (de *dirEntry) Info() (fs.FileInfo, error) {
	ino := de.getInode()
	if de.inodeErr != nil {
		return nil, de.inodeErr
	}

	return &fileInfo{
		image: de.image,
		name:  de.name,
		inode: ino,
	}, nil
}

func (de *dirEntry) lookup(name string) (*dirEntry, error) {
	ino := de.getInode()
	if de.inodeErr != nil {
		return nil, de.inodeErr
	}

	d, err := ino.Lookup(name)
	if err != nil {
		return nil, err
	}

	return &dirEntry{
		image: de.image,
		name:  name,
		nid:   d.Nid,
		typ:   d.FileType,
	}, nil
}

func (de *dirEntry) getInode() Inode {
	de.readInodeOnce.Do(func() {
		ino, err := de.image.Inode(de.nid)
		if err != nil {
			de.inodeErr = err
			return
		}
		de.inode = &ino
	})

	if de.inode == nil {
		return Inode{}
	}
	return *de.inode
}

type fileInfo struct {
	image *Image
	name  string
	inode Inode
}

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return int64(fi.inode.Size())
}

func (fi *fileInfo) Mode() fs.FileMode {
	return fi.inode.Mode()
}

func (fi *fileInfo) ModTime() time.Time {
	return time.Unix(int64(fi.inode.Mtime()), int64(fi.inode.MtimeNsec()))
}

func (fi *fileInfo) IsDir() bool {
	return fi.inode.IsDir()
}

func (fi *fileInfo) Sys() any {
	return &fi.inode
}

func splitPath(path string) []string {
	var components []string
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part != "" {
			components = append(components, part)
		}
	}
	return components
}
