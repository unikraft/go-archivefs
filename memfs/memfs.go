// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions of this file are based on code originally from:
 * github.com/psanford/memfs
 *
 * Copyright (c) 2021 The memfs Authors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 * * Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package memfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	syspath "path"
	"slices"
	"strings"
	"sync"
	"time"
)

// FS is an in-memory filesystem that implements
// io/fs.FS
type FS struct {
	dir *dir
}

// New creates a new in-memory FileSystem.
func New() *FS {
	return &FS{
		dir: &dir{
			children: make(map[string]childI),
		},
	}
}

// MkdirAll creates a directory named path,
// along with any necessary parents, and returns nil,
// or else returns an error.
// The permission bits perm (before umask) are used for all
// directories that MkdirAll creates.
// If path is already a directory, MkdirAll does nothing
// and returns nil.
func (rootFS *FS) MkdirAll(path string, perm os.FileMode) error {
	if !fs.ValidPath(path) {
		return fmt.Errorf("invalid path: %s: %w", path, fs.ErrInvalid)
	}

	if path == "." {
		// root dir always exists
		return nil
	}

	parts := strings.Split(path, "/")

	next := rootFS.dir
	for i, part := range parts {
		cur := next
		cur.mu.Lock()
		child := cur.children[part]
		if child == nil {
			newDir := &dir{
				name:     part,
				perm:     perm,
				children: make(map[string]childI),
			}
			cur.children[part] = newDir
			next = newDir
		} else if childDir, ok := child.(*dir); ok {
			next = childDir
		} else if _, ok := child.(*symlink); ok {
			cur.mu.Unlock()
			// Resolve the path up to this point (following symlinks)
			// and continue creating from the resolved directory.
			partial := strings.Join(parts[:i+1], "/")
			resolved, err := rootFS.resolve(partial, false)
			if err != nil {
				return err
			}
			d, ok := resolved.(*dir)
			if !ok {
				return fmt.Errorf("not a directory: %s: %w", part, fs.ErrInvalid)
			}
			next = d
			continue
		} else {
			cur.mu.Unlock()
			return fmt.Errorf("not a directory: %s: %w", part, fs.ErrInvalid)
		}
		// Lock the child before unlocking the parent to prevent
		// concurrent MkdirAll calls from racing on the same path.
		next.mu.Lock()
		cur.mu.Unlock()
		next.mu.Unlock()
	}

	return nil
}

func (rootFS *FS) resolve(name string, noFollowLast bool) (childI, error) {
	return rootFS.resolveDepth(name, noFollowLast, maxSymlinks)
}

func (rootFS *FS) resolveDepth(name string, noFollowLast bool, remaining int) (childI, error) {
	if name == "" {
		return rootFS.dir, nil
	}

	components := strings.Split(name, "/")
	cur := rootFS.dir

	for i, comp := range components {
		cur.mu.Lock()
		child := cur.children[comp]
		cur.mu.Unlock()

		if child == nil {
			return nil, fmt.Errorf("no such file or directory: %s: %w", comp, fs.ErrNotExist)
		}

		isLast := i == len(components)-1

		switch c := child.(type) {
		case *symlink:
			if noFollowLast && isLast {
				return c, nil
			}
			if remaining <= 0 {
				return nil, errors.New("too many levels of symbolic links")
			}

			target := c.target
			if strings.HasPrefix(target, "/") {
				target = strings.TrimPrefix(target, "/")
			} else {
				// Relative: join with parent path.
				if i > 0 {
					parentPath := strings.Join(components[:i], "/")
					target = parentPath + "/" + target
				}
			}
			// Append remaining unresolved components.
			if !isLast {
				rest := strings.Join(components[i+1:], "/")
				target = target + "/" + rest
			}
			target = syspath.Clean(target)
			if target == "." {
				target = ""
			}
			return rootFS.resolveDepth(target, noFollowLast, remaining-1)

		case *dir:
			cur = c

		case *File:
			if isLast {
				return c, nil
			}
			return nil, fmt.Errorf("not a directory: %s: %w", comp, fs.ErrNotExist)

		default:
			return nil, fmt.Errorf("unexpected file type: %s: %w", comp, fs.ErrInvalid)
		}
	}

	return cur, nil
}

func (rootFS *FS) getDir(path string) (*dir, error) {
	child, err := rootFS.resolve(path, false)
	if err != nil {
		return nil, err
	}
	d, ok := child.(*dir)
	if !ok {
		return nil, fmt.Errorf("not a directory: %s: %w", path, fs.ErrInvalid)
	}
	return d, nil
}

func (rootFS *FS) get(path string) (childI, error) {
	return rootFS.resolve(path, false)
}

func (rootFS *FS) create(path string) (*File, error) {
	if !fs.ValidPath(path) {
		return nil, fmt.Errorf("invalid path: %s: %w", path, fs.ErrInvalid)
	}

	if path == "." {
		path = ""
	}

	// Resolve the full path (following all symlinks). If it resolves
	// to an existing File we can overwrite it directly.
	existing, err := rootFS.resolve(path, false)
	if err == nil {
		if f, ok := existing.(*File); ok {
			return f, nil
		}
		return nil, fmt.Errorf("path is a directory: %s: %w", path, fs.ErrExist)
	}

	// The path doesn't fully exist. Resolve the parent directory
	// (following symlinks) and create the file there.
	dirPart, filePart := syspath.Split(path)
	dirPart = strings.TrimSuffix(dirPart, "/")

	parent, err := rootFS.getDir(dirPart)
	if err != nil {
		return nil, err
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	// Double-check: another goroutine may have created it.
	if child := parent.children[filePart]; child != nil {
		if f, ok := child.(*File); ok {
			return f, nil
		}
		return nil, fmt.Errorf("path already exists: %s: %w", path, fs.ErrExist)
	}

	newFile := &File{
		name:    filePart,
		perm:    0666,
		content: &bytes.Buffer{},
	}
	parent.children[filePart] = newFile

	return newFile, nil
}

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm
// (before umask); otherwise WriteFile truncates it before writing, without changing permissions.
func (rootFS *FS) WriteFile(path string, data []byte, perm os.FileMode) error {
	if !fs.ValidPath(path) {
		return fmt.Errorf("invalid path: %s: %w", path, fs.ErrInvalid)
	}

	if path == "." {
		// root dir
		path = ""
	}

	f, err := rootFS.create(path)
	if err != nil {
		return err
	}
	f.content = bytes.NewBuffer(data)
	f.size = int64(len(data))
	f.perm = perm
	return nil
}

// Open opens the named file.
func (rootFS *FS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fs.ErrInvalid,
		}
	}

	isRoot := name == "."
	if isRoot {
		// root dir
		name = ""
	}

	child, err := rootFS.get(name)
	if err != nil {
		return nil, err
	}

	switch cc := child.(type) {
	case *File:
		handle := &File{
			name:    cc.name,
			perm:    cc.perm,
			size:    int64(cc.content.Len()),
			content: bytes.NewBuffer(cc.content.Bytes()),
		}
		return handle, nil
	case *dir:
		dirName := cc.name
		if isRoot {
			dirName = "."
		}
		handle := &fhDir{
			dir:  cc,
			name: dirName,
		}
		return handle, nil
	}

	return nil, fmt.Errorf("unexpected file type in fs: %s: %w", name, fs.ErrInvalid)
}

// Sub returns an FS corresponding to the subtree rooted at path.
func (rootFS *FS) Sub(path string) (fs.FS, error) {
	dir, err := rootFS.getDir(path)
	if err != nil {
		return nil, err
	}
	return &FS{dir: dir}, nil
}

// Symlink creates a symbolic link at newname pointing to oldname.
// The oldname target is stored verbatim and is not validated or resolved.
func (rootFS *FS) Symlink(oldname, newname string) error {
	if !fs.ValidPath(newname) {
		return &fs.PathError{Op: "symlink", Path: newname, Err: fs.ErrInvalid}
	}
	if newname == "." {
		return &fs.PathError{Op: "symlink", Path: newname, Err: fs.ErrInvalid}
	}

	dirPart, filePart := syspath.Split(newname)
	dirPart = strings.TrimSuffix(dirPart, "/")

	parent, err := rootFS.getDir(dirPart)
	if err != nil {
		return err
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	if parent.children[filePart] != nil {
		return &fs.PathError{Op: "symlink", Path: newname, Err: fs.ErrExist}
	}

	parent.children[filePart] = &symlink{
		name:    filePart,
		target:  oldname,
		perm:    fs.ModeSymlink | 0o777,
		modTime: time.Now(),
	}
	return nil
}

// ReadLink returns the destination of the named symbolic link.
func (rootFS *FS) ReadLink(name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}
	if name == "." {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}

	child, err := rootFS.resolve(name, true)
	if err != nil {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: err}
	}

	sl, ok := child.(*symlink)
	if !ok {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}

	return sl.target, nil
}

// Lstat returns a FileInfo describing the named file without following symlinks.
func (rootFS *FS) Lstat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: fs.ErrInvalid}
	}

	n := name
	if n == "." {
		n = ""
	}

	child, err := rootFS.resolve(n, true)
	if err != nil {
		return nil, &fs.PathError{Op: "lstat", Path: name, Err: err}
	}

	return childFileInfo(child), nil
}

// StatLink returns a FileInfo describing the file without following symbolic links.
func (rootFS *FS) StatLink(name string) (fs.FileInfo, error) {
	return rootFS.Lstat(name)
}

func childFileInfo(c childI) *fileInfo {
	switch v := c.(type) {
	case *File:
		return &fileInfo{name: v.name, size: v.size, modTime: v.modTime, mode: v.perm}
	case *dir:
		return &fileInfo{name: v.name, size: 4096, modTime: v.modTime, mode: v.perm | fs.ModeDir}
	case *symlink:
		return &fileInfo{name: v.name, size: int64(len(v.target)), modTime: v.modTime, mode: v.perm}
	}
	return nil
}

type dir struct {
	mu       sync.Mutex
	name     string
	perm     os.FileMode
	modTime  time.Time
	children map[string]childI
}

type fhDir struct {
	dir  *dir
	name string
	idx  int
}

func (d *fhDir) Stat() (fs.FileInfo, error) {
	fi := fileInfo{
		name:    d.name,
		size:    4096,
		modTime: d.dir.modTime,
		mode:    d.dir.perm | fs.ModeDir,
	}
	return &fi, nil
}

func (d *fhDir) Read(b []byte) (int, error) {
	return 0, errors.New("is a directory")
}

func (d *fhDir) Close() error {
	return nil
}

func (d *fhDir) ReadDir(n int) ([]fs.DirEntry, error) {
	d.dir.mu.Lock()
	defer d.dir.mu.Unlock()

	names := make([]string, 0, len(d.dir.children))
	for name := range d.dir.children {
		names = append(names, name)
	}
	slices.Sort(names)

	remaining := len(names) - d.idx
	if n > 0 && remaining == 0 {
		return nil, io.EOF
	}
	if n <= 0 || n > remaining {
		n = remaining
	}

	out := make([]fs.DirEntry, 0, n)
	for _, name := range names[d.idx : d.idx+n] {
		child := d.dir.children[name]

		switch c := child.(type) {
		case *File:
			stat, _ := c.Stat()
			out = append(out, &dirEntry{info: stat})
		case *dir:
			fi := fileInfo{
				name:    c.name,
				size:    4096,
				modTime: c.modTime,
				mode:    c.perm | fs.ModeDir,
			}
			out = append(out, &dirEntry{info: &fi})
		case *symlink:
			fi := fileInfo{
				name:    c.name,
				size:    int64(len(c.target)),
				modTime: c.modTime,
				mode:    c.perm,
			}
			out = append(out, &dirEntry{info: &fi})
		}
	}
	d.idx += n
	return out, nil
}

type File struct {
	name    string
	perm    os.FileMode
	size    int64
	content *bytes.Buffer
	modTime time.Time
	closed  bool
}

func (f *File) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}
	fi := fileInfo{
		name:    f.name,
		size:    f.size,
		modTime: f.modTime,
		mode:    f.perm,
	}
	return &fi, nil
}

func (f *File) Read(b []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.content.Read(b)
}

func (f *File) Close() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return nil
}

type childI interface{}

type symlink struct {
	name    string
	target  string
	perm    os.FileMode
	modTime time.Time
}

// maxSymlinks is the maximum number of symlink resolutions allowed during
// a single path resolution, matching the Linux kernel limit (MAXSYMLINKS).
const maxSymlinks = 40

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	mode    fs.FileMode
}

// base name of the file
func (fi *fileInfo) Name() string {
	return fi.name
}

// length in bytes for regular files; system-dependent for others
func (fi *fileInfo) Size() int64 {
	return fi.size
}

// file mode bits
func (fi *fileInfo) Mode() fs.FileMode {
	return fi.mode
}

// modification time
func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

// abbreviation for Mode().IsDir()
func (fi *fileInfo) IsDir() bool {
	return fi.mode&fs.ModeDir > 0
}

// underlying data source (can return nil)
func (fi *fileInfo) Sys() interface{} {
	return nil
}

type dirEntry struct {
	info fs.FileInfo
}

func (de *dirEntry) Name() string {
	return de.info.Name()
}

func (de *dirEntry) IsDir() bool {
	return de.info.IsDir()
}

func (de *dirEntry) Type() fs.FileMode {
	return de.info.Mode().Type()
}

func (de *dirEntry) Info() (fs.FileInfo, error) {
	return de.info, nil
}
