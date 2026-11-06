// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math"
	"path"
	"slices"
	"strings"
)

var (
	_ fs.FS         = (*FS)(nil)
	_ fs.ReadDirFS  = (*FS)(nil)
	_ fs.ReadFileFS = (*FS)(nil)
	_ fs.StatFS     = (*FS)(nil)
	_ fs.ReadLinkFS = (*FS)(nil)
)

// maxSymlinks is the maximum number of symbolic links that one lookup follows.
const maxSymlinks = 40

var (
	errNotDir       = errors.New("not a directory")
	errIsDir        = errors.New("is a directory")
	errTooManyLinks = errors.New("too many levels of symbolic links")
)

// FS is a read-only file system for the files in a CPIO archive.
type FS struct {
	ra   io.ReaderAt
	root *dirent
}

// OpenFS reads the headers of the SVR4 CPIO archive in ra. The returned FS
// reads the file data from ra when necessary, thus ra must stay open.
func OpenFS(ra io.ReaderAt) (*FS, error) {
	root := &dirent{name: ".", node: &inode{hdr: Header{Mode: TypeDir | 0o755}}}
	root.parent = root
	fsys := &FS{ra: ra, root: root}

	links := map[linkKey]*inode{}
	r := newOffsetReader(ra)
	for {
		hdr, _, err := readSVR4Header(r)
		if err == io.EOF {
			// A header is returned only for the trailer.
			if hdr == nil {
				return nil, io.ErrUnexpectedEOF
			}
			break
		}
		if err != nil {
			return nil, err
		}

		off := r.off
		r.skip(hdr.Size + hdr.pad)
		fsys.insert(hdr, off, links)
	}

	fsys.finish()

	return fsys, nil
}

// Open opens the named file. It follows symbolic links.
func (fsys *FS) Open(name string) (fs.File, error) {
	d, err := fsys.find("open", name, true)
	if err != nil {
		return nil, err
	}

	if d.IsDir() {
		return &dirFile{name: name, info: d.info(name), entries: d.entries()}, nil
	}

	return &file{
		SectionReader: io.NewSectionReader(fsys.ra, d.node.off, d.node.hdr.Size),
		info:          d.info(name),
	}, nil
}

// ReadDir returns the entries of the named directory, sorted by name.
func (fsys *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	d, err := fsys.find("readdir", name, true)
	if err != nil {
		return nil, err
	}

	if !d.IsDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: errNotDir}
	}

	return d.entries(), nil
}

// ReadFile returns the contents of the named file.
func (fsys *FS) ReadFile(name string) ([]byte, error) {
	d, err := fsys.find("read", name, true)
	if err != nil {
		return nil, err
	}

	if d.IsDir() {
		return nil, &fs.PathError{Op: "read", Path: name, Err: errIsDir}
	}

	b := make([]byte, d.node.hdr.Size)
	if _, err := io.ReadFull(io.NewSectionReader(fsys.ra, d.node.off, d.node.hdr.Size), b); err != nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: err}
	}

	return b, nil
}

// Stat returns the file information of the named file. It follows symbolic
// links.
func (fsys *FS) Stat(name string) (fs.FileInfo, error) {
	d, err := fsys.find("stat", name, true)
	if err != nil {
		return nil, err
	}

	return d.info(name), nil
}

// Lstat returns the file information of the named file. It does not follow a
// symbolic link at the end of name.
func (fsys *FS) Lstat(name string) (fs.FileInfo, error) {
	d, err := fsys.find("lstat", name, false)
	if err != nil {
		return nil, err
	}

	return d.info(name), nil
}

// ReadLink returns the target of the named symbolic link.
func (fsys *FS) ReadLink(name string) (string, error) {
	d, err := fsys.find("readlink", name, false)
	if err != nil {
		return "", err
	}

	if d.node.hdr.Mode&ModeType != TypeSymlink {
		return "", &fs.PathError{Op: "readlink", Path: name, Err: fs.ErrInvalid}
	}

	return d.node.hdr.Linkname, nil
}

// find makes sure that name is valid and finds its file. It puts errors in an
// fs.PathError for op.
func (fsys *FS) find(op, name string, follow bool) (*dirent, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}

	d, err := fsys.lookup(name, follow)
	if err != nil {
		return nil, &fs.PathError{Op: op, Path: name, Err: err}
	}

	return d, nil
}

// lookup finds the file at name. It follows symbolic links in the parent
// directories, and at the end of name only if follow is true.
func (fsys *FS) lookup(name string, follow bool) (*dirent, error) {
	hops := 0
	d := fsys.root

	rest := name
	for rest != "" {
		if !d.IsDir() {
			return nil, errNotDir
		}

		var elem string
		elem, rest, _ = strings.Cut(rest, "/")

		switch elem {
		case "", ".":
			continue
		case "..":
			d = d.parent
			continue
		}

		c, ok := d.children[elem]
		if !ok {
			return nil, fs.ErrNotExist
		}

		if c.node.hdr.Mode&ModeType == TypeSymlink && (rest != "" || follow) {
			hops++
			if hops > maxSymlinks {
				return nil, errTooManyLinks
			}

			// Continue with the target and the remaining elements. An absolute
			// target starts again from the root.
			if path.IsAbs(c.node.hdr.Linkname) {
				d = fsys.root
			}
			rest = c.node.hdr.Linkname + "/" + rest
			continue
		}

		d = c
	}

	return d, nil
}

// insert puts the entry hdr, with its data at off, in the tree as the Linux
// initramfs unpacker does. It skips an entry that it cannot put in the tree.
func (fsys *FS) insert(hdr *Header, off int64, links map[linkKey]*inode) {
	parent, elem, err := fsys.parent(hdr.Name)
	if err != nil {
		return
	}

	t := hdr.Mode & ModeType
	if elem == "" {
		// Only a directory entry can update the directory that name ends in.
		if t == TypeDir {
			parent.node.set(hdr, off)
		}
		return
	}

	// The unpacker cannot remove a directory that is not empty.
	old := parent.children[elem]
	busy := old != nil && len(old.children) > 0

	switch t {
	case TypeDir:
		if old != nil && old.IsDir() {
			old.node.set(hdr, off)
			return
		}
		parent.add(elem, newInode(hdr, off))

	case TypeSymlink:
		if !busy {
			parent.add(elem, newInode(hdr, off))
		}

	default:
		key := linkKey{hdr.Inode, hdr.DevMajor, hdr.DevMinor, t}
		linked := hdr.Links >= 2
		if n := links[key]; linked && n != nil {
			if !busy {
				n.link(hdr, off)
				parent.add(elem, n)
			}
			return
		}

		var n *inode
		switch {
		case old != nil && old.node.hdr.Mode&ModeType == t:
			// The unpacker writes through the file that is there, thus all
			// of its hard links change.
			n = old.node
			n.set(hdr, off)
		case busy:
			return
		default:
			n = newInode(hdr, off)
			parent.add(elem, n)
		}

		if linked {
			links[key] = n
		}
	}
}

// parent returns the directory for the entry name and its last element. If
// name ends in "/" or ".", it returns that directory and "".
func (fsys *FS) parent(name string) (*dirent, string, error) {
	elems := strings.Split(name, "/")
	last := elems[len(elems)-1]
	elems = slices.DeleteFunc(elems, func(e string) bool {
		return e == "" || e == "."
	})

	var elem string
	if last != "" && last != "." {
		elem = elems[len(elems)-1]
		elems = elems[:len(elems)-1]
		if elem == ".." {
			return nil, "", fs.ErrInvalid
		}
	}

	// Make the directories that do not exist, and follow symbolic links.
	d := fsys.root
	for _, e := range elems {
		if e == ".." {
			d = d.parent
			continue
		}

		c, ok := d.children[e]
		if !ok {
			c = d.add(e, newInode(&Header{Mode: TypeDir | 0o755}, 0))
		}

		if c.node.hdr.Mode&ModeType == TypeSymlink {
			var err error
			if c, err = fsys.lookup(c.name, true); err != nil {
				return nil, "", err
			}
		}

		if !c.IsDir() {
			return nil, "", errNotDir
		}

		d = c
	}

	return d, elem, nil
}

// finish sorts the children of all directories. It gives inode numbers and
// link counts in path order.
func (fsys *FS) finish() {
	var ino int64
	seen := map[*inode]bool{}

	var visit func(d *dirent)
	visit = func(d *dirent) {
		n := d.node
		if !seen[n] {
			seen[n] = true
			ino++
			n.hdr.Inode = ino
			n.hdr.Links = 0
		}

		if !d.IsDir() {
			n.hdr.Links++
			return
		}

		// A directory has a link from its parent, from "." and from ".." of
		// each subdirectory.
		n.hdr.Links = 2
		for _, name := range slices.Sorted(maps.Keys(d.children)) {
			c := d.children[name]
			d.sorted = append(d.sorted, c)
			if c.IsDir() {
				n.hdr.Links++
			}
			visit(c)
		}
	}

	visit(fsys.root)
}

// linkKey identifies the entries of one hard link group, as the Linux
// initramfs unpacker does.
type linkKey struct {
	ino          int64
	major, minor int
	mode         FileMode // only the type bits
}

// inode is the metadata and the data of a file. All hard links of a file
// share one inode.
type inode struct {
	hdr Header // Inode and Links are set when the tree is complete
	off int64  // offset of the file data in the archive
}

func newInode(hdr *Header, off int64) *inode {
	return &inode{hdr: *hdr, off: off}
}

// set replaces the metadata and the data of n with those of the entry hdr.
func (n *inode) set(hdr *Header, off int64) {
	n.hdr, n.off = *hdr, off
}

// link replaces the metadata of n with those of the hard link hdr. It keeps
// the data of n if hdr has no data.
func (n *inode) link(hdr *Header, off int64) {
	size, dataOff := n.hdr.Size, n.off
	n.set(hdr, off)
	if hdr.Size == 0 {
		n.hdr.Size, n.off = size, dataOff
	}
}

// dirent is a path in the tree of an FS.
type dirent struct {
	name     string // path in the tree
	parent   *dirent
	node     *inode
	children map[string]*dirent
	sorted   []fs.DirEntry // children sorted by name
}

var _ fs.DirEntry = (*dirent)(nil)

func (d *dirent) Name() string               { return path.Base(d.name) }
func (d *dirent) IsDir() bool                { return d.node.hdr.Mode.IsDir() }
func (d *dirent) Type() fs.FileMode          { return d.node.hdr.FileInfo().Mode().Type() }
func (d *dirent) Info() (fs.FileInfo, error) { return d.info(d.name), nil }

// info returns the file information of d, with name as the path.
func (d *dirent) info(name string) fs.FileInfo {
	h := d.node.hdr
	h.Name = name
	return h.FileInfo()
}

// add puts a new path for n in d as elem, and returns it.
func (d *dirent) add(elem string, n *inode) *dirent {
	c := &dirent{name: path.Join(d.name, elem), parent: d, node: n}
	if d.children == nil {
		d.children = map[string]*dirent{}
	}
	d.children[elem] = c
	return c
}

// entries returns the children of d, sorted by name.
func (d *dirent) entries() []fs.DirEntry {
	return slices.Clone(d.sorted)
}

// file is an open file that is not a directory.
type file struct {
	*io.SectionReader
	info fs.FileInfo
}

func (f *file) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *file) Close() error               { return nil }

// dirFile is an open directory.
type dirFile struct {
	name    string
	info    fs.FileInfo
	entries []fs.DirEntry
	pos     int
}

func (f *dirFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *dirFile) Close() error               { return nil }

func (f *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: f.name, Err: errIsDir}
}

// ReadDir returns the next n entries of the directory, or all of the remaining
// entries if n <= 0.
func (f *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	rest := f.entries[f.pos:]
	if n <= 0 {
		f.pos = len(f.entries)
		return rest, nil
	}

	if len(rest) == 0 {
		return nil, io.EOF
	}

	n = min(n, len(rest))
	f.pos += n

	return rest[:n], nil
}

// offsetReader reads from an io.ReaderAt through a buffer. It keeps the offset
// of the next byte that Read returns.
type offsetReader struct {
	sr  *io.SectionReader
	br  *bufio.Reader
	off int64
}

func newOffsetReader(ra io.ReaderAt) *offsetReader {
	sr := io.NewSectionReader(ra, 0, math.MaxInt64)
	return &offsetReader{sr: sr, br: bufio.NewReader(sr)}
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.br.Read(p)
	r.off += int64(n)
	return n, err
}

// skip moves the offset n bytes forward. It reads no data from the
// io.ReaderAt.
func (r *offsetReader) skip(n int64) {
	if n <= int64(r.br.Buffered()) {
		r.br.Discard(int(n))
	} else {
		r.sr.Seek(r.off+n, io.SeekStart)
		r.br.Reset(r.sr)
	}
	r.off += n
}
