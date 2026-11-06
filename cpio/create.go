// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"path"
	"time"

	"github.com/unikraft/go-archivefs"
)

// Create writes the files of src to dst as an SVR4 archive, without the root
// directory. CPIO cannot keep extended attributes, thus Create does not write them.
func Create(dst io.Writer, src fs.FS) error {
	members, err := collect(src)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(dst, 1<<16)
	cw := NewWriter(bw)
	buf := make([]byte, 1<<16)
	for _, m := range members {
		if err := m.write(cw, src, buf); err != nil {
			return createError(m.name, err)
		}
	}
	if err := cw.Close(); err != nil {
		return err
	}
	return bw.Flush()
}

// member is one entry of the archive.
type member struct {
	name string // path in the source file system
	hdr  Header
}

// collect walks src and returns its entries with archive inode numbers and
// link counts. It reads no file data.
func collect(src fs.FS) ([]*member, error) {
	var (
		members []*member
		dirs    = map[string]*member{}
		groups  = map[linkKey][]*member{}
		ino     int64
	)

	if err := fs.WalkDir(src, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || name == "." {
			return err
		}
		if !fs.ValidPath(name) || path.Base(name) != d.Name() {
			return createError(name, fs.ErrInvalid)
		}
		fi, err := d.Info()
		if err != nil {
			return createError(name, err)
		}
		mode, err := fileMode(fi.Mode())
		if err != nil {
			return createError(name, err)
		}
		var target string
		if fi.Mode()&fs.ModeSymlink != 0 {
			if target, err = fs.ReadLink(src, name); err != nil {
				return createError(name, err)
			}
		}

		sys := fi.Sys()
		m := &member{
			name: name,
			hdr: Header{
				// The "./" prefix makes sure that no entry has the name of the trailer.
				Name:     "./" + name,
				Linkname: target,
				Mode:     mode,
				ModTime:  clampTime(fi.ModTime()),
				Uid:      archivefs.GetUID(sys),
				Guid:     archivefs.GetGID(sys),
				Links:    1,
			},
		}
		switch mode & ModeType {
		case TypeRegular:
			m.hdr.Size = fi.Size()
		case TypeSymlink:
			m.hdr.Size = int64(len(target))
		case TypeCharDevice, TypeDevice:
			m.hdr.RdevMajor = int(archivefs.GetDevMajor(sys))
			m.hdr.RdevMinor = int(archivefs.GetDevMinor(sys))
		case TypeDir:
			m.hdr.Links = 2
			dirs[name] = m
			if p, ok := dirs[path.Dir(name)]; ok {
				p.hdr.Links++
			}
		}

		key := linkKey{ino: int64(archivefs.GetIno(sys)), mode: mode & ModeType}
		if !mode.IsDir() && key.ino != 0 && archivefs.GetNlink(sys) >= 2 {
			if g := groups[key]; len(g) > 0 {
				// Files on different devices can have the same inode number.
				first := g[0]
				if m.hdr.Mode != first.hdr.Mode || m.hdr.Size != first.hdr.Size || target != first.hdr.Linkname {
					return createError(name, fmt.Errorf("cpio: inode of %s, but a different mode, size or target", first.name))
				}
				m.hdr.Inode = first.hdr.Inode
				// Only the first link of a regular file carries the data. Each
				// link of a symbolic link carries the target.
				if mode.IsRegular() {
					m.hdr.Size = 0
				}
			}
			groups[key] = append(groups[key], m)
		}
		if m.hdr.Inode == 0 {
			ino++
			m.hdr.Inode = ino
		}

		members = append(members, m)
		return nil
	}); err != nil {
		return nil, err
	}

	for _, g := range groups {
		for _, m := range g {
			m.hdr.Links = len(g)
		}
	}
	for _, m := range members {
		if err := checkSVR4Header(&m.hdr); err != nil {
			return nil, createError(m.name, err)
		}
	}

	return members, nil
}

// write writes the header of m to cw, then the data of the file or the target
// of the symbolic link. It copies the data through buf.
func (m *member) write(cw *Writer, src fs.FS, buf []byte) error {
	if err := cw.WriteHeader(&m.hdr); err != nil {
		return err
	}

	switch {
	case m.hdr.Mode&ModeType == TypeSymlink:
		_, err := io.WriteString(cw, m.hdr.Linkname)
		return err
	case m.hdr.Mode.IsRegular() && m.hdr.Size > 0:
		f, err := src.Open(m.name)
		if err != nil {
			return err
		}
		defer f.Close()

		// The struct hides the WriterTo method of f, thus io.CopyBuffer uses buf.
		n, err := io.CopyBuffer(cw, struct{ io.Reader }{f}, buf)
		if err != nil {
			return err
		}
		if n != m.hdr.Size {
			return fmt.Errorf("cpio: file changed while it was written: read %d of %d bytes", n, m.hdr.Size)
		}
	}

	return nil
}

// createError returns err with the path name, if err does not have a path.
func createError(name string, err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return err
	}
	return &fs.PathError{Op: "create", Path: name, Err: err}
}

// clampTime returns t in the range of times that an SVR4 header can hold.
func clampTime(t time.Time) time.Time {
	switch {
	case t.Unix() < 0:
		return time.Unix(0, 0)
	case t.Unix() > math.MaxUint32:
		return time.Unix(math.MaxUint32, 0)
	}
	return t
}
