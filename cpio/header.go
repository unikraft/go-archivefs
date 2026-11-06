// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2017, Ryan Armstrong.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio

import (
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	// TypeRegular indicates a regular file
	TypeRegular = 0o100000

	// The following are header-only flags and may not have a data body.
	TypeSocket     = 0o140000 // Socket
	TypeSymlink    = 0o120000 // Symbolic link
	TypeDevice     = 0o60000  // Block device node
	TypeDir        = 0o40000  // Directory
	TypeCharDevice = 0o20000  // Character device node
	TypeNamedPipe  = 0o10000  // FIFO node
)

const (
	ModeSetuid = 0o4000 // Set uid
	ModeSetgid = 0o2000 // Set gid
	ModeSticky = 0o1000 // Save text (sticky bit)

	ModeType = 0o170000 // Mask for the type bits
	ModePerm = 0o777    // Unix permission bits
)

// Deprecated: these constants are provided for backward compatibility. Use the
// Type* constants instead.
const (
	ModeDir        = TypeDir
	ModeNamedPipe  = TypeNamedPipe
	ModeRegular    = TypeRegular
	ModeSymlink    = TypeSymlink
	ModeDevice     = TypeDevice
	ModeCharDevice = TypeCharDevice
	ModeSocket     = TypeSocket
)

const (
	// headerEOF is the value of the filename of the last header in a CPIO archive.
	headerEOF = "TRAILER!!!"
)

// ErrHeader indicates there was an error decoding a CPIO header entry.
var ErrHeader = errors.New("cpio: invalid cpio header")

// A FileMode represents a file's mode and permission bits.
type FileMode uint32

func (m FileMode) String() string {
	return fmt.Sprintf("%#o", m)
}

// IsDir reports whether m describes a directory. That is, it tests that the
// type bits of m are TypeDir.
func (m FileMode) IsDir() bool {
	return m&ModeType == TypeDir
}

// IsRegular reports whether m describes a regular file. That is, it tests that
// the type bits of m are TypeRegular.
func (m FileMode) IsRegular() bool {
	return m&ModeType == TypeRegular
}

// Perm returns the Unix permission bits in m.
func (m FileMode) Perm() FileMode {
	return m & ModePerm
}

// A Header represents a single header in a CPIO archive. Some fields may not be
// populated.
//
// For forward compatibility, users that retrieve a Header from Reader.Next,
// mutate it in some ways, and then pass it back to Writer.WriteHeader should do
// so by creating a new Header and copying the fields that they are interested
// in preserving.
type Header struct {
	Name     string // Name of the file entry
	Linkname string // Target name of link (valid for TypeLink or TypeSymlink)
	Links    int    // Number of inbound links

	Size int64    // Size in bytes
	Mode FileMode // Permission and mode bits
	Uid  int      // User id of the owner
	Guid int      // Group id of the owner

	ModTime time.Time // Modification time

	Checksum uint32 // Computed checksum

	// Deprecated: DeviceID is not read or written. Use DevMajor and DevMinor.
	DeviceID int
	Inode    int64 // Inode number

	DevMajor  int // Major number of the device that contains the file
	DevMinor  int // Minor number of the device that contains the file
	RdevMajor int // Major number of a character or block device node
	RdevMinor int // Minor number of a character or block device node

	pad int64 // bytes to pad before next header
}

// GetIno returns the inode number.
func (h *Header) GetIno() uint64 { return uint64(h.Inode) }

// GetNlink returns the number of hard links.
func (h *Header) GetNlink() uint64 { return uint64(h.Links) }

// GetUID returns the user ID of the owner.
func (h *Header) GetUID() int { return h.Uid }

// GetGID returns the group ID of the owner.
func (h *Header) GetGID() int { return h.Guid }

// GetDevMajor returns RdevMajor, the major number of a device node. It is not
// DevMajor.
func (h *Header) GetDevMajor() uint32 { return uint32(h.RdevMajor) }

// GetDevMinor returns RdevMinor, the minor number of a device node. It is not
// DevMinor.
func (h *Header) GetDevMinor() uint32 { return uint32(h.RdevMinor) }

// RawHeader exposes the unparsed header data.
type RawHeader [110]byte

// Bytes returns the header as a byte slice.
func (rh *RawHeader) Bytes() []byte {
	return rh[:]
}

// FileInfo returns an fs.FileInfo for the Header.
func (h *Header) FileInfo() os.FileInfo {
	return fileInfo{h}
}

// FileInfoHeader creates a partially-populated Header from fi. If fi describes
// a symlink, FileInfoHeader records link as the link target. If fi describes a
// directory, a slash is appended to the name.
//
// Since fs.FileInfo's Name method returns only the base name of the file it
// describes, it may be necessary to modify Header.Name to provide the full path
// name of the file.
func FileInfoHeader(fi os.FileInfo, link string) (*Header, error) {
	if fi == nil {
		return nil, errors.New("cpio: FileInfo is nil")
	}
	if sys, ok := fi.Sys().(*Header); ok {
		// This FileInfo came from a Header (not the OS). Return a copy of the
		// original Header.
		h := &Header{}
		*h = *sys
		return h, nil
	}
	fm := fi.Mode()
	mode, err := fileMode(fm)
	if err != nil {
		return nil, err
	}
	h := &Header{
		Name:    fi.Name(),
		Mode:    mode,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
	}
	switch {
	case fm.IsDir():
		h.Name += "/"
		h.Size = 0
	case fm&os.ModeSymlink != 0:
		h.Linkname = link
	}
	return h, nil
}

// fileMode returns the CPIO type, permission and special bits of fm.
func fileMode(fm os.FileMode) (FileMode, error) {
	m := FileMode(fm.Perm())
	switch {
	case fm.IsRegular():
		m |= TypeRegular
	case fm.IsDir():
		m |= TypeDir
	case fm&os.ModeSymlink != 0:
		m |= TypeSymlink
	case fm&os.ModeDevice != 0:
		if fm&os.ModeCharDevice != 0 {
			m |= TypeCharDevice
		} else {
			m |= TypeDevice
		}
	case fm&os.ModeNamedPipe != 0:
		m |= TypeNamedPipe
	case fm&os.ModeSocket != 0:
		m |= TypeSocket
	default:
		return 0, fmt.Errorf("cpio: unknown file mode %v", fm)
	}
	if fm&os.ModeSetuid != 0 {
		m |= ModeSetuid
	}
	if fm&os.ModeSetgid != 0 {
		m |= ModeSetgid
	}
	if fm&os.ModeSticky != 0 {
		m |= ModeSticky
	}
	return m, nil
}
