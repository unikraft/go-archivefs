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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	stdpath "path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/unikraft/go-archivefs"
)

const (
	BlockSize     = 4096
	BlockSizeBits = 12
	InodeSlotSize = 1 << InodeSlotBits
	// MaxInlineDataSize is the largest regular file stored inline; 0 keeps file data aligned for DAX.
	MaxInlineDataSize = 0
	// MaxTailSize is the largest regular file tail stored inline; 0 keeps file data aligned for DAX.
	MaxTailSize = 0
)

// Create creates an EROFS filesystem image from the source filesystem and writes
// it to the destination writer.
func Create(dst io.WriterAt, src fs.FS, opts ...ErofsCreateOption) error {
	w := &writer{
		src: src,
		dst: &imageWriter{WriterAt: dst},
	}

	for _, opt := range opts {
		if err := opt(&w.opts); err != nil {
			return err
		}
	}

	return w.write()
}

type writer struct {
	src        fs.FS
	dst        *imageWriter
	inodes     map[string]any
	inodeOrder []string
	fileSizes  map[string]int64
	xattrs     map[string][]byte     // inline xattr area by path
	links      map[string]*linkGroup // hard link group of each path with more than one link
	opts       ErofsCreateOptions
}

// linkKey identifies a hard link group by device, inode number and type, like the Linux initramfs unpacker.
type linkKey struct {
	dev uint64
	ino uint64
	typ uint16
}

// linkGroup is a hard link group, whose paths share one inode.
type linkGroup struct {
	nlink int    // the number of links in the source tree
	first string // the first path that firstPass gave an inode
}

func (w *writer) write() error {
	if err := w.populateInodes(); err != nil {
		return fmt.Errorf("failed to populate inodes: %w", err)
	}

	metaSize, dataSize, err := w.firstPass()
	if err != nil {
		return fmt.Errorf("failed to calculate metadata and data size: %w", err)
	}

	// Metadata starts at block 0 and shares it with the superblock, like mkfs.erofs.
	metaBlockAddr := int64(0)

	if err := w.writeMetadata(metaBlockAddr); err != nil {
		return fmt.Errorf("failed to write metadata blocks: %w", err)
	}

	if err := w.writeData(); err != nil {
		return fmt.Errorf("failed to write data blocks: %w", err)
	}

	rootIno, ok := w.inodes["."]
	if !ok {
		return fmt.Errorf("root inode not found")
	}
	var rootNid uint16
	switch ino := rootIno.(type) {
	case InodeCompact:
		rootNid = uint16(ino.Ino)
	case InodeExtended:
		rootNid = uint16(ino.Ino)
	default:
		return fmt.Errorf("unsupported root inode type %T", rootIno)
	}

	var sbUUID [16]uint8
	if w.opts.uuid != nil {
		sbUUID = *w.opts.uuid
	} else {
		// Generate a UUID for the filesystem.
		uuidBytes, err := uuid.New().MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to generate UUID: %w", err)
		}
		copy(sbUUID[:], uuidBytes)
	}

	buildTime := time.Now()
	if w.opts.buildTime != nil {
		buildTime = *w.opts.buildTime
	}

	// Hard links share one inode, so count the inodes and not the paths.
	uniqueInodes := make(map[uint32]bool)
	for _, path := range w.inodeOrder {
		switch ino := w.inodes[path].(type) {
		case InodeCompact:
			uniqueInodes[ino.Ino] = true
		case InodeExtended:
			uniqueInodes[ino.Ino] = true
		}
	}

	sb := SuperBlock{
		Magic:         SuperBlockMagicV1,
		BlockSizeBits: BlockSizeBits,
		RootNid:       rootNid,
		Inodes:        uint64(len(uniqueInodes)),
		Blocks:        uint32((metaSize + dataSize) / BlockSize),
		MetaBlockAddr: uint32(metaBlockAddr),
		UUID:          sbUUID,
		BuildTime:     uint64(buildTime.Unix()),
		BuildTimeNsec: uint32(buildTime.Nanosecond()),
		FeatureCompat: EROFS_FEATURE_COMPAT_SB_CHKSUM | EROFS_FEATURE_COMPAT_MTIME,
		// TODO: other fields (volume name, etc.)
	}

	if err := w.checksumSuperBlock(&sb); err != nil {
		return fmt.Errorf("failed to calculate superblock checksum: %w", err)
	}

	if err := binary.Write(io.NewOffsetWriter(w.dst, SuperBlockOffset), binary.LittleEndian, &sb); err != nil {
		return fmt.Errorf("failed to write superblock: %w", err)
	}

	if err := w.dst.setSize(int64(sb.Blocks) * BlockSize); err != nil {
		return fmt.Errorf("failed to set the image size: %w", err)
	}

	return nil
}

// imageWriter writes the image, writes zeros in the gaps between writes, and keeps a copy of block 0.
type imageWriter struct {
	io.WriterAt
	end    int64 // the end of the furthest write
	block0 [BlockSize]byte
}

func (w *imageWriter) WriteAt(p []byte, off int64) (int, error) {
	if err := w.zeroTo(off); err != nil {
		return 0, err
	}
	n, err := w.WriterAt.WriteAt(p, off)
	if off >= 0 && off < BlockSize {
		copy(w.block0[off:], p[:n])
	}
	w.end = max(w.end, off+int64(n))
	return n, err
}

var zeroBlock [BlockSize]byte

// zeroTo writes zeros from the furthest write up to off, so that no old data of the destination stays in the image.
func (w *imageWriter) zeroTo(off int64) error {
	for w.end < off {
		n, err := w.WriterAt.WriteAt(zeroBlock[:min(BlockSize, off-w.end)], w.end)
		w.end += int64(n)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// setSize writes zeros up to size, and truncates the destination if it can.
func (w *imageWriter) setSize(size int64) error {
	if err := w.zeroTo(size); err != nil {
		return err
	}

	// A device cannot be truncated, but the zeros already make the image complete.
	if t, ok := w.WriterAt.(interface{ Truncate(int64) error }); ok {
		if err := t.Truncate(size); err != nil && !errors.Is(err, syscall.EINVAL) {
			return err
		}
	}

	return nil
}

// firstPass precomputes the layout of the blocks, and inodes.
func (w *writer) firstPass() (metaSize, dataSize int64, err error) {
	// The inodes start in block 0, after the superblock.
	metaSize = roundUp(SuperBlockOffset+int64(binary.Size(SuperBlock{})), InodeSlotSize)

	for _, path := range w.inodeOrder {
		// The other links of a group use the inode of the first link: they use no metadata slot and no data blocks.
		group := w.links[path]
		if group != nil && group.first != "" {
			w.inodes[path] = w.inodes[group.first]
			continue
		}

		ino := w.inodes[path]

		// For regular files, use the cached size from populateInodes to avoid
		// opening the file twice (once here for size, once in the write phase
		// for data). Directories and symlinks must still call dataForInode
		// because their encoded size differs from fi.Size().
		var size int64
		var mode uint16
		switch ino := ino.(type) {
		case InodeCompact:
			mode = ino.Mode
		case InodeExtended:
			mode = ino.Mode
		}
		if mode&S_IFMT == S_IFREG {
			size = w.fileSizes[path]
		} else {
			data, sz, err := w.dataForInode(path, ino)
			if err != nil {
				return metaSize, dataSize, fmt.Errorf("failed to get data for %q: %w", path, err)
			}
			if data != nil {
				if err := data.Close(); err != nil {
					return metaSize, dataSize, fmt.Errorf("failed to close data for %q: %w", path, err)
				}
			}
			size = sz
		}

		// Special files have no data, and keep the device number that toInode put in RawBlockAddr.
		special := isSpecialFile(mode)
		if special {
			size = 0
		}

		// Decide the data layout: fully inline, tail-packed (only the tail inline) or plain.
		inodeSize := int64(binary.Size(ino)) + int64(len(w.xattrs[path]))
		nblocks := size / BlockSize
		tailSize := size % BlockSize
		inlined, useTailPacking := inlinePolicy(mode, size, inodeSize)

		// The inode, its xattrs and its inline data must not cross a block boundary, so they start in the next block if they would.
		if inlined || useTailPacking {
			inlineSize := size
			if useTailPacking {
				inlineSize = tailSize
			}
			spaceInCurrentBlock := roundUp(metaSize, BlockSize) - metaSize
			if spaceInCurrentBlock > 0 && inodeSize+inlineSize > spaceInCurrentBlock {
				metaSize = roundUp(metaSize, BlockSize)
			}
		}

		// Allocate the inode number.
		nid, err := offsetToNID(metaSize)
		if err != nil {
			return metaSize, dataSize, fmt.Errorf("failed to convert offset to inode number: %w", err)
		}

		// Tail-packed data keeps its full blocks in the data area, and the tail with the inode.
		layout := uint16(InodeDataLayoutFlatPlain)
		if inlined || useTailPacking {
			layout = InodeDataLayoutFlatInline
		}
		if group != nil {
			group.first = path
		}

		switch ino := ino.(type) {
		case InodeCompact:
			ino.Ino = nid
			ino.Size = uint32(size)
			ino.Format = setBits(ino.Format, layout, InodeDataLayoutBit, InodeDataLayoutBits)
			if group != nil {
				ino.Nlink = uint16(group.nlink)
			}
			if !inlined && !special {
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			}
			w.inodes[path] = ino

		case InodeExtended:
			ino.Ino = nid
			ino.Size = uint64(size)
			ino.Format = setBits(ino.Format, layout, InodeDataLayoutBit, InodeDataLayoutBits)
			if group != nil {
				ino.Nlink = uint32(group.nlink)
			}
			if !inlined && !special {
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			}
			w.inodes[path] = ino

		default:
			return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
		}

		metaSize += inodeSize

		if inlined {
			metaSize += size
		} else if !special {
			if useTailPacking {
				// The full blocks go to the data area, and the tail goes after the inode.
				dataSize += nblocks * BlockSize
				metaSize += tailSize
			} else {
				dataSize += size
				dataSize = roundUp(dataSize, BlockSize)
			}
		}
		metaSize = roundUp(metaSize, InodeSlotSize)
	}

	metaSize = roundUp(metaSize, BlockSize)

	// The data blocks start after the metadata blocks.
	dataBlockAddr := metaSize / BlockSize

	// fix up the raw block addresses now that we know the total size of the
	// metadata space.
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		switch ino := ino.(type) {
		case InodeCompact:
			if hasDataBlocks(ino) {
				// Special files (devices, FIFOs, sockets) store a device number in
				// RawBlockAddr, not a data block address; do not offset it.
				if !isSpecialFile(ino.Mode) {
					ino.RawBlockAddr += uint32(dataBlockAddr)
				}
				w.inodes[path] = ino
			}
		case InodeExtended:
			if hasDataBlocks(ino) {
				// Special files (devices, FIFOs, sockets) store a device number in
				// RawBlockAddr, not a data block address; do not offset it.
				if !isSpecialFile(ino.Mode) {
					ino.RawBlockAddr += uint32(dataBlockAddr)
				}
				w.inodes[path] = ino
			}
		default:
			return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
		}
	}

	return
}

func (w *writer) writeMetadata(metaBlockAddr int64) error {
	// Hard links share one inode, so write each inode and its inline data once.
	writtenInodes := make(map[uint32]bool)

	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		var nid uint32
		var fileSize int64
		switch ino := ino.(type) {
		case InodeCompact:
			nid = ino.Ino
			fileSize = int64(ino.Size)
		case InodeExtended:
			nid = ino.Ino
			fileSize = int64(ino.Size)
		default:
			return fmt.Errorf("unsupported inode type %T", ino)
		}

		if writtenInodes[nid] {
			continue
		}
		writtenInodes[nid] = true

		// Get the address of the inode.
		off := metaBlockAddr*BlockSize + int64(nid)*InodeSlotSize

		// Write the inode.
		inodeBytes, err := marshalInode(ino)
		if err != nil {
			return fmt.Errorf("failed to marshal inode for %q: %w", path, err)
		}
		if _, err := w.dst.WriteAt(inodeBytes, off); err != nil {
			return fmt.Errorf("failed to write inode for %q: %w", path, err)
		}

		inodeEnd := off + int64(len(inodeBytes))
		if xattrs := w.xattrs[path]; len(xattrs) > 0 {
			if _, err := w.dst.WriteAt(xattrs, inodeEnd); err != nil {
				return fmt.Errorf("failed to write xattrs for %q: %w", path, err)
			}
			inodeEnd += int64(len(xattrs))
		}

		// An inline inode has all of its data after it, and a tail-packed inode has only its tail.
		if isInlined(ino) {
			data, _, err := w.dataForInode(path, ino)
			if err != nil {
				return fmt.Errorf("failed to get data for %q: %w", path, err)
			}

			// Skip the full blocks, which go to the data area.
			nblocks := fileSize / BlockSize
			inlineSize := fileSize % BlockSize
			if nblocks > 0 {
				if _, err := io.CopyN(io.Discard, data, nblocks*BlockSize); err != nil {
					_ = data.Close()
					return fmt.Errorf("failed to skip full blocks for %q: %w", path, err)
				}
			}

			if inlineSize == 0 {
				_ = data.Close()
				continue
			}

			// Like in the reader, the inline data follows the inode and its xattrs, and must end in the block where it starts.
			inlineOff := inodeEnd
			blockEnd := (inlineOff/BlockSize + 1) * BlockSize
			if inlineOff+inlineSize > blockEnd {
				_ = data.Close()
				return fmt.Errorf("inline data would cross block boundary for %q: inode at %d, inline data %d-%d, block ends at %d",
					path, off, inlineOff, inlineOff+inlineSize, blockEnd)
			}

			inlineBuf := make([]byte, inlineSize)
			if _, err := io.ReadFull(data, inlineBuf); err != nil {
				_ = data.Close()
				return fmt.Errorf("failed to read inline data for %q: %w", path, err)
			}
			_ = data.Close()

			if _, err := w.dst.WriteAt(inlineBuf, inlineOff); err != nil {
				return fmt.Errorf("failed to write inline data for %q: %w", path, err)
			}
		}
	}

	return nil
}

func (w *writer) writeData() error {
	// Hard links share one inode, so write the data of each inode only once.
	writtenInodes := make(map[uint32]bool)

	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		if !hasDataBlocks(ino) {
			// Fully inlined files are stored alongside their inode.
			continue
		}

		var rawBlockAddr uint32
		var nid uint32
		var size int64
		switch ino := ino.(type) {
		case InodeCompact:
			rawBlockAddr = ino.RawBlockAddr
			nid = ino.Ino
			size = int64(ino.Size)
		case InodeExtended:
			rawBlockAddr = ino.RawBlockAddr
			nid = ino.Ino
			size = int64(ino.Size)
		default:
			return fmt.Errorf("unsupported inode type %T", ino)
		}

		if writtenInodes[nid] {
			continue
		}
		writtenInodes[nid] = true

		// The file can change after the first pass, so copy only the size in the inode.
		data, _, err := w.dataForInode(path, ino)
		if err != nil {
			return fmt.Errorf("failed to get data for %q: %w", path, err)
		}
		if data == nil {
			continue
		}

		if isInlined(ino) {
			// Only the full blocks go to the data area, because the tail is after the inode.
			nblocks := size / BlockSize
			if nblocks > 0 {
				_, err = io.CopyN(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data, nblocks*BlockSize)
			}
		} else {
			_, err = io.CopyN(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data, size)
		}
		_ = data.Close()
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("the file became shorter after the first pass: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to write data for %q: %w", path, err)
		}
	}

	return nil
}

func (w *writer) populateInodes() error {
	w.inodes = map[string]any{}
	w.fileSizes = map[string]int64{}
	w.xattrs = map[string][]byte{}
	w.links = map[string]*linkGroup{}
	groups := map[linkKey]*linkGroup{}

	err := fs.WalkDir(w.src, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		fi, err := d.Info()
		if err != nil {
			return err
		}

		nlink := 1
		if fi.IsDir() {
			entries, err := fs.ReadDir(w.src, path)
			if err != nil {
				return fmt.Errorf("failed to read directory entries: %w", err)
			}

			// A directory has links from its parent, from "." and from each subdirectory.
			nlink = 2
			for _, e := range entries {
				if e.IsDir() {
					nlink++
				}
			}
		} else {
			// Like in the Linux initramfs unpacker, only files with more than one link are in a group.
			if n, ino := archivefs.GetNlink(fi.Sys()), archivefs.GetIno(fi.Sys()); n > 1 && ino != 0 {
				nlink = int(n)
				key := linkKey{archivefs.GetDev(fi.Sys()), ino, statModeFromFileMode(fi.Mode()) & S_IFMT}
				if groups[key] == nil {
					groups[key] = &linkGroup{}
				}
				groups[key].nlink++
				w.links[path] = groups[key]
			}
		}

		var originalFInfo *FileInfo
		if w.opts.fInfoMap != nil {
			// DirFS believes this is the root directory, so we set as such
			toCheckForName := filepath.Join("/", path)

			if filepath.Base(path) == ".." {
				toCheckForName = filepath.Dir(filepath.Dir(path))
			} else if filepath.Base(path) == "." {
				toCheckForName = filepath.Dir(path)
			}

			if finfo, ok := w.opts.fInfoMap[toCheckForName]; ok {
				originalFInfo = &finfo
			}
		}

		xattrs, err := archivefs.GetXattrs(fi.Sys())
		if err != nil {
			return fmt.Errorf("failed to get xattrs of %q: %w", path, err)
		}
		ibody, err := encodeXattrs(xattrs)
		if err != nil {
			return fmt.Errorf("failed to encode xattrs of %q: %w", path, err)
		}
		if ibody != nil {
			w.xattrs[path] = ibody
		}

		w.inodes[path] = toInode(fi, nlink, xattrCount(ibody), w.opts.allRoot, originalFInfo)
		w.inodeOrder = append(w.inodeOrder, path)
		w.fileSizes[path] = fi.Size()

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk source filesystem: %w", err)
	}

	return nil
}

func (w *writer) dataForInode(path string, ino any) (io.ReadCloser, int64, error) {
	var mode uint16
	switch ino := ino.(type) {
	case InodeCompact:
		mode = ino.Mode
	case InodeExtended:
		mode = ino.Mode
	default:
		return nil, 0, fmt.Errorf("unsupported inode type %T", ino)
	}

	switch mode & S_IFMT {
	case S_IFREG:
		f, err := w.src.Open(path)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to open file %q: %w", path, err)
		}

		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("failed to stat source file %q: %w", path, err)
		}

		return f, fi.Size(), nil

	case S_IFDIR:
		entries, err := fs.ReadDir(w.src, path)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read directory entries: %w", err)
		}

		// Add information about the directory itself.
		rootNid, err := w.findInodeAtPath(path)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to find inode for path %q: %w", path, err)
		}
		dirents := []Dirent{
			{
				Nid:      rootNid,
				FileType: uint8(fileTypeFromFileMode(fs.ModeDir)),
			},
		}
		names := []string{"."}

		// Add information about the parent directory.
		if path != "." {
			parentNid, err := w.findInodeAtPath(stdpath.Join(path, ".."))
			if err != nil {
				return nil, 0, fmt.Errorf("failed to find inode for path %q: %w", path, err)
			}
			dirents = append(dirents, Dirent{
				Nid:      parentNid,
				FileType: uint8(fileTypeFromFileMode(fs.ModeDir)),
			})
		} else {
			// The parent is the root directory itself in that case
			dirents = append(dirents, Dirent{
				Nid:      rootNid,
				FileType: uint8(fileTypeFromFileMode(fs.ModeDir)),
			})
		}
		names = append(names, "..")

		for _, de := range entries {
			childPath := stdpath.Join(path, de.Name())
			nid, err := w.findInodeAtPath(childPath)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to find inode for path %q: %w", childPath, err)
			}

			dirents = append(dirents, Dirent{
				Nid:      nid,
				FileType: uint8(fileTypeFromFileMode(de.Type())),
			})
			names = append(names, de.Name())
		}

		// EROFS requires directory entries in strict alphabetical order
		// for binary search lookup. Sort all entries (including . and ..)
		// by name. Previously . and .. were hardcoded at the front, which
		// broke lookup for filenames starting with characters before '.'
		// in ASCII (e.g. '#' = 0x23 < '.' = 0x2E).
		sortDirents(dirents, names)

		buf, err := encodeDirents(dirents, names)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to encode directory entries: %w", err)
		}

		return io.NopCloser(bytes.NewReader(buf)), int64(len(buf)), nil

	case S_IFLNK:
		fsys, ok := w.src.(fs.ReadLinkFS)
		if !ok {
			return nil, 0, fmt.Errorf("source filesystem must implement readLinkFS")
		}

		target, err := fsys.ReadLink(path)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read symlink target: %w", err)
		}

		return io.NopCloser(bytes.NewReader([]byte(target))), int64(len(target)), nil

	case S_IFBLK, S_IFCHR, S_IFIFO, S_IFSOCK:
		// Special files have no data.
		return nil, 0, nil

	default:
		return nil, 0, fmt.Errorf("unsupported file type %o", mode&S_IFMT)
	}
}

func (w *writer) findInodeAtPath(path string) (uint64, error) {
	cleanPath := stdpath.Clean(path)

	ino, ok := w.inodes[cleanPath]
	if !ok {
		return 0, fmt.Errorf("failed to find inode for path %q", path)
	}

	var nid uint32
	switch ino := ino.(type) {
	case InodeCompact:
		nid = ino.Ino
	case InodeExtended:
		nid = ino.Ino
	default:
		return 0, fmt.Errorf("unsupported inode type %T", ino)
	}

	return uint64(nid), nil
}

func toInode(fi fs.FileInfo, nlink int, icount uint16, allRoot bool, originalFInfo *FileInfo) any {
	var uid, gid int
	mode := fi.Mode()

	switch {
	case allRoot:
		uid, gid = 0, 0
	case originalFInfo != nil:
		uid = originalFInfo.Uid
		gid = originalFInfo.Gid
	default:
		uid, gid = archivefs.GetUID(fi.Sys()), archivefs.GetGID(fi.Sys())
	}

	// Clear permission bits from 'mode' and set the ones from originalFInfo.mode
	if originalFInfo != nil {
		mode = mode&^fs.ModePerm | originalFInfo.Mode.Perm()
	}

	// For block and character devices, RawBlockAddr stores the encoded device
	// number (EROFS new_encode_dev format). FIFOs and sockets use 0.
	var rdev uint32
	if modeType := mode.Type(); modeType&fs.ModeDevice != 0 {
		rdev = encodeDeviceID(
			archivefs.GetDevMajor(fi.Sys()),
			archivefs.GetDevMinor(fi.Sys()),
		)
	}

	compact := fi.Size() <= math.MaxUint32 &&
		uid <= math.MaxUint16 && gid <= math.MaxUint16 &&
		nlink <= math.MaxUint16 &&
		fi.ModTime().IsZero()

	if compact {
		return InodeCompact{
			Format:       setBits(0, InodeLayoutCompact, InodeLayoutBit, InodeLayoutBits),
			XattrCount:   icount,
			Mode:         statModeFromFileMode(mode),
			Nlink:        uint16(nlink),
			UID:          uint16(uid),
			GID:          uint16(gid),
			RawBlockAddr: rdev,
		}
	}

	return InodeExtended{
		Format:       setBits(0, InodeLayoutExtended, InodeLayoutBit, InodeLayoutBits),
		XattrCount:   icount,
		Mode:         statModeFromFileMode(mode),
		Nlink:        uint32(nlink),
		UID:          uint32(uid),
		GID:          uint32(gid),
		Mtime:        uint64(fi.ModTime().Unix()),
		MtimeNsec:    uint32(fi.ModTime().Nanosecond()),
		RawBlockAddr: rdev,
	}
}

func encodeDirents(dirents []Dirent, names []string) ([]byte, error) {
	if len(dirents) == 0 {
		return nil, fmt.Errorf("encodeDirents called with empty dirents slice")
	}
	if len(dirents) != len(names) {
		return nil, fmt.Errorf("encodeDirents: dirents and names length mismatch (%d vs %d)", len(dirents), len(names))
	}

	blocks := splitIntoDirentBlocks(dirents, names)

	var buf bytes.Buffer
	for i, block := range blocks {
		nameOff := uint16(int64(len(block.entries)) * DirentSize) // nameoff0

		// write the dirents
		for i, dirent := range block.entries {
			dirent.NameOff = nameOff
			nameOff += uint16(len(block.names[i]))

			if err := binary.Write(&buf, binary.LittleEndian, dirent); err != nil {
				return nil, fmt.Errorf("failed to write dirent: %w", err)
			}
		}

		// write the names
		for _, name := range block.names {
			if _, err := buf.WriteString(name); err != nil {
				return nil, fmt.Errorf("failed to write name: %w", err)
			}
		}

		// Null-terminate the final name.
		if err := buf.WriteByte(0); err != nil {
			return nil, fmt.Errorf("failed to write null terminator: %w", err)
		}

		if i < len(blocks)-1 {
			// Pad to the next block boundary.
			paddingBytes := roundUp(int64(buf.Len()), BlockSize) - int64(buf.Len())
			if _, err := buf.Write(make([]byte, paddingBytes)); err != nil {
				return nil, fmt.Errorf("failed to write padding: %w", err)
			}
		}
	}

	return buf.Bytes(), nil
}

type direntBlock struct {
	entries []Dirent
	names   []string
}

func splitIntoDirentBlocks(dirents []Dirent, names []string) []direntBlock {
	var blocks []direntBlock
	var currentBlock direntBlock
	currentBlockSize := int64(0)

	for i, dirent := range dirents {
		name := names[i]
		nameSize := int64(len(name))

		// Check if adding this dirent and name (plus null terminator)
		// exceeds the block size
		if currentBlockSize+DirentSize+nameSize+1 > BlockSize {
			// Start a new block
			blocks = append(blocks, currentBlock)
			currentBlock = direntBlock{}
			currentBlockSize = 0
		}

		// Add dirent and name to the current block
		currentBlock.entries = append(currentBlock.entries, dirent)
		currentBlock.names = append(currentBlock.names, name)
		currentBlockSize += DirentSize + nameSize
	}

	if len(currentBlock.entries) > 0 {
		blocks = append(blocks, currentBlock)
	}

	return blocks
}

func offsetToNID(metaOffset int64) (uint32, error) {
	// The inode number is the relative offset divided by the inode slot size.
	if metaOffset%InodeSlotSize != 0 {
		return 0, fmt.Errorf("offset %d is not properly aligned", metaOffset)
	}

	nid := uint32(metaOffset >> InodeSlotBits)
	return nid, nil
}

func isInlined(ino any) bool {
	return dataLayout(ino) == InodeDataLayoutFlatInline
}

func dataLayout(ino any) uint16 {
	switch ino := ino.(type) {
	case InodeCompact:
		return bitRange(ino.Format, InodeDataLayoutBit, InodeDataLayoutBits)
	case InodeExtended:
		return bitRange(ino.Format, InodeDataLayoutBit, InodeDataLayoutBits)
	}
	return InodeDataLayoutFlatPlain
}

// inlinePolicy decides how an inode's data is laid out. It returns
// inlined=true when all the data is stored with the inode metadata (no data
// blocks), tailPacked=true when the full blocks go to the data area and only
// the tail (size % BlockSize) is stored with the inode. When both are false
// the data lives entirely in the data area (flat plain).
//
// Regular file data is only inlined within the MaxInlineDataSize /
// MaxTailSize thresholds (currently 0: inline file data is incompatible with
// DAX, which requires block-aligned file data). Directory and symlink data is
// never mapped through DAX and is always inlined when it fits in the inode's
// block — matching mkfs.erofs, whose '-E noinline_data' option only affects
// regular files.
func inlinePolicy(mode uint16, size, inodeSize int64) (inlined, tailPacked bool) {
	if size == 0 {
		return false, false
	}

	nblocks := size / BlockSize
	tailSize := size % BlockSize

	switch mode & S_IFMT {
	case S_IFREG:
		if size <= MaxInlineDataSize {
			return true, false
		}
		if nblocks > 0 && tailSize > 0 && tailSize <= MaxTailSize {
			return false, true
		}
	case S_IFDIR, S_IFLNK:
		// The tail shares a block with the inode, so it must leave room for the inode.
		if tailSize == 0 || tailSize > BlockSize-inodeSize {
			return false, false
		}
		if nblocks == 0 {
			return true, false
		}
		return false, true
	}

	return false, false
}

// hasDataBlocks reports whether the inode has blocks in the data area: a plain inode, or a tail-packed inode.
func hasDataBlocks(ino any) bool {
	var size uint64
	switch v := ino.(type) {
	case InodeCompact:
		size = uint64(v.Size)
	case InodeExtended:
		size = v.Size
	default:
		return false
	}
	layout := dataLayout(ino)
	return layout == InodeDataLayoutFlatPlain || layout == InodeDataLayoutFlatInline && size >= BlockSize
}

func marshalInode(ino any) ([]byte, error) {
	var buf bytes.Buffer
	switch v := ino.(type) {
	case InodeCompact:
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	case InodeExtended:
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported inode type %T", ino)
	}
	return buf.Bytes(), nil
}

// checksumSuperBlock calculates the checksum from the superblock to the end of block 0.
func (w *writer) checksumSuperBlock(sb *SuperBlock) error {
	sbCopy := *sb
	sbCopy.Checksum = 0

	var marshalled bytes.Buffer
	if err := binary.Write(&marshalled, binary.LittleEndian, sbCopy); err != nil {
		return err
	}

	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(marshalled.Bytes(), table)

	// The checksum also covers the rest of block 0 after the superblock.
	tailStart := SuperBlockOffset + int64(marshalled.Len())
	checksum = ^crc32.Update(checksum, table, w.dst.block0[tailStart:])

	sb.Checksum = checksum

	return nil
}

// isSpecialFile reports whether the inode mode represents a device file,
// FIFO, or socket — file types that have no data blocks and whose RawBlockAddr
// field holds a device number rather than a block address.
func isSpecialFile(mode uint16) bool {
	t := mode & S_IFMT
	return t == S_IFBLK || t == S_IFCHR || t == S_IFIFO || t == S_IFSOCK
}

// encodeDeviceID encodes a major/minor device number pair into the 32-bit format
// used by EROFS (equivalent to the kernel's new_encode_dev).
func encodeDeviceID(major, minor uint32) uint32 {
	return (minor & 0xff) | (major << 8) | ((minor &^ uint32(0xff)) << 12)
}

// decodeDeviceID decodes a 32-bit EROFS device number into its major and minor
// components (equivalent to the kernel's new_decode_dev).
func decodeDeviceID(dev uint32) (major, minor uint32) {
	major = (dev & 0x000fff00) >> 8
	minor = (dev & 0xff) | ((dev >> 12) & 0xfff00)
	return
}

func setBits(value, newValue, bit, bits uint16) uint16 {
	mask := uint16((1<<bits)-1) << bit
	return (value & ^mask) | ((newValue << bit) & mask)
}

func roundUp(x, align int64) int64 {
	if x%align == 0 {
		return x
	}

	return (x + align - 1) &^ (align - 1)
}

// sortDirents sorts dirents and names together by name, so that directory
// entries are in strict alphabetical order as required by EROFS.
func sortDirents(dirents []Dirent, names []string) {
	type pair struct {
		d Dirent
		n string
	}
	pairs := make([]pair, len(dirents))
	for i := range dirents {
		pairs[i] = pair{dirents[i], names[i]}
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		return strings.Compare(a.n, b.n)
	})
	for i, p := range pairs {
		dirents[i] = p.d
		names[i] = p.n
	}
}
