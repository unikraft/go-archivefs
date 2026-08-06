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
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	stdpath "path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/unikraft/go-archivefs"
)

const (
	BlockSize     = 4096
	BlockSizeBits = 12
	InodeSlotSize = 1 << InodeSlotBits
	// MaxInlineDataSize is the threshold for inlining small regular files.
	// Files up to this size are stored inline with the inode metadata. Set to
	// 0 to represent the flag '-E noinline_data': inline file data is
	// incompatible with DAX, which requires file data to be block-aligned.
	// Directory and symlink data is not affected by this threshold (it is not
	// mapped through DAX and is always inlined when it fits, matching
	// mkfs.erofs behavior).
	MaxInlineDataSize = 0
	// MaxTailSize is the maximum tail size (size % BlockSize) that is inlined
	// with the inode for regular files larger than one block (tail-packing).
	// Set to 0 to disable tail-packing for file data, which is likewise
	// incompatible with DAX.
	MaxTailSize = 0
)

// Create creates an EROFS filesystem image from the source filesystem and writes
// it to the destination writer.
func Create(dst io.WriterAt, src fs.FS, opts ...ErofsCreateOption) error {
	w := &writer{
		src: src,
		dst: dst,
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
	dst        io.WriterAt
	inodes     map[string]any
	inodeOrder []string
	fileSizes  map[string]int64
	linkMap    map[uint64]inodeCount
	opts       ErofsCreateOptions
	// block0 mirrors the first block of the image. Metadata shares block 0
	// with the superblock, and the superblock checksum covers the whole
	// block, so we need a copy of what was written there.
	block0 []byte
}

type inodeCount struct {
	Count        int
	Inode        uint64
	RawBlockAddr uint32 // The block address for the first occurrence of this hardlink
}

func (w *writer) write() error {
	w.linkMap = map[uint64]inodeCount{}

	if err := w.populateInodes(); err != nil {
		return fmt.Errorf("failed to populate inodes: %w", err)
	}

	metaSize, dataSize, err := w.firstPass()
	if err != nil {
		return fmt.Errorf("failed to calculate metadata and data size: %w", err)
	}

	// Metadata starts at block 0. The superblock lives at offset 1024 within
	// block 0 and firstPass reserved space for it, so inodes can share the
	// first block with the superblock. This matches the mkfs.erofs behavior.
	metaBlockAddr := int64(0)

	// Zero out block 0 before writing metadata, since the superblock checksum
	// covers the whole block and the destination may contain stale data.
	if _, err := w.dst.WriteAt(make([]byte, BlockSize), 0); err != nil {
		return fmt.Errorf("failed to zero block 0: %w", err)
	}
	w.block0 = make([]byte, BlockSize)

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

	// Generate a UUID for the filesystem.
	uuidBytes, err := uuid.New().MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	var uuid [16]uint8
	copy(uuid[:], uuidBytes)

	timeNow := time.Now()

	// Count unique inodes: hard links share an inode, so counting paths would
	// overcount.
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
		UUID:          uuid,
		BuildTime:     uint64(timeNow.Unix()),
		BuildTimeNsec: uint32(timeNow.Nanosecond()),
		FeatureCompat: EROFS_FEATURE_COMPAT_SB_CHKSUM | EROFS_FEATURE_COMPAT_MTIME,
		// TODO: other fields (volume name, etc.)
	}

	if err := w.checksumSuperBlock(&sb); err != nil {
		return fmt.Errorf("failed to calculate superblock checksum: %w", err)
	}

	if err := binary.Write(io.NewOffsetWriter(w.dst, SuperBlockOffset), binary.LittleEndian, &sb); err != nil {
		return fmt.Errorf("failed to write superblock: %w", err)
	}

	if f, ok := w.dst.(*os.File); ok {
		if err := f.Truncate(int64(sb.Blocks) * BlockSize); err != nil {
			return fmt.Errorf("failed to truncate destination file: %w", err)
		}
	}

	return nil
}

// firstPass precomputes the layout of the blocks, and inodes.
func (w *writer) firstPass() (metaSize, dataSize int64, err error) {
	// Metadata shares block 0 with the superblock: reserve the space up to and
	// including the superblock (at offset 1024) before allocating inodes.
	metaSize = roundUp(SuperBlockOffset+int64(binary.Size(SuperBlock{})), InodeSlotSize)

	for _, path := range w.inodeOrder {
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

		// Resolve the source inode number up front for hard-link detection.
		// Directories and special files are never deduplicated.
		var fsIno uint64
		if mode&S_IFMT != S_IFDIR && !isSpecialFile(mode) {
			fsys, ok := w.src.(fs.ReadLinkFS)
			if !ok {
				return metaSize, dataSize, fmt.Errorf("source filesystem must implement readLinkFS")
			}

			info, err := fsys.Lstat(path)
			if err != nil {
				return metaSize, dataSize, fmt.Errorf("failed to stat file %q: %w", path, err)
			}
			fsIno = archivefs.GetIno(info.Sys())
		}

		// Decide the data layout: fully inline (all data with the inode),
		// tail-packed (full blocks in the data area, tail with the inode),
		// or plain (all data in the data area).
		inodeSize := int64(binary.Size(ino))
		nblocks := size / BlockSize
		tailSize := size % BlockSize
		inlined, useTailPacking := inlinePolicy(mode, size, inodeSize)

		// Subsequent occurrences of a hard link reuse the first occurrence's
		// inode: they consume no metadata slot and no data blocks.
		if fsIno != 0 {
			if entry, ok := w.linkMap[fsIno]; ok && entry.Count > 0 {
				entry.Count++
				w.linkMap[fsIno] = entry

				layout := uint16(InodeDataLayoutFlatPlain)
				if inlined || useTailPacking {
					layout = InodeDataLayoutFlatInline
				}

				switch ino := ino.(type) {
				case InodeCompact:
					ino.Ino = uint32(entry.Inode)
					ino.Size = uint32(size)
					ino.RawBlockAddr = entry.RawBlockAddr
					ino.Format = setBits(ino.Format, layout, InodeDataLayoutBit, InodeDataLayoutBits)
					w.inodes[path] = ino
				case InodeExtended:
					ino.Ino = uint32(entry.Inode)
					ino.Size = uint64(size)
					ino.RawBlockAddr = entry.RawBlockAddr
					ino.Format = setBits(ino.Format, layout, InodeDataLayoutBit, InodeDataLayoutBits)
					w.inodes[path] = ino
				default:
					return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
				}

				continue
			}
		}

		// The inode and its inline data (whole file or tail) must not cross a
		// block boundary: pad to the next block first if they would.
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

		// Track if this is the first occurrence of a hardlinked file
		shouldAllocate := false
		rawBlockAssigned := false

		switch ino := ino.(type) {
		case InodeCompact:
			if ino.Mode&S_IFMT == S_IFDIR {
				ino.Ino = nid
				ino.Size = uint32(size)
				shouldAllocate = true
			} else if isSpecialFile(ino.Mode) {
				// Device files, FIFOs, and sockets have no file data.
				ino.Ino = nid
				ino.Size = 0
				rawBlockAssigned = true
			} else {
				// Non-directory files: first occurrence of each source inode
				// (subsequent hard links were short-circuited above).
				if fsIno == 0 {
					// No valid inode info; skip deduplication.
					ino.Ino = nid
					ino.Size = uint32(size)
					shouldAllocate = true
				} else if entry, ok := w.linkMap[fsIno]; ok {
					ino.Ino = nid
					entry.Count = 1
					entry.Inode = uint64(ino.Ino)
					entry.RawBlockAddr = uint32(dataSize / BlockSize)
					shouldAllocate = true
					ino.Size = uint32(size)
					ino.RawBlockAddr = entry.RawBlockAddr
					rawBlockAssigned = true

					w.linkMap[fsIno] = entry
				} else {
					return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
				}
			}
			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
			} else {
				if useTailPacking {
					// Tail-packed files keep their full blocks in the data
					// area and inline the tail with the inode.
					ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
				} else {
					ino.Format = setBits(ino.Format, InodeDataLayoutFlatPlain, InodeDataLayoutBit, InodeDataLayoutBits)
				}
				// Assign a block address when hardlink logic didn't set one (directories).
				if !rawBlockAssigned {
					ino.RawBlockAddr = uint32(dataSize / BlockSize)
				}
			}
			w.inodes[path] = ino

		case InodeExtended:
			if ino.Mode&S_IFMT == S_IFDIR {
				ino.Ino = nid
				ino.Size = uint64(size)
				shouldAllocate = true
			} else if isSpecialFile(ino.Mode) {
				// Device files, FIFOs, and sockets have no file data. RawBlockAddr
				// was set by toInode to the encoded device number and must not be
				// overwritten here or adjusted in the fixup pass.
				ino.Ino = nid
				ino.Size = 0
				rawBlockAssigned = true
			} else {
				// Non-directory files: first occurrence of each source inode
				// (subsequent hard links were short-circuited above).
				if fsIno == 0 {
					// No valid inode info; skip deduplication.
					ino.Ino = nid
					ino.Size = uint64(size)
					shouldAllocate = true
				} else if entry, ok := w.linkMap[fsIno]; ok {
					ino.Ino = nid
					entry.Count = 1
					entry.Inode = uint64(ino.Ino)
					entry.RawBlockAddr = uint32(dataSize / BlockSize)
					shouldAllocate = true
					ino.Size = uint64(size)
					ino.RawBlockAddr = entry.RawBlockAddr
					rawBlockAssigned = true

					w.linkMap[fsIno] = entry
				} else {
					return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
				}
			}
			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
			} else {
				if useTailPacking {
					// Tail-packed files keep their full blocks in the data
					// area and inline the tail with the inode.
					ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
				} else {
					ino.Format = setBits(ino.Format, InodeDataLayoutFlatPlain, InodeDataLayoutBit, InodeDataLayoutBits)
				}
				// Assign a block address when hardlink logic didn't set one (directories).
				if !rawBlockAssigned {
					ino.RawBlockAddr = uint32(dataSize / BlockSize)
				}
			}
			w.inodes[path] = ino

		default:
			return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
		}

		metaSize += int64(binary.Size(ino))

		if inlined {
			metaSize += size
			metaSize = roundUp(metaSize, InodeSlotSize)
		} else if shouldAllocate {
			// Only allocate data space for the first occurrence of each
			// hardlinked file, and every directory.
			if useTailPacking {
				// Full blocks go to the data area; the tail lives with the
				// inode in the metadata area.
				dataSize += nblocks * BlockSize
				metaSize += tailSize
				metaSize = roundUp(metaSize, InodeSlotSize)
			} else {
				dataSize += size
				dataSize = roundUp(dataSize, BlockSize)
			}
		}
	}

	metaSize = roundUp(metaSize, BlockSize)

	// Data blocks start immediately after the metadata blocks. Metadata
	// starts at block 0, so the data area begins at metaSize / BlockSize.
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
	// Hard links share a single inode; write each inode (and its inline
	// data) only once.
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
		w.updateBlock0(off, inodeBytes)

		// Fully inlined files store all their data alongside the inode;
		// tail-packed files store only the final partial block there.
		if isInlined(ino) {
			data, _, err := w.dataForInode(path, ino)
			if err != nil {
				return fmt.Errorf("failed to get data for %q: %w", path, err)
			}

			// Skip the full blocks (they are written to the data area) and
			// inline only the tail. Fully inlined data has no full blocks,
			// so everything after this is the whole file.
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

			// The inline data must stay within the inode's block.
			inlineOff := off + int64(len(inodeBytes))
			blockEnd := (off/BlockSize + 1) * BlockSize
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
			w.updateBlock0(inlineOff, inlineBuf)
		}
	}

	return nil
}

func (w *writer) writeData() error {
	// Track which inodes have already had data written (for hardlink deduplication)
	// Only track regular files, as directories are unique per inode.
	writtenInodes := make(map[uint32]bool)

	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		if !hasDataBlocks(ino) {
			// Fully inlined files are stored alongside their inode.
			continue
		}

		var rawBlockAddr uint32
		var mode uint16
		var nid uint32
		switch ino := ino.(type) {
		case InodeCompact:
			rawBlockAddr = ino.RawBlockAddr
			mode = ino.Mode
			nid = ino.Ino
		case InodeExtended:
			rawBlockAddr = ino.RawBlockAddr
			mode = ino.Mode
			nid = ino.Ino
		default:
			return fmt.Errorf("unsupported inode type %T", ino)
		}

		// Skip if we've already written data for this inode (hardlink case)
		if mode&S_IFMT == S_IFREG && writtenInodes[nid] {
			continue
		}

		data, size, err := w.dataForInode(path, ino)
		if err != nil {
			return fmt.Errorf("failed to get data for %q: %w", path, err)
		}
		if data == nil {
			continue
		}

		if isInlined(ino) {
			// Tail-packed: only the full blocks live in the data area; the
			// tail was written alongside the inode.
			nblocks := size / BlockSize
			if nblocks > 0 {
				_, err = io.CopyN(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data, nblocks*BlockSize)
			}
		} else {
			_, err = io.Copy(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data)
		}
		_ = data.Close()
		if err != nil {
			return fmt.Errorf("failed to write data for %q: %w", path, err)
		}

		// Mark this inode's data as written (only for regular files)
		if mode&S_IFMT == S_IFREG {
			writtenInodes[nid] = true
		}
	}

	return nil
}

func (w *writer) populateInodes() error {
	w.inodes = map[string]any{}
	w.fileSizes = map[string]int64{}

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

			nlink = len(entries) + 2
		} else {
			nlink = int(archivefs.GetNlink(fi.Sys()))
			ino := archivefs.GetIno(fi.Sys())
			if ino != 0 {
				if _, ok := w.linkMap[ino]; !ok {
					w.linkMap[ino] = inodeCount{
						Count: 0,
					}
				}
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

		w.inodes[path] = toInode(fi, nlink, w.opts.allRoot, originalFInfo)
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

func toInode(fi fs.FileInfo, nlink int, allRoot bool, originalFInfo *FileInfo) any {
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
			Mode:         statModeFromFileMode(mode),
			Nlink:        uint16(nlink),
			UID:          uint16(uid),
			GID:          uint16(gid),
			RawBlockAddr: rdev,
		}
	}

	return InodeExtended{
		Format:       setBits(0, InodeLayoutExtended, InodeLayoutBit, InodeLayoutBits),
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
	var format uint16
	switch ino := ino.(type) {
	case InodeCompact:
		format = ino.Format
	case InodeExtended:
		format = ino.Format
	default:
		return false
	}

	return bitRange(format, InodeDataLayoutBit, InodeDataLayoutBits) == InodeDataLayoutFlatInline
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
		// The inline data shares a block with the inode, so the tail must
		// leave room for the inode itself; otherwise fall back to plain
		// blocks.
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

// hasDataBlocks reports whether the inode owns blocks in the data area:
// all flat-plain inodes do, and flat-inline inodes do when they are
// tail-packed (at least one full block), since their full blocks live in
// the data area and only the tail is inline.
func hasDataBlocks(ino any) bool {
	switch v := ino.(type) {
	case InodeCompact:
		layout := bitRange(v.Format, InodeDataLayoutBit, InodeDataLayoutBits)
		if layout == InodeDataLayoutFlatPlain {
			return true
		}
		if layout == InodeDataLayoutFlatInline {
			return v.Size >= BlockSize
		}
	case InodeExtended:
		layout := bitRange(v.Format, InodeDataLayoutBit, InodeDataLayoutBits)
		if layout == InodeDataLayoutFlatPlain {
			return true
		}
		if layout == InodeDataLayoutFlatInline {
			return v.Size >= BlockSize
		}
	}
	return false
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

// updateBlock0 mirrors writes that land within the first block into the
// shadow copy used for the superblock checksum.
func (w *writer) updateBlock0(off int64, data []byte) {
	if w.block0 == nil || len(data) == 0 || off < 0 || off >= BlockSize {
		return
	}
	end := off + int64(len(data))
	if end > BlockSize {
		end = BlockSize
	}
	copy(w.block0[off:end], data[:end-off])
}

// checksumSuperBlock calculates the superblock checksum over the superblock
// itself and the remainder of block 0, which may contain inode metadata.
func (w *writer) checksumSuperBlock(sb *SuperBlock) error {
	sbCopy := *sb
	sbCopy.Checksum = 0

	var marshalled bytes.Buffer
	if err := binary.Write(&marshalled, binary.LittleEndian, sbCopy); err != nil {
		return err
	}

	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(marshalled.Bytes(), table)

	// Include the rest of block 0 after the superblock, using the shadow
	// copy of what was actually written there.
	tailStart := SuperBlockOffset + int64(marshalled.Len())
	tail := make([]byte, BlockSize-tailStart)
	if w.block0 != nil {
		copy(tail, w.block0[tailStart:])
	}
	checksum = ^crc32.Update(checksum, table, tail)

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
