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
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	BlockSize     = 4096
	BlockSizeBits = 12
	InodeSlotSize = 1 << InodeSlotBits
	// MaxInlineDataSize is the threshold for inlining small files.
	// Files up to this size will be stored inline with the inode metadata.
	MaxInlineDataSize = 0 // DISABLED for DAX
	// MaxTailSize is the maximum tail size that can be inlined with an inode.
	// Must account for the largest inode size (InodeExtended = 64 bytes).
	MaxTailSize = 0 // DISABLED for DAX
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
	src               fs.FS
	dst               io.WriterAt
	inodes            map[string]any
	inodeOrder        []string
	opts              ErofsCreateOptions
	linkMap           map[uint64]inodeCount
	writtenDataBlocks map[uint32]bool
	writtenMetaInodes map[uint32]bool
	block0            []byte
}

type inodeCount struct {
	Count int
	Inode uint64
}

func (w *writer) write() error {
	w.linkMap = map[uint64]inodeCount{}

	if err := w.populateInodes(); err != nil {
		return fmt.Errorf("failed to populate inodes: %w", err)
	}

	w.writtenDataBlocks = make(map[uint32]bool)
	w.writtenMetaInodes = make(map[uint32]bool)

	metaSize, dataSize, err := w.firstPass()
	if err != nil {
		return fmt.Errorf("failed to calculate metadata and data size: %w", err)
	}

	// Metadata starts at block 0. The superblock is at offset 1024 within block 0,
	// so metadata can start at the beginning of block 0 (before the superblock).
	// This matches the mkfs.erofs behavior.
	metaBlockAddr := int64(0)

	// Zero out block 0 before writing metadata, since the superblock checksum
	// assumes the rest of the block is zeros.
	zeroBlock := make([]byte, BlockSize)
	if _, err := w.dst.WriteAt(zeroBlock, 0); err != nil {
		return fmt.Errorf("failed to zero block 0: %w", err)
	}
	w.block0 = make([]byte, BlockSize)

	if err := w.writeMetadata(metaBlockAddr); err != nil {
		return fmt.Errorf("failed to write metadata blocks: %w", err)
	}

	if err := w.writeData(); err != nil {
		return fmt.Errorf("failed to write data blocks: %w", err)
	}

	// Generate a UUID for the filesystem.
	uuidBytes, err := uuid.New().MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	var uuid [16]uint8
	copy(uuid[:], uuidBytes)

	timeNow := time.Now()

	// Get the root inode number (should be the "." directory)
	rootNid, err := w.findInodeAtPath(".")
	if err != nil {
		return fmt.Errorf("failed to find root inode: %w", err)
	}

	// Count unique inodes (not paths, since hard links share inodes)
	uniqueInodes := make(map[uint32]bool)
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]
		var nid uint32
		switch ino := ino.(type) {
		case InodeCompact:
			nid = ino.Ino
		case InodeExtended:
			nid = ino.Ino
		}
		uniqueInodes[nid] = true
	}

	sb := SuperBlock{
		Magic:         SuperBlockMagicV1,
		BlockSizeBits: BlockSizeBits,
		RootNid:       uint16(rootNid),
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
	// Reserve space for the superblock. The superblock is at offset 1024 and
	// is 128 bytes, so we need to start metadata allocation after it.
	sbSize := int64(binary.Size(SuperBlock{}))
	metaSize = roundUp(SuperBlockOffset+sbSize, InodeSlotSize)

	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		data, size, err := w.dataForInode(path, ino)
		if err != nil {
			return metaSize, dataSize, fmt.Errorf("failed to get data for %q: %w", path, err)
		}
		_ = data.Close()

		// Check if this is a hard link (non-directory, non-first occurrence)
		isHardLink := false
		var mode uint16
		switch ino := ino.(type) {
		case InodeCompact:
			mode = ino.Mode
		case InodeExtended:
			mode = ino.Mode
		}

		if mode&S_IFMT != S_IFDIR {
			fsys, ok := w.src.(fs.ReadLinkFS)
			if !ok {
				return metaSize, dataSize, fmt.Errorf("source filesystem must implement readLinkFS")
			}

			info, err := fsys.Lstat(path)
			if err != nil {
				return metaSize, dataSize, fmt.Errorf("failed to stat file %q: %w", path, err)
			}
			fsIno := getIno(info)

			if fsIno != 0 {
				if entry, ok := w.linkMap[fsIno]; ok {
					isHardLink = (entry.Count > 0)
				}
			}
		}

		if isHardLink {
			fsys, _ := w.src.(fs.ReadLinkFS)
			info, _ := fsys.Lstat(path)
			fsIno := getIno(info)
			entry := w.linkMap[fsIno]

			// For hard links, we need to reuse the SAME inode structure from the first occurrence,
			// not create a new one. This ensures all hard links share the same inode data.
			firstPath := w.findFirstOccurrenceOfInode(entry.Inode)
			if firstIno := w.inodes[firstPath]; firstIno != nil {
				w.inodes[path] = firstIno
			} else {
				return metaSize, dataSize, fmt.Errorf("first inode for hard link %q not found", path)
			}

			entry.Count++
			w.linkMap[fsIno] = entry

			continue
		}

		// Not a hard link - allocate space normally
		inlined := size <= MaxInlineDataSize

		inodeSize := int64(binary.Size(ino))

		if inlined {
			// Check if the inode + inline data would cross a block boundary.
			// If so, pad to the next block first to avoid crossing boundaries.
			currentBlockEnd := roundUp(metaSize, BlockSize)
			spaceInCurrentBlock := currentBlockEnd - metaSize

			if inodeSize+size > spaceInCurrentBlock {
				// Would cross block boundary - pad to next block first
				metaSize = currentBlockEnd
			}
		} else {
			var mode uint16
			switch ino := ino.(type) {
			case InodeCompact:
				mode = ino.Mode
			case InodeExtended:
				mode = ino.Mode
			}
			isDir := (mode & S_IFMT) == S_IFDIR

			// For tail-packing (regular files only), check if inode + tail would cross boundary
			// Only pad if we're actually going to use tail-packing (tail small enough)
			if !isDir {
				tailSize := size % BlockSize
				if tailSize > 0 && tailSize <= MaxTailSize {
					currentBlockEnd := roundUp(metaSize, BlockSize)
					spaceInCurrentBlock := currentBlockEnd - metaSize

					if inodeSize+tailSize > spaceInCurrentBlock {
						// Would cross block boundary - pad to next block first
						metaSize = currentBlockEnd
					}
				}
			}
		}

		// Allocate the inode number.
		nid, err := offsetToNID(metaSize)
		if err != nil {
			return metaSize, dataSize, fmt.Errorf("failed to convert offset to inode number: %w", err)
		}

		switch ino := ino.(type) {
		case InodeCompact:
			if ino.Mode&S_IFMT == S_IFDIR {
				ino.Ino = nid
				ino.Size = uint32(size)
			} else {
				fsys, ok := w.src.(fs.ReadLinkFS)
				if !ok {
					return metaSize, dataSize, fmt.Errorf("source filesystem must implement readLinkFS")
				}

				info, err := fsys.Lstat(path)
				if err != nil {
					return metaSize, dataSize, fmt.Errorf("failed to stat file %q: %w", path, err)
				}
				fsIno := getIno(info)

				if fsIno != 0 {
					if entry, ok := w.linkMap[fsIno]; ok {
						ino.Ino = nid
						entry.Count = 1
						entry.Inode = uint64(ino.Ino)
						ino.Size = uint32(size)
						w.linkMap[fsIno] = entry
					} else {
						return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
					}
				} else {
					// No inode info available, treat as unique file
					ino.Ino = nid
					ino.Size = uint32(size)
				}
			}

			var mode uint16 = ino.Mode
			isDir := (mode & S_IFMT) == S_IFDIR
			nblocks := size / BlockSize
			tailSize := size % BlockSize
			// Tail-packing only makes sense if there are full blocks to pack separately
			// Files with only a tail (nblocks=0) should use full blocks instead
			useTailPacking := !isDir && nblocks > 0 && tailSize > 0 && tailSize <= MaxTailSize

			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
			} else if useTailPacking {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			} else {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatPlain, InodeDataLayoutBit, InodeDataLayoutBits)
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			}
			w.inodes[path] = ino

		case InodeExtended:
			if ino.Mode&S_IFMT == S_IFDIR {
				ino.Ino = nid
				ino.Size = uint64(size)
			} else {
				fsys, ok := w.src.(fs.ReadLinkFS)
				if !ok {
					return metaSize, dataSize, fmt.Errorf("source filesystem must implement readLinkFS")
				}

				info, err := fsys.Lstat(path)
				if err != nil {
					return metaSize, dataSize, fmt.Errorf("failed to stat file %q: %w", path, err)
				}
				fsIno := getIno(info)

				if fsIno != 0 {
					if entry, ok := w.linkMap[fsIno]; ok {
						ino.Ino = nid
						entry.Count = 1
						entry.Inode = uint64(ino.Ino)
						ino.Size = uint64(size)
						w.linkMap[fsIno] = entry
					} else {
						return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
					}
				} else {
					// No inode info available, treat as unique file
					ino.Ino = nid
					ino.Size = uint64(size)
				}
			}

			var mode uint16 = ino.Mode
			isDir := (mode & S_IFMT) == S_IFDIR
			nblocks := size / BlockSize
			tailSize := size % BlockSize
			// Tail-packing only makes sense if there are full blocks to pack separately
			// Files with only a tail (nblocks=0) should use full blocks instead
			useTailPacking := !isDir && nblocks > 0 && tailSize > 0 && tailSize <= MaxTailSize

			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
			} else if useTailPacking {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			} else {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatPlain, InodeDataLayoutBit, InodeDataLayoutBits)
				ino.RawBlockAddr = uint32(dataSize / BlockSize)
			}
			w.inodes[path] = ino

		default:
			return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
		}

		metaSize += int64(binary.Size(ino))

		if inlined {
			metaSize += size
			metaSize = roundUp(metaSize, InodeSlotSize)
		} else {
			var mode uint16
			switch ino := ino.(type) {
			case InodeCompact:
				mode = ino.Mode
			case InodeExtended:
				mode = ino.Mode
			}
			isDir := (mode & S_IFMT) == S_IFDIR

			if !isDir {
				// Use data blocks for full blocks, inline the tail (if any)
				nblocks := size / BlockSize
				tailSize := size % BlockSize

				if nblocks > 0 && tailSize > 0 && tailSize <= MaxTailSize {
					dataSize += nblocks * BlockSize
					dataSize = roundUp(dataSize, BlockSize)
					metaSize += tailSize
					metaSize = roundUp(metaSize, InodeSlotSize)
				} else {
					dataSize += size
					dataSize = roundUp(dataSize, BlockSize)
				}
			} else {
				// Directories use full blocks without tail-packing
				dataSize += size
				dataSize = roundUp(dataSize, BlockSize)
			}
		}
	}

	metaSize = roundUp(metaSize, BlockSize)

	// Data blocks start immediately after metadata blocks.
	// Since metadata starts at block 0, data starts at (metaSize / BlockSize).
	dataBlockAddr := metaSize / BlockSize

	// fix up the raw block addresses now that we know the total size of the
	// metadata space.
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		switch ino := ino.(type) {
		case InodeCompact:
			if hasDataBlocks(ino) {
				ino.RawBlockAddr += uint32(dataBlockAddr)
				w.inodes[path] = ino
			}
		case InodeExtended:
			if hasDataBlocks(ino) {
				ino.RawBlockAddr += uint32(dataBlockAddr)
				w.inodes[path] = ino
			}
		default:
			return metaSize, dataSize, fmt.Errorf("unsupported inode type %T", ino)
		}
	}

	return
}

func (w *writer) writeMetadata(metaBlockAddr int64) error {
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		var nid uint32
		switch ino := ino.(type) {
		case InodeCompact:
			nid = ino.Ino
		case InodeExtended:
			nid = ino.Ino
		default:
			return fmt.Errorf("unsupported inode type %T", ino)
		}

		if w.writtenMetaInodes[nid] {
			continue
		}

		// Get the address of the inode.
		off := metaBlockAddr*BlockSize + int64(nid)*InodeSlotSize

		inodeBytes, err := marshalInode(ino)
		if err != nil {
			return fmt.Errorf("failed to marshal inode for %q: %w", path, err)
		}

		if _, err := io.NewOffsetWriter(w.dst, off).Write(inodeBytes); err != nil {
			return fmt.Errorf("failed to write inode for %q: %w", path, err)
		}
		w.updateBlock0(off, inodeBytes)

		w.writtenMetaInodes[nid] = true

		// Handle inline data
		inodeSize := int64(len(inodeBytes))

		if isInlined(ino) {
			data, _, err := w.dataForInode(path, ino)
			if err != nil {
				return fmt.Errorf("failed to get data for %q: %w", path, err)
			}

			// Check file size and type
			var fileSize int64
			var mode uint16
			switch ino := ino.(type) {
			case InodeCompact:
				fileSize = int64(ino.Size)
				mode = ino.Mode
			case InodeExtended:
				fileSize = int64(ino.Size)
				mode = ino.Mode
			}
			isDir := (mode & S_IFMT) == S_IFDIR

			// For fully inlined files, write all data inline
			// For tail-packed files (regular files only, not directories), only write the tail
			var inlineData io.Reader

			if fileSize <= MaxInlineDataSize || isDir {
				inlineData = data
			} else {
				nblocks := fileSize / BlockSize
				tailSize := fileSize % BlockSize

				if tailSize > 0 {
					if nblocks > 0 {
						_, err := io.CopyN(io.Discard, data, nblocks*BlockSize)
						if err != nil {
							_ = data.Close()
							return fmt.Errorf("failed to skip full blocks for %q: %w", path, err)
						}
					}
					inlineData = data
				} else {
					_ = data.Close()
					continue
				}
			}

			inodeOff := off
			inlineDataStart := inodeOff + inodeSize

			var inlineDataSize int64
			if fileSize <= MaxInlineDataSize || isDir {
				inlineDataSize = fileSize
			} else {
				inlineDataSize = fileSize % BlockSize
			}

			// Check if inline data would cross block boundary
			blockStart := (inodeOff / BlockSize) * BlockSize
			blockEnd := blockStart + BlockSize
			inlineDataEnd := inlineDataStart + inlineDataSize

			if inlineDataEnd > blockEnd {
				return fmt.Errorf("inline data would cross block boundary for %q: inode at %d, inline data %d-%d, block %d-%d",
					path, inodeOff, inlineDataStart, inlineDataEnd, blockStart, blockEnd)
			}

			if inlineDataSize > 0 {
				if inlineDataSize > int64(math.MaxInt) {
					_ = data.Close()
					return fmt.Errorf("inline data too large (%d bytes) for %q", inlineDataSize, path)
				}
				bufLen := int(inlineDataSize)
				inlineBuf := make([]byte, bufLen)
				if _, err := io.ReadFull(inlineData, inlineBuf); err != nil {
					_ = data.Close()
					return fmt.Errorf("failed to read inline data for %q: %w", path, err)
				}

				inlineOff := off + inodeSize
				if _, err := io.NewOffsetWriter(w.dst, inlineOff).Write(inlineBuf); err != nil {
					_ = data.Close()
					return fmt.Errorf("failed to write inline data for %q: %w", path, err)
				}
				w.updateBlock0(inlineOff, inlineBuf)
			}
			_ = data.Close()
		}
	}

	return nil
}

func (w *writer) writeData() error {
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		if !hasDataBlocks(ino) {
			// Small files are stored in the inline with the inode.
			continue
		}

		var rawBlockAddr uint32
		switch ino := ino.(type) {
		case InodeCompact:
			rawBlockAddr = ino.RawBlockAddr
		case InodeExtended:
			rawBlockAddr = ino.RawBlockAddr
		default:
			return fmt.Errorf("unsupported inode type %T", ino)
		}

		// Skip if this data block has already been written (hard link case)
		if w.writtenDataBlocks[rawBlockAddr] {
			continue
		}

		data, size, err := w.dataForInode(path, ino)
		if err != nil {
			return fmt.Errorf("failed to get data for %q: %w", path, err)
		}

		var format uint16
		switch ino := ino.(type) {
		case InodeCompact:
			format = ino.Format
		case InodeExtended:
			format = ino.Format
		}
		isTailPacked := bitRange(format, InodeDataLayoutBit, InodeDataLayoutBits) == InodeDataLayoutFlatInline

		// For tail-packed files, only write full blocks (tail is inlined)
		// For plain files, write all data
		if isTailPacked {
			nblocks := size / BlockSize
			if nblocks > 0 {
				bytesToWrite := nblocks * BlockSize
				_, err = io.CopyN(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data, bytesToWrite)
				if err != nil {
					_ = data.Close()
					return fmt.Errorf("failed to write data for %q: %w", path, err)
				}
			}
		} else {
			offset := int64(rawBlockAddr) * BlockSize

			_, err = io.Copy(io.NewOffsetWriter(w.dst, offset), data)
			if err != nil {
				_ = data.Close()
				return fmt.Errorf("failed to write data for %q: %w", path, err)
			}
		}
		_ = data.Close()

		w.writtenDataBlocks[rawBlockAddr] = true
	}

	return nil
}

func (w *writer) populateInodes() error {
	w.inodes = map[string]any{}

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
			nlink = getNLinks(fi)
			ino := getIno(fi)
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
			parentNid, err := w.findInodeAtPath(filepath.Join(path, ".."))
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
			path := filepath.Clean(filepath.Join(path, de.Name()))
			nid, err := w.findInodeAtPath(path)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to find inode for path %q: %w", path, err)
			}

			dirents = append(dirents, Dirent{
				Nid:      nid,
				FileType: uint8(fileTypeFromFileMode(de.Type())),
			})
			names = append(names, de.Name())
		}

		// Sort the directory entries by name.
		type pair struct {
			d  Dirent
			nm string
		}
		pairs := make([]pair, len(names))
		for i := range names {
			pairs[i] = pair{d: dirents[i], nm: names[i]}
		}
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].nm < pairs[j].nm
		})
		for i := range pairs {
			dirents[i] = pairs[i].d
			names[i] = pairs[i].nm
		}

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

	// TODO: device files, named pipes, sockets, etc.

	default:
		return nil, 0, fmt.Errorf("unsupported file type %o", mode&S_IFMT)
	}
}

func (w *writer) findInodeAtPath(path string) (uint64, error) {
	cleanPath := filepath.Clean(path)

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
		uid, gid = getOwner(fi)
	}

	// Clear permission bits from 'mode' and set the ones from originalFInfo.mode
	if originalFInfo != nil {
		mode = mode&^fs.ModePerm | originalFInfo.Mode.Perm()
	}

	compact := fi.Size() <= math.MaxUint32 &&
		uid <= math.MaxUint16 && gid <= math.MaxUint16 &&
		nlink <= math.MaxUint16

	if compact {
		return InodeCompact{
			Format: setBits(0, InodeLayoutCompact, InodeLayoutBit, InodeLayoutBits),
			Mode:   statModeFromFileMode(mode),
			Nlink:  uint16(nlink),
			UID:    uint16(uid),
			GID:    uint16(gid),
		}
	}

	return InodeExtended{
		Format:    setBits(0, InodeLayoutExtended, InodeLayoutBit, InodeLayoutBits),
		Mode:      statModeFromFileMode(mode),
		Nlink:     uint32(nlink),
		UID:       uint32(uid),
		GID:       uint32(gid),
		Mtime:     uint64(fi.ModTime().Unix()),
		MtimeNsec: uint32(fi.ModTime().Nanosecond()),
	}
}

func encodeDirents(dirents []Dirent, names []string) ([]byte, error) {
	// For inline directories, we encode everything in a single block without padding
	// For non-inline directories, we split into blocks and pad each block

	if len(dirents) == 0 {
		return nil, fmt.Errorf("encodeDirents called with empty dirents slice")
	}

	if len(dirents) != len(names) {
		return nil, fmt.Errorf("encodeDirents: dirents and names length mismatch (%d vs %d)", len(dirents), len(names))
	}

	var totalSize int64
	for i := range names {
		totalSize += DirentSize + int64(len(names[i]))
	}
	totalSize += 1 // null terminator

	if totalSize <= BlockSize {
		var buf bytes.Buffer
		nameOff := uint16(int64(len(dirents)) * DirentSize) // nameoff0

		for i, dirent := range dirents {
			dirent.NameOff = nameOff
			nameOff += uint16(len(names[i]))

			if err := binary.Write(&buf, binary.LittleEndian, dirent); err != nil {
				return nil, fmt.Errorf("failed to write dirent: %w", err)
			}
		}

		for _, name := range names {
			if _, err := buf.WriteString(name); err != nil {
				return nil, fmt.Errorf("failed to write name: %w", err)
			}
		}

		if err := buf.WriteByte(0); err != nil {
			return nil, fmt.Errorf("failed to write null terminator: %w", err)
		}

		return buf.Bytes(), nil
	}

	// For larger directories, split into blocks
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

// findFirstOccurrenceOfInode finds the path of the first file with the given
// inode number
func (w *writer) findFirstOccurrenceOfInode(nid uint64) string {
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]
		var inodeNum uint32
		switch ino := ino.(type) {
		case InodeCompact:
			inodeNum = ino.Ino
		case InodeExtended:
			inodeNum = ino.Ino
		default:
			continue
		}
		if uint64(inodeNum) == nid {
			return path
		}
	}
	return ""
}

func hasDataBlocks(ino any) bool {
	switch v := ino.(type) {
	case InodeCompact:
		layout := bitRange(v.Format, InodeDataLayoutBit, InodeDataLayoutBits)
		if layout == InodeDataLayoutFlatPlain {
			return true
		}
		if layout == InodeDataLayoutFlatInline {
			return v.Size > MaxInlineDataSize
		}
	case InodeExtended:
		layout := bitRange(v.Format, InodeDataLayoutBit, InodeDataLayoutBits)
		if layout == InodeDataLayoutFlatPlain {
			return true
		}
		if layout == InodeDataLayoutFlatInline {
			return v.Size > uint64(MaxInlineDataSize)
		}
	}
	return false
}

func (w *writer) updateBlock0(off int64, data []byte) {
	if len(data) == 0 {
		return
	}
	if w.block0 == nil {
		return
	}
	if off < 0 || off >= BlockSize {
		return
	}
	start := int(off)
	end := start + len(data)
	if end > BlockSize {
		end = BlockSize
		data = data[:end-start]
	}
	copy(w.block0[start:end], data)
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

// checksumSuperBlock calculates the checksum for the superblock using the
// current contents of block 0 (including any metadata stored after the
// superblock).
func (w *writer) checksumSuperBlock(sb *SuperBlock) error {
	sbCopy := *sb
	sbCopy.Checksum = 0

	var marshalled bytes.Buffer
	if err := binary.Write(&marshalled, binary.LittleEndian, sbCopy); err != nil {
		return err
	}

	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(marshalled.Bytes(), table)

	tailStart := SuperBlockOffset + marshalled.Len()
	tailLen := max(BlockSize-tailStart, 0)
	tail := make([]byte, tailLen)
	if w.block0 != nil && tailLen > 0 && tailStart < len(w.block0) {
		available := len(w.block0) - tailStart
		if available > 0 {
			copyLen := min(available, tailLen)
			copy(tail, w.block0[tailStart:tailStart+copyLen])
		}
	}
	checksum = ^crc32.Update(checksum, table, tail)

	sb.Checksum = checksum

	var final bytes.Buffer
	if err := binary.Write(&final, binary.LittleEndian, *sb); err != nil {
		return err
	}
	w.updateBlock0(int64(SuperBlockOffset), final.Bytes())

	return nil
}
