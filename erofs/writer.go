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
	// MaxInlineDataSize = BlockSize / 4
	// We change this to 0 to represent the flag '-E noinline_data'
	MaxInlineDataSize = 0
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
	linkMap    map[uint64]inodeCount
	opts       ErofsCreateOptions
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

	metaSize, dataSize, err := w.firstPass()
	if err != nil {
		return fmt.Errorf("failed to calculate metadata and data size: %w", err)
	}

	// Reserve the first block for the superblock.
	metaBlockAddr := int64(1)

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

	sb := SuperBlock{
		Magic:         SuperBlockMagicV1,
		BlockSizeBits: BlockSizeBits,
		Inodes:        uint64(len(w.inodes)),
		Blocks:        uint32(1 + (metaSize+dataSize)/BlockSize),
		MetaBlockAddr: uint32(metaBlockAddr),
		UUID:          uuid,
		BuildTime:     uint64(timeNow.Unix()),
		BuildTimeNsec: uint32(timeNow.Nanosecond()),
		FeatureCompat: EROFS_FEATURE_COMPAT_SB_CHKSUM | EROFS_FEATURE_COMPAT_MTIME,
		// TODO: other fields (volume name, etc.)
	}

	if err := sb.checksum(); err != nil {
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
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		data, size, err := w.dataForInode(path, ino)
		if err != nil {
			return metaSize, dataSize, fmt.Errorf("failed to get data for %q: %w", path, err)
		}
		_ = data.Close()

		// Avoid inlining empty files: the reader expects tailSize != 0 for inline
		// layout, and MaxInlineDataSize=0 is used to represent '-E noinline_data'.
		inlined := size > 0 && size <= MaxInlineDataSize
		if inlined {
			// if the size of the inode and data exceeds the block size, we need to
			// pad to the next block boundary before inlining the data.
			spaceAvailable := roundUp(metaSize, BlockSize) - metaSize
			if spaceAvailable > 0 && int64(binary.Size(ino))+size > spaceAvailable {
				// Pad the metadata to the next block boundary.
				metaSize = roundUp(metaSize, BlockSize)
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

				if entry, ok := w.linkMap[fsIno]; ok {
					if w.linkMap[fsIno].Count == 0 {
						ino.Ino = nid
						entry.Count = 1
						entry.Inode = uint64(ino.Ino)
					} else {
						// If this is a hard link, we reuse the inode number from the first
						// file with the same fsInode.
						ino.Ino = uint32(w.linkMap[fsIno].Inode)
						entry.Count = w.linkMap[fsIno].Count + 1
					}
					ino.Size = uint32(size)

					w.linkMap[fsIno] = entry
				} else {
					return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
				}
			}
			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
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

				if entry, ok := w.linkMap[fsIno]; ok {
					if w.linkMap[fsIno].Count == 0 {
						ino.Ino = nid
						entry.Count = 1
						entry.Inode = uint64(ino.Ino)
					} else {
						// If this is a hard link, we reuse the inode number from the first
						// file with the same fsInode.
						ino.Ino = uint32(w.linkMap[fsIno].Inode)
						entry.Count = w.linkMap[fsIno].Count + 1
					}
					ino.Size = uint64(size)

					w.linkMap[fsIno] = entry
				} else {
					return metaSize, dataSize, fmt.Errorf("inode count for %q not found", path)
				}
			}
			if inlined {
				ino.Format = setBits(ino.Format, InodeDataLayoutFlatInline, InodeDataLayoutBit, InodeDataLayoutBits)
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
			dataSize += size
			dataSize = roundUp(dataSize, BlockSize)
		}
	}

	metaSize = roundUp(metaSize, BlockSize)

	dataBlockAddr := 1 + (metaSize / BlockSize)

	// fix up the raw block addresses now that we know the total size of the
	// metadata space.
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		switch ino := ino.(type) {
		case InodeCompact:
			if !isInlined(ino) {
				ino.RawBlockAddr += uint32(dataBlockAddr)
				w.inodes[path] = ino
			}
		case InodeExtended:
			if !isInlined(ino) {
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

		// Get the address of the inode.
		off := metaBlockAddr*BlockSize + int64(nid)*InodeSlotSize

		// Write the inode.
		if err := binary.Write(io.NewOffsetWriter(w.dst, off), binary.LittleEndian, ino); err != nil {
			return fmt.Errorf("failed to write inode for %q: %w", path, err)
		}

		// Small files are stored in the inline with the inode.
		if isInlined(ino) {
			data, _, err := w.dataForInode(path, ino)
			if err != nil {
				return fmt.Errorf("failed to get data for %q: %w", path, err)
			}

			// Write the inlined data.
			_, err = io.Copy(io.NewOffsetWriter(w.dst, off+int64(binary.Size(ino))), data)
			_ = data.Close()
			if err != nil {
				return fmt.Errorf("failed to write inline data for %q: %w", path, err)
			}
		}
	}

	return nil
}

func (w *writer) writeData() error {
	for _, path := range w.inodeOrder {
		ino := w.inodes[path]

		if isInlined(ino) {
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

		data, _, err := w.dataForInode(path, ino)
		if err != nil {
			return fmt.Errorf("failed to get data for %q: %w", path, err)
		}

		_, err = io.Copy(io.NewOffsetWriter(w.dst, int64(rawBlockAddr)*BlockSize), data)
		_ = data.Close()
		if err != nil {
			return fmt.Errorf("failed to write data for %q: %w", path, err)
		}
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
			if _, ok := w.linkMap[ino]; !ok {
				w.linkMap[ino] = inodeCount{
					Count: 0,
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
		fi.ModTime().Equal(time.Time{})

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
