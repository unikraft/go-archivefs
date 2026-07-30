// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH and The Unikraft CLI Authors.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package erofs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type dirMeta struct {
	path string
	info fs.FileInfo
}

// Unpack reads an EROFS image from src and extracts its entries under dest.
// Directories and regular files are restored with their permission bits
// (including setuid/setgid/sticky) and modification time.
// Device, fifo, and socket entries are skipped.
func Unpack(src io.ReaderAt, dest string) error {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("creating destination directory: %w", err)
	}

	fsys, err := Open(src)
	if err != nil {
		return fmt.Errorf("opening EROFS image: %w", err)
	}

	var dirs []dirMeta

	walkErr := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == "." {
			return nil
		}

		p, err := safeJoin(dest, path)
		if err != nil {
			return err
		}
		if p == "" {
			return nil
		}
		if err := ensureNoSymlinkParents(dest, p); err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		mode := info.Mode()
		perm := mode.Perm()

		switch {
		case mode.IsDir():
			if err := os.MkdirAll(p, perm); err != nil {
				return fmt.Errorf("creating directory %q: %w", p, err)
			}
			dirs = append(dirs, dirMeta{path: p, info: info})

		case mode&os.ModeSymlink != 0:
			target, err := fsys.ReadLink(path)
			if err != nil {
				return fmt.Errorf("reading symlink %q: %w", path, err)
			}
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return fmt.Errorf("creating parent of %q: %w", p, err)
			}
			_ = os.Remove(p)
			if err := os.Symlink(target, p); err != nil {
				return fmt.Errorf("creating symlink %q: %w", p, err)
			}
			tryChown(p, info)

		case mode.IsRegular():
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return fmt.Errorf("creating parent of %q: %w", p, err)
			}
			srcFile, err := fsys.Open(path)
			if err != nil {
				return fmt.Errorf("opening file %q: %w", path, err)
			}
			dstFile, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
			if err != nil {
				srcFile.Close()
				return fmt.Errorf("creating file %q: %w", p, err)
			}
			if _, err := io.Copy(dstFile, srcFile); err != nil {
				dstFile.Close()
				srcFile.Close()
				return fmt.Errorf("writing file %q: %w", p, err)
			}
			if err := dstFile.Close(); err != nil {
				srcFile.Close()
				return fmt.Errorf("closing file %q: %w", p, err)
			}
			srcFile.Close()
			_ = os.Chmod(p, mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky|os.ModePerm))
			setMtime(p, info.ModTime())
			tryChown(p, info)

		default:
			// Skip device, char device, fifo, and socket entries.
		}
		return nil
	})

	// Apply deferred directory metadata even on error so a partial extraction
	// still restores what was already written. The chmod special bits are
	// included via info.Mode().
	dirErr := applyDirMeta(dirs)
	if walkErr != nil {
		return fmt.Errorf("%w", joinErrors(walkErr, dirErr))
	}
	return dirErr
}

// applyDirMeta restores permission bits (incl. special bits), mtime, and
// ownership for directories recorded during the walk, applied deepest-first so
// a parent's mtime is set after its children. Each step is best-effort; the
// first metadata error (if any) is returned.
func applyDirMeta(dirs []dirMeta) error {
	var err error
	for i := len(dirs) - 1; i >= 0; i-- {
		d := dirs[i]
		mode := d.info.Mode()
		err = joinErrors(err, os.Chmod(d.path, mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky|os.ModePerm)))
		setMtime(d.path, d.info.ModTime())
		tryChown(d.path, d.info)
	}
	return err
}

// joinErrors combines two errors, either of which may be nil.
func joinErrors(a, b error) error {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return fmt.Errorf("%w; %v", a, b)
	}
}

// setMtime sets the access and modification times of p to mtime.
// Restoration is best-effort.
func setMtime(p string, mtime time.Time) {
	if mtime.IsZero() {
		return
	}
	_ = os.Chtimes(p, mtime, mtime)
}

// tryChown restores ownership best-effort using the archive's OwnerInfo (if the
// entry carries one). Errors are ignored.
func tryChown(p string, info fs.FileInfo) {
	owner, ok := info.Sys().(interface{ GetUID() int })
	if !ok {
		return
	}
	uid := owner.GetUID()
	gid := 0
	if g, ok := info.Sys().(interface{ GetGID() int }); ok {
		gid = g.GetGID()
	}
	_ = os.Lchown(p, uid, gid)
}

// ensureNoSymlinkParents verifies that no component of p between dest and the
// final element is a symlink.
func ensureNoSymlinkParents(dest, p string) error {
	absDest, err := filepath.Abs(dest)
	if err != nil {
		return fmt.Errorf("resolving destination: %w", err)
	}
	absP, err := filepath.Abs(p)
	if err != nil {
		return fmt.Errorf("resolving %q: %w", p, err)
	}

	rel, err := filepath.Rel(absDest, absP)
	if err != nil {
		return err
	}

	cur := absDest
	for _, seg := range splitSegments(rel) {
		cur = filepath.Join(cur, seg)
		fi, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("entry %q traverses symlink %q", p, cur)
		}
	}
	return nil
}

// splitSegments returns the non-empty, non-"." path segments of rel, split on
// '/' (normalized from the OS separator first).
func splitSegments(rel string) []string {
	rel = filepath.ToSlash(filepath.Clean(rel))
	if rel == "" || rel == "." {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(rel); i++ {
		if rel[i] == '/' {
			if i > start {
				if seg := rel[start:i]; seg != "." {
					out = append(out, seg)
				}
			}
			start = i + 1
		}
	}
	if start < len(rel) {
		if seg := rel[start:]; seg != "." {
			out = append(out, seg)
		}
	}
	return out
}

// safeJoin resolves an archive entry path to a path under dest, refusing any
// entry that would lexically escape dest.
func safeJoin(dest, name string) (string, error) {
	rel := filepath.Clean("/" + name)
	rel = rel[1:] // drop the forced leading "/"
	if rel == "" || rel == "." {
		return "", nil
	}
	if filepath.IsAbs(rel) || hasVolumeName(rel) {
		return "", fmt.Errorf("entry %q escapes destination directory", name)
	}

	joined := filepath.Join(dest, rel)

	absDest, err := filepath.Abs(dest)
	if err != nil {
		return "", fmt.Errorf("resolving destination: %w", err)
	}
	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("resolving %q: %w", rel, err)
	}

	relToDest, err := filepath.Rel(absDest, absJoined)
	if err != nil {
		return "", fmt.Errorf("entry %q escapes destination directory", name)
	}
	relToDest = filepath.ToSlash(relToDest)
	if relToDest == ".." || strings.HasPrefix(relToDest, "../") {
		return "", fmt.Errorf("entry %q escapes destination directory", name)
	}

	return joined, nil
}

// hasVolumeName reports whether path begins with a Windows volume name.
func hasVolumeName(path string) bool {
	if len(path) < 2 {
		return false
	}
	return path[1] == ':'
}
