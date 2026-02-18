package memfs_test

import (
	"fmt"
	"errors"
	"io/fs"
	"syscall"
	"testing"

	"github.com/unikraft/go-archivefs/memfs"

	"github.com/stretchr/testify/require"
)

// Absolute symlink targets with multiple leading/internal slashes must
// resolve to the root-relative path (fixup #1).
func TestFixupAbsoluteSymlinkExtraSlashes(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.MkdirAll("deep/nested", 0o755))
	require.NoError(t, rootFS.WriteFile("deep/nested/file.txt", []byte("abs"), 0o644))

	require.NoError(t, rootFS.Symlink("//deep//nested", "shortcut"))

	got, err := fs.ReadFile(rootFS, "shortcut/file.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("abs"), got)
}

// A symlink loop must surface a detectable ELOOP error (fixup #2).
func TestFixupSymlinkLoopDetectable(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.Symlink("b", "a"))
	require.NoError(t, rootFS.Symlink("a", "b"))

	_, err := rootFS.Open("a")
	require.ErrorIs(t, err, syscall.ELOOP)
	// And still be wrapped recognisably for fs consumers.
	require.True(t, errors.Is(err, fs.ErrNotExist) == false, "loop should not be reported as ErrNotExist")
}

// Concurrent MkdirAll on overlapping paths must not lose directories.
// Guards the hand-over-hand locking fix (fixup #3).
func TestFixupMkdirAllConcurrentOverlap(t *testing.T) {
	rootFS := memfs.New()
	// Pre-create the shared prefix so the interesting contention happens
	// below it on the per-goroutine leaves and on a shared leaf.
	require.NoError(t, rootFS.MkdirAll("shared", 0o755))

	const goroutines = 20
	const iterations = 200
	done := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			for n := 0; n < iterations; n++ {
				// Every goroutine creates the same shared deep path; the
				// second-and-later creators must observe it as existing.
				if err := rootFS.MkdirAll("shared/deep/common", 0o755); err != nil {
					done <- err
					return
				}
				// And a per-goroutine branch off the shared prefix.
				leaf := fmt.Sprintf("shared/g%d/leaf", g)
				if err := rootFS.MkdirAll(leaf, 0o755); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for g := 0; g < goroutines; g++ {
		require.NoError(t, <-done)
	}
	// All created paths must be observable afterwards.
	require.NoError(t, rootFS.MkdirAll("shared/deep/common", 0o755))
	for g := 0; g < goroutines; g++ {
		fi, err := fs.Stat(rootFS, fmt.Sprintf("shared/g%d/leaf", g))
		require.NoError(t, err, "goroutine %d leaf missing", g)
		require.True(t, fi.IsDir())
	}
}

// Writing through a dangling symlink should create/truncate the link's
// target, matching OS open(O_CREATE|O_TRUNC) semantics (fixup #4).
func TestFixupWriteFileThroughDanglingSymlink(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.Symlink("nonexistent", "link"))

	require.NoError(t, rootFS.WriteFile("link", []byte("data"), 0o644))

	// The data should land at the target, and reading through the link
	// should return it.
	got, err := fs.ReadFile(rootFS, "nonexistent")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), got)
	got2, err := fs.ReadFile(rootFS, "link")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), got2)

	// A relative symlink resolves against its parent directory.
	require.NoError(t, rootFS.MkdirAll("dir", 0o755))
	require.NoError(t, rootFS.Symlink("inside", "dir/rel"))
	require.NoError(t, rootFS.WriteFile("dir/rel", []byte("rel"), 0o644))
	got3, err := fs.ReadFile(rootFS, "dir/inside")
	require.NoError(t, err)
	require.Equal(t, []byte("rel"), got3)
}

// Writing through a symlink whose target is still missing its parent dir
// must report ErrNotExist, not create the target (matching the existing
// WriteFile contract verified in TestMemFS).
func TestFixupWriteFileSymlinkMissingParent(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.Symlink("missing/child", "link"))

	err := rootFS.WriteFile("link", []byte("x"), 0o644)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// Lstat(".") must report Name() == ".", matching Open(".").Stat(),
// for both the root FS and a Sub FS (fixup #5).
func TestFixupLstatRootName(t *testing.T) {
	rootFS := memfs.New()
	require.NoError(t, rootFS.MkdirAll("sub", 0o755))

	fi, err := rootFS.Lstat(".")
	require.NoError(t, err)
	require.Equal(t, ".", fi.Name())

	// Cross-check against Open(".").Stat().
	f, err := rootFS.Open(".")
	require.NoError(t, err)
	ofi, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, ".", ofi.Name())

	sub, err := rootFS.Sub("sub")
	require.NoError(t, err)
	sfi, err := sub.(*memfs.FS).Lstat(".")
	require.NoError(t, err)
	require.Equal(t, ".", sfi.Name())
}
