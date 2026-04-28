package erofs_test

import (
	"archive/tar"
	"bytes"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs/erofs"
	"github.com/unikraft/go-archivefs/tarfs"
)

// createImage is a test helper that writes an EROFS image from a source fs
// and returns a Filesystem opened from it. The image file is cleaned up
// automatically.
func createImage(t *testing.T, src fs.FS, opts ...erofs.ErofsCreateOption) *erofs.Filesystem {
	t.Helper()

	imgFile, err := os.CreateTemp(t.TempDir(), "erofs-test-*.img")
	require.NoError(t, err)
	t.Cleanup(func() { imgFile.Close() })

	require.NoError(t, erofs.Create(imgFile, src, opts...))

	fsys, err := erofs.Open(imgFile)
	require.NoError(t, err)

	return fsys
}

// createTarFS is a test helper that builds a tarfs from a slice of tar entries.
func createTarFS(t *testing.T, entries []tar.Header, fileData map[string][]byte) *tarfs.FS {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, h := range entries {
		hCopy := h
		if data, ok := fileData[h.Name]; ok {
			hCopy.Size = int64(len(data))
		}
		require.NoError(t, tw.WriteHeader(&hCopy))
		if data, ok := fileData[h.Name]; ok {
			_, err := tw.Write(data)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())

	tfs, err := tarfs.Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	return tfs
}

// TestEROFSPermissionPreservation verifies that file permissions are
// preserved through a write→read round-trip.
func TestEROFSPermissionPreservation(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeReg, Name: "readonly.txt", Mode: 0o444},
		{Typeflag: tar.TypeReg, Name: "executable", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "restricted.txt", Mode: 0o600},
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o700},
	}
	fileData := map[string][]byte{
		"readonly.txt":   []byte("r"),
		"executable":     []byte("x"),
		"restricted.txt": []byte("s"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	tests := []struct {
		name string
		perm fs.FileMode
	}{
		{"readonly.txt", 0o444},
		{"executable", 0o755},
		{"restricted.txt", 0o600},
		{"dir", 0o700},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := fsys.Stat(tt.name)
			require.NoError(t, err)
			require.Equal(t, tt.perm, info.Mode().Perm(),
				"permissions for %s", tt.name)
		})
	}
}

// TestEROFSSymlinkRelativeTarget verifies that relative symlinks
// in subdirectories resolve correctly.
func TestEROFSSymlinkRelativeTarget(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "dir/target.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "dir/link", Linkname: "target.txt"},
	}
	fileData := map[string][]byte{
		"dir/target.txt": []byte("relative-target"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	// Opening the symlink should resolve to the target.
	got, err := fs.ReadFile(fsys, "dir/link")
	require.NoError(t, err)
	require.Equal(t, []byte("relative-target"), got)

	// Stat follows the symlink.
	info, err := fsys.Stat("dir/link")
	require.NoError(t, err)
	require.Equal(t, "target.txt", info.Name())
	require.False(t, info.Mode().IsDir())

	// Lstat returns the symlink itself.
	linkInfo, err := fsys.Lstat("dir/link")
	require.NoError(t, err)
	require.Equal(t, "link", linkInfo.Name())
	require.NotZero(t, linkInfo.Mode()&fs.ModeSymlink)
}

// TestEROFSSymlinkAbsoluteTarget verifies that absolute-path
// symlinks resolve from the root of the image.
func TestEROFSSymlinkAbsoluteTarget(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "a/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "a/file.txt", Mode: 0o644},
		{Typeflag: tar.TypeDir, Name: "b/", Mode: 0o755},
		{Typeflag: tar.TypeSymlink, Name: "b/link", Linkname: "/a/file.txt"},
	}
	fileData := map[string][]byte{
		"a/file.txt": []byte("absolute-target"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	// Should resolve the absolute symlink from the fs root.
	got, err := fs.ReadFile(fsys, "b/link")
	require.NoError(t, err)
	require.Equal(t, []byte("absolute-target"), got)
}

// TestEROFSSymlinkChainNonCyclic verifies that a chain of symlinks
// that is within the limit resolves correctly.
func TestEROFSSymlinkChainNonCyclic(t *testing.T) {
	// Build chain: s3 -> s2 -> s1 -> target
	headers := []tar.Header{
		{Typeflag: tar.TypeReg, Name: "target", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "s1", Linkname: "target"},
		{Typeflag: tar.TypeSymlink, Name: "s2", Linkname: "s1"},
		{Typeflag: tar.TypeSymlink, Name: "s3", Linkname: "s2"},
	}
	fileData := map[string][]byte{
		"target": []byte("chain-end"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	got, err := fs.ReadFile(fsys, "s3")
	require.NoError(t, err)
	require.Equal(t, []byte("chain-end"), got)
}

// TestEROFSSymlinkToDirectory verifies that symlinks pointing to
// directories allow traversal through them.
func TestEROFSSymlinkToDirectory(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "real/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "real/data.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "alias", Linkname: "real"},
	}
	fileData := map[string][]byte{
		"real/data.txt": []byte("through-symdir"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	// Open file through the directory symlink.
	got, err := fs.ReadFile(fsys, "alias/data.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("through-symdir"), got)

	// ReadDir through the symlink directory.
	entries, err := fsys.ReadDir("alias")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "data.txt", entries[0].Name())
}

// TestEROFSWalkDirSymlinksNotFollowed verifies that WalkDir does not
// follow symlinks (per fs.WalkDir contract).
func TestEROFSWalkDirSymlinksNotFollowed(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "dir/file.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "dir"},
	}
	fileData := map[string][]byte{
		"dir/file.txt": []byte("data"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	var paths []string
	var types []fs.FileMode
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, p)
		types = append(types, d.Type())
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{".", "dir", "dir/file.txt", "link"}, paths)
	require.Equal(t, fs.ModeSymlink, types[3],
		"WalkDir should report symlink type, not follow it")
}

// TestEROFSRoundTripModTime verifies that modification times are
// preserved through a round-trip for non-zero times.
func TestEROFSRoundTripModTime(t *testing.T) {
	modTime := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)

	headers := []tar.Header{
		{
			Typeflag: tar.TypeReg,
			Name:     "file.txt",
			Mode:     0o644,
			ModTime:  modTime,
		},
	}
	fileData := map[string][]byte{
		"file.txt": []byte("timed"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	info, err := fsys.Stat("file.txt")
	require.NoError(t, err)
	require.Equal(t, modTime.Unix(), info.ModTime().Unix())
}

// TestEROFSReadLinkValue verifies that ReadLink returns the correct
// target path for symlinks.
func TestEROFSReadLinkValue(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeReg, Name: "target.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "target.txt"},
		{Typeflag: tar.TypeDir, Name: "sub/", Mode: 0o755},
		{Typeflag: tar.TypeSymlink, Name: "sub/up", Linkname: "../target.txt"},
	}
	fileData := map[string][]byte{
		"target.txt": []byte("t"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	target, err := fsys.ReadLink("link")
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)

	target, err = fsys.ReadLink("sub/up")
	require.NoError(t, err)
	require.Equal(t, "../target.txt", target)
}

// TestEROFSSymlinkDangling verifies that opening a dangling symlink
// returns an error, but ReadLink and Lstat still work.
func TestEROFSSymlinkDangling(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeSymlink, Name: "broken", Linkname: "nonexistent"},
	}

	tfs := createTarFS(t, headers, nil)
	fsys := createImage(t, tfs)

	// ReadLink should return the target even though it's dangling.
	target, err := fsys.ReadLink("broken")
	require.NoError(t, err)
	require.Equal(t, "nonexistent", target)

	// Lstat should return symlink info.
	info, err := fsys.Lstat("broken")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&fs.ModeSymlink)

	// Open and Stat should fail because the target doesn't exist.
	_, err = fsys.Open("broken")
	require.Error(t, err)

	_, err = fsys.Stat("broken")
	require.Error(t, err)
}

// TestEROFSSetuidSetgidStickyRoundTrip verifies that special mode
// bits survive a full write→read round-trip (not just reader).
func TestEROFSSetuidSetgidStickyRoundTrip(t *testing.T) {
	headers := []tar.Header{
		{Typeflag: tar.TypeReg, Name: "suid", Mode: 0o4755},
		{Typeflag: tar.TypeReg, Name: "sgid", Mode: 0o2755},
		{Typeflag: tar.TypeDir, Name: "sticky/", Mode: 0o1755},
	}
	fileData := map[string][]byte{
		"suid": []byte("s"),
		"sgid": []byte("g"),
	}

	tfs := createTarFS(t, headers, fileData)
	fsys := createImage(t, tfs)

	info, err := fsys.Stat("suid")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&fs.ModeSetuid,
		"setuid bit should be preserved")
	require.Equal(t, fs.FileMode(0o755), info.Mode().Perm())

	info, err = fsys.Stat("sgid")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&fs.ModeSetgid,
		"setgid bit should be preserved")

	info, err = fsys.Stat("sticky")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&fs.ModeSticky,
		"sticky bit should be preserved")
	require.True(t, info.IsDir())
}
