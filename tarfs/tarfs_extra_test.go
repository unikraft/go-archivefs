package tarfs_test

import (
	"archive/tar"
	"bytes"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs/tarfs"
)

// buildTar is a test helper that creates a tar archive from headers and data.
func buildTar(t *testing.T, entries []tar.Header, data map[string][]byte) *tarfs.FS {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, h := range entries {
		hCopy := h
		if d, ok := data[h.Name]; ok {
			hCopy.Size = int64(len(d))
		}
		require.NoError(t, tw.WriteHeader(&hCopy))
		if d, ok := data[h.Name]; ok {
			_, err := tw.Write(d)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())

	fsys, err := tarfs.Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	return fsys
}

// TestTarFSValidPathRejection verifies that all public methods reject
// names that don't satisfy fs.ValidPath.
func TestTarFSValidPathRejection(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	invalid := []string{
		"",
		"/absolute",
		"./relative",
		"../escape",
		"foo/../bar",
		"foo/",
		"foo//bar",
	}

	for _, name := range invalid {
		t.Run("Open_"+name, func(t *testing.T) {
			_, err := fsys.Open(name)
			require.Error(t, err)
			var pathErr *fs.PathError
			require.ErrorAs(t, err, &pathErr)
			require.ErrorIs(t, pathErr.Err, fs.ErrInvalid)
		})
		t.Run("Stat_"+name, func(t *testing.T) {
			_, err := fsys.Stat(name)
			require.Error(t, err)
		})
		t.Run("ReadDir_"+name, func(t *testing.T) {
			_, err := fsys.ReadDir(name)
			require.Error(t, err)
		})
		t.Run("ReadLink_"+name, func(t *testing.T) {
			_, err := fsys.ReadLink(name)
			require.Error(t, err)
		})
		t.Run("Lstat_"+name, func(t *testing.T) {
			_, err := fsys.Lstat(name)
			require.Error(t, err)
		})
	}
}

// TestTarFSOpenDirReturnsReadDirFile verifies that Open on a directory
// returns a ReadDirFile with proper pagination.
func TestTarFSOpenDirReturnsReadDirFile(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "dir/a.txt", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "dir/b.txt", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "dir/c.txt", Mode: 0o644},
	}, map[string][]byte{
		"dir/a.txt": []byte("a"),
		"dir/b.txt": []byte("b"),
		"dir/c.txt": []byte("c"),
	})

	f, err := fsys.Open("dir")
	require.NoError(t, err)
	defer f.Close()

	rdf, ok := f.(fs.ReadDirFile)
	require.True(t, ok, "Open on directory should return fs.ReadDirFile")

	// Stat on directory handle.
	info, err := rdf.Stat()
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// Read on directory should fail.
	buf := make([]byte, 10)
	_, err = rdf.Read(buf)
	require.Error(t, err)

	// Paginated ReadDir.
	batch1, err := rdf.ReadDir(2)
	require.NoError(t, err)
	require.Len(t, batch1, 2)
	require.Equal(t, "a.txt", batch1[0].Name())
	require.Equal(t, "b.txt", batch1[1].Name())

	// Last entry with io.EOF.
	batch2, err := rdf.ReadDir(2)
	require.ErrorIs(t, err, io.EOF)
	require.Len(t, batch2, 1)
	require.Equal(t, "c.txt", batch2[0].Name())

	// Exhausted.
	batch3, err := rdf.ReadDir(1)
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, batch3)
}

// TestTarFSOpenRootDir verifies that Open(".") returns a working
// ReadDirFile for the root.
func TestTarFSOpenRootDir(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "alpha.txt", Mode: 0o644},
		{Typeflag: tar.TypeDir, Name: "beta/", Mode: 0o755},
	}, map[string][]byte{
		"alpha.txt": []byte("a"),
	})

	f, err := fsys.Open(".")
	require.NoError(t, err)
	defer f.Close()

	rdf := f.(fs.ReadDirFile)
	entries, err := rdf.ReadDir(0)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Entries should be sorted.
	require.Equal(t, "alpha.txt", entries[0].Name())
	require.Equal(t, "beta", entries[1].Name())
}

// TestTarFSSymlinkCycleDetection verifies that circular symlinks
// are detected and return an error instead of looping forever.
func TestTarFSSymlinkCycleDetection(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeSymlink, Name: "a", Linkname: "b"},
		{Typeflag: tar.TypeSymlink, Name: "b", Linkname: "a"},
	}, nil)

	_, err := fsys.Stat("a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many levels of symbolic links")

	_, err = fsys.Open("b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many levels of symbolic links")
}

// TestTarFSSymlinkChain verifies that a non-cyclic chain of
// symlinks resolves correctly.
func TestTarFSSymlinkChain(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "target", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "s1", Linkname: "target"},
		{Typeflag: tar.TypeSymlink, Name: "s2", Linkname: "s1"},
		{Typeflag: tar.TypeSymlink, Name: "s3", Linkname: "s2"},
	}, map[string][]byte{
		"target": []byte("chain-end"),
	})

	got, err := fs.ReadFile(fsys, "s3")
	require.NoError(t, err)
	require.Equal(t, []byte("chain-end"), got)
}

// TestTarFSSymlinkRelative verifies that relative symlinks in
// subdirectories resolve correctly.
func TestTarFSSymlinkRelative(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "dir/target.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "dir/link", Linkname: "target.txt"},
	}, map[string][]byte{
		"dir/target.txt": []byte("relative"),
	})

	got, err := fs.ReadFile(fsys, "dir/link")
	require.NoError(t, err)
	require.Equal(t, []byte("relative"), got)
}

// TestTarFSSymlinkAbsolute verifies that absolute symlinks resolve
// from the archive root.
func TestTarFSSymlinkAbsolute(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "real/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "real/file.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "/real/file.txt"},
	}, map[string][]byte{
		"real/file.txt": []byte("absolute"),
	})

	got, err := fs.ReadFile(fsys, "link")
	require.NoError(t, err)
	require.Equal(t, []byte("absolute"), got)
}

// TestTarFSSymlinkToDirectory verifies that symlinks to directories
// allow traversal through them.
func TestTarFSSymlinkToDirectory(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "real/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "real/data.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "alias", Linkname: "real"},
	}, map[string][]byte{
		"real/data.txt": []byte("through-symdir"),
	})

	got, err := fs.ReadFile(fsys, "alias/data.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("through-symdir"), got)

	entries, err := fsys.ReadDir("alias")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "data.txt", entries[0].Name())
}

// TestTarFSSymlinkDangling verifies that a dangling symlink can
// still be inspected via ReadLink and Lstat, but Open/Stat fail.
func TestTarFSSymlinkDangling(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeSymlink, Name: "broken", Linkname: "nonexistent"},
	}, nil)

	target, err := fsys.ReadLink("broken")
	require.NoError(t, err)
	require.Equal(t, "nonexistent", target)

	info, err := fsys.Lstat("broken")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&fs.ModeSymlink)

	_, err = fsys.Open("broken")
	require.Error(t, err)

	_, err = fsys.Stat("broken")
	require.Error(t, err)
}

// TestTarFSReadLinkOnNonSymlink verifies that ReadLink returns an
// error when called on a regular file or directory.
func TestTarFSReadLinkOnNonSymlink(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	_, err := fsys.ReadLink("file.txt")
	require.ErrorIs(t, err, fs.ErrInvalid)

	_, err = fsys.ReadLink("dir")
	require.ErrorIs(t, err, fs.ErrInvalid)
}

// TestTarFSReadLinkRoot verifies that ReadLink(".") returns
// fs.ErrInvalid since the root is not a symlink.
func TestTarFSReadLinkRoot(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	_, err := fsys.ReadLink(".")
	require.Error(t, err)

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	require.ErrorIs(t, pathErr.Err, fs.ErrInvalid)
}

// TestTarFSLstatRoot verifies that Lstat(".") returns the root
// directory info.
func TestTarFSLstatRoot(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	info, err := fsys.Lstat(".")
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestTarFSLstatSymlink verifies that Lstat returns the symlink's
// own info, not the target's.
func TestTarFSLstatSymlink(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "target.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "target.txt", Mode: 0o777},
	}, map[string][]byte{
		"target.txt": []byte("hello"),
	})

	// Lstat should return the symlink's info.
	linkInfo, err := fsys.Lstat("link")
	require.NoError(t, err)
	require.Equal(t, "link", linkInfo.Name())
	require.NotZero(t, linkInfo.Mode()&fs.ModeSymlink)

	// Stat should follow the symlink.
	statInfo, err := fsys.Stat("link")
	require.NoError(t, err)
	require.Equal(t, "link", statInfo.Name())
	require.Zero(t, statInfo.Mode()&fs.ModeSymlink)
}

// TestTarFSReadLinkValue verifies that ReadLink returns the
// correct target path.
func TestTarFSReadLinkValue(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "target.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "target.txt"},
		{Typeflag: tar.TypeDir, Name: "sub/", Mode: 0o755},
		{Typeflag: tar.TypeSymlink, Name: "sub/up", Linkname: "../target.txt"},
	}, map[string][]byte{
		"target.txt": []byte("t"),
	})

	target, err := fsys.ReadLink("link")
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)

	target, err = fsys.ReadLink("sub/up")
	require.NoError(t, err)
	require.Equal(t, "../target.txt", target)
}

// TestTarFSDirectoryMetadataPreserved verifies that explicit
// directory entries in the tar preserve their metadata (mode, time)
// rather than being overwritten by implicit defaults.
func TestTarFSDirectoryMetadataPreserved(t *testing.T) {
	dirTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	fsys := buildTar(t, []tar.Header{
		// File appears first, creating an implicit parent.
		{Typeflag: tar.TypeReg, Name: "dir/file.txt", Mode: 0o644},
		// Explicit directory entry comes after with specific metadata.
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o700, ModTime: dirTime},
	}, map[string][]byte{
		"dir/file.txt": []byte("data"),
	})

	info, err := fsys.Stat("dir")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// The explicit metadata should win over the implicit default.
	require.Equal(t, fs.FileMode(0o700), info.Mode().Perm(),
		"explicit directory permissions should be preserved")
	require.Equal(t, dirTime.Unix(), info.ModTime().Unix(),
		"explicit directory modtime should be preserved")
}

// TestTarFSStatRoot verifies Stat(".") returns root directory info.
func TestTarFSStatRoot(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	info, err := fsys.Stat(".")
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, ".", info.Name())
}

// TestTarFSStatNonExistent verifies proper errors for missing paths.
func TestTarFSStatNonExistent(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	_, err := fsys.Stat("nonexistent")
	require.Error(t, err)

	_, err = fsys.Open("nonexistent")
	require.Error(t, err)
}

// TestTarFSReadDirSorted verifies ReadDir returns entries in
// sorted order regardless of tar entry order.
func TestTarFSReadDirSorted(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "z.txt", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "a.txt", Mode: 0o644},
		{Typeflag: tar.TypeDir, Name: "m_dir/", Mode: 0o755},
	}, map[string][]byte{
		"z.txt": []byte("z"),
		"a.txt": []byte("a"),
	})

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	require.Equal(t, []string{"a.txt", "m_dir", "z.txt"}, names)
}

// TestTarFSContentIntegrity verifies that file contents are read
// correctly for various sizes.
func TestTarFSContentIntegrity(t *testing.T) {
	files := map[string][]byte{
		"empty.txt":  {},
		"small.txt":  []byte("hello"),
		"medium.txt": bytes.Repeat([]byte("x"), 4096),
		"large.txt":  bytes.Repeat([]byte("y"), 65536),
	}

	var entries []tar.Header
	for name := range files {
		entries = append(entries, tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0o644,
		})
	}

	fsys := buildTar(t, entries, files)

	for name, want := range files {
		t.Run(name, func(t *testing.T) {
			got, err := fs.ReadFile(fsys, name)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestTarFSWalkDir verifies that WalkDir visits all paths in the
// correct order and that symlinks are not followed.
func TestTarFSWalkDir(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "a/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "a/file.txt", Mode: 0o644},
		{Typeflag: tar.TypeDir, Name: "b/", Mode: 0o755},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "a"},
	}, map[string][]byte{
		"a/file.txt": []byte("data"),
	})

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

	require.Equal(t, []string{".", "a", "a/file.txt", "b", "link"}, paths)
	require.Equal(t, fs.ModeSymlink, types[4],
		"WalkDir should report symlink type")
}

// TestTarFSPathTraversalSanitization verifies that path traversal
// attempts in tar entries are neutralized by sanitizePath.
func TestTarFSPathTraversalSanitization(t *testing.T) {
	malicious := []string{
		"../../../etc/passwd",
		"..",
		"normal/../../../escape",
	}

	var entries []tar.Header
	data := map[string][]byte{}
	for _, p := range malicious {
		entries = append(entries, tar.Header{
			Typeflag: tar.TypeReg,
			Name:     p,
			Mode:     0o644,
		})
		data[p] = []byte("malicious")
	}
	// Add a safe file.
	entries = append(entries, tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "safe.txt",
		Mode:     0o644,
	})
	data["safe.txt"] = []byte("safe")

	fsys := buildTar(t, entries, data)

	// Safe file should be accessible.
	got, err := fs.ReadFile(fsys, "safe.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("safe"), got)

	// Root should only contain the safe file.
	rootEntries, err := fsys.ReadDir(".")
	require.NoError(t, err)
	require.Len(t, rootEntries, 1)
	require.Equal(t, "safe.txt", rootEntries[0].Name())
}

// TestTarFSHiddenDirAccess verifies that directories starting with
// a dot are accessible (not mistakenly filtered by sanitizePath).
func TestTarFSHiddenDirAccess(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: ".config/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: ".config/settings.json", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: ".gitignore", Mode: 0o644},
	}, map[string][]byte{
		".config/settings.json": []byte(`{"key":"val"}`),
		".gitignore":            []byte("*.o"),
	})

	// Stat hidden directory.
	info, err := fsys.Stat(".config")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// Read file inside hidden directory.
	got, err := fs.ReadFile(fsys, ".config/settings.json")
	require.NoError(t, err)
	require.Equal(t, `{"key":"val"}`, string(got))

	// Read dot-file at root.
	got, err = fs.ReadFile(fsys, ".gitignore")
	require.NoError(t, err)
	require.Equal(t, "*.o", string(got))
}

// TestTarFSImplicitParentDirectories verifies that parent directories
// are automatically created when files reference them without explicit
// directory entries.
func TestTarFSImplicitParentDirectories(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		// No explicit directory entries for "a" or "a/b".
		{Typeflag: tar.TypeReg, Name: "a/b/file.txt", Mode: 0o644},
	}, map[string][]byte{
		"a/b/file.txt": []byte("deep"),
	})

	// Implicit parent directories should be stat-able.
	info, err := fsys.Stat("a")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	info, err = fsys.Stat("a/b")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// File should be readable.
	got, err := fs.ReadFile(fsys, "a/b/file.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("deep"), got)
}

// TestTarFSDeepNesting verifies deeply nested paths work.
func TestTarFSDeepNesting(t *testing.T) {
	parts := make([]string, 10)
	for i := range parts {
		parts[i] = "d"
	}
	deepDir := strings.Join(parts, "/")
	deepFile := deepDir + "/leaf.txt"

	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: deepFile, Mode: 0o644},
	}, map[string][]byte{
		deepFile: []byte("leaf"),
	})

	got, err := fs.ReadFile(fsys, deepFile)
	require.NoError(t, err)
	require.Equal(t, []byte("leaf"), got)
}

// TestTarFSMultipleFilesInDir verifies reading multiple files
// from the same directory works correctly.
func TestTarFSMultipleFilesInDir(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "dir/one.txt", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "dir/two.txt", Mode: 0o644},
		{Typeflag: tar.TypeReg, Name: "dir/three.txt", Mode: 0o644},
	}, map[string][]byte{
		"dir/one.txt":   []byte("1"),
		"dir/two.txt":   []byte("22"),
		"dir/three.txt": []byte("333"),
	})

	for name, want := range map[string]string{
		"dir/one.txt":   "1",
		"dir/two.txt":   "22",
		"dir/three.txt": "333",
	} {
		got, err := fs.ReadFile(fsys, name)
		require.NoError(t, err)
		require.Equal(t, want, string(got), "content mismatch for %s", name)
	}
}

// TestTarFSDirEntryType verifies that DirEntry.Type() returns only
// type bits (not permission bits) for directories, files, and symlinks.
func TestTarFSDirEntryType(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeDir, Name: "dir/", Mode: 0o755},
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
		{Typeflag: tar.TypeSymlink, Name: "link", Linkname: "file.txt"},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)

	typeMap := make(map[string]fs.FileMode)
	for _, e := range entries {
		typeMap[e.Name()] = e.Type()
	}

	require.Equal(t, fs.ModeDir, typeMap["dir"])
	require.Equal(t, fs.FileMode(0), typeMap["file.txt"])
	require.Equal(t, fs.ModeSymlink, typeMap["link"])
}

// TestTarFSOpenFileCloseIdempotent verifies that file handles
// can be closed without error.
func TestTarFSOpenFileCloseIdempotent(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	f, err := fsys.Open("file.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestTarFSEmptyArchive verifies behavior with a tar containing
// no entries.
func TestTarFSEmptyArchive(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.Close())

	fsys, err := tarfs.Open(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	// Root should be accessible.
	info, err := fsys.Stat(".")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	// ReadDir root should be empty.
	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestTarFSLstatNonExistent verifies Lstat on a missing path returns error.
func TestTarFSLstatNonExistent(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	_, err := fsys.Lstat("missing")
	require.Error(t, err)
}

// TestTarFSReadLinkNonExistent verifies ReadLink on a missing path
// returns error.
func TestTarFSReadLinkNonExistent(t *testing.T) {
	fsys := buildTar(t, []tar.Header{
		{Typeflag: tar.TypeReg, Name: "file.txt", Mode: 0o644},
	}, map[string][]byte{
		"file.txt": []byte("data"),
	})

	_, err := fsys.ReadLink("missing")
	require.Error(t, err)
}
