// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package erofs_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs/erofs"
)

func TestXattrsSharedFromMkfs(t *testing.T) {
	mkfs, err := exec.LookPath("mkfs.erofs")
	if err != nil {
		t.Skip("mkfs.erofs is not installed")
	}

	// mkfs.erofs shares an xattr that many files of a directory source have.
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	require.NoError(t, os.Mkdir(src, 0o755))
	want := map[string]map[string]string{}
	for i := range 5 {
		name := fmt.Sprintf("f%d", i)
		p := filepath.Join(src, name)
		require.NoError(t, os.WriteFile(p, []byte(name), 0o644))
		xattrs := map[string]string{"user.shared": "same", "user.own": name}
		for k, v := range xattrs {
			if err := syscall.Setxattr(p, k, []byte(v), 0); err != nil {
				t.Skipf("cannot set user xattrs in %s: %v", dir, err)
			}
		}
		want[name] = xattrs
	}

	imgPath := filepath.Join(dir, "shared.img")
	out, err := exec.Command(mkfs, "-x2", imgPath, src).CombinedOutput()
	require.NoError(t, err, string(out))

	img, err := os.Open(imgPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, img.Close()) })

	fsys, err := erofs.Open(img)
	require.NoError(t, err)
	// The host can add other xattrs, such as an SELinux label.
	checkXattrs(t, fsys, want, "user.")
}
