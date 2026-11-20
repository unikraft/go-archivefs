// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package archivefs_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs"
)

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"f": true, ".f": true, "..f": true, "a b": true, `a\b`: true,
		"": false, ".": false, "..": false, "/": false, "a/b": false, "/a": false, "a\x00b": false,
	} {
		assert.Equal(t, want, archivefs.ValidName(name), "%q", name)
	}
}

func TestNamesCheck(t *testing.T) {
	src := fstest.MapFS{"d/f": {Mode: 0o644}}
	root, err := fs.Stat(src, ".")
	require.NoError(t, err)
	entries, err := fs.ReadDir(src, ".")
	require.NoError(t, err)
	d := entries[0]

	var names archivefs.Names
	require.NoError(t, names.Check(".", fs.FileInfoToDirEntry(root)))
	require.NoError(t, names.Check("d", d))
	require.ErrorIs(t, names.Check("d", d), fs.ErrInvalid)
	require.ErrorIs(t, names.Check(".", fs.FileInfoToDirEntry(root)), fs.ErrInvalid)
}
