// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package testutil

import (
	"io/fs"
	"testing/fstest"
)

// RenamedFS is a MapFS whose root lists the entry File a second time, under
// the name Name.
type RenamedFS struct {
	fstest.MapFS
	File string
	Name string
}

type renamedEntry struct {
	fs.DirEntry
	name string
}

func (e renamedEntry) Name() string { return e.name }

// ReadDir implements fs.ReadDirFS.
func (f RenamedFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := f.MapFS.ReadDir(name)
	if err != nil || name != "." {
		return entries, err
	}
	fi, err := f.Stat(f.File)
	if err != nil {
		return nil, err
	}
	return append(entries, renamedEntry{fs.FileInfoToDirEntry(fi), f.Name}), nil
}
