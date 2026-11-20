//go:build linux

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
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestHardLinksOtherDevice(t *testing.T) {
	fsys := fstest.MapFS{
		"a":  {Data: []byte("a"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 7, Nlink: 2}},
		"a2": {Data: []byte("a"), Mode: 0o644, Sys: &syscall.Stat_t{Dev: 1, Ino: 7, Nlink: 2}},
		"b":  {Data: []byte("bb"), Mode: 0o600, Sys: &syscall.Stat_t{Dev: 2, Ino: 7, Nlink: 2}},
	}

	got := addLinks(t, fsys, "a", "b", "a2")
	require.Equal(t, []string{"a", "b", "a"}, got)
}
