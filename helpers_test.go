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
	"archive/tar"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unikraft/go-archivefs"
)

type xattrSys map[string]string

func (x xattrSys) GetXattrs() (map[string]string, error) { return x, nil }

func TestGetXattrs(t *testing.T) {
	hdr := &tar.Header{
		PAXRecords: map[string]string{
			"SCHILY.xattr.security.capability": "cap",
			"SCHILY.xattr.user.a":              "pax",
			"SCHILY.xattr.user.b%3Dc%25":       "escaped",
			"SCHILY.xattr.user.50%41":          "unescaped",
			"mtime":                            "1",
		},
		Xattrs: map[string]string{ //nolint:staticcheck // The test covers the deprecated field.
			"user.a":        "new",
			"user.b%3Dc%25": "escaped",
			"user.d":        "d",
			"user.e%25":     "literal",
			"user.e%":       "literal too",
		},
	}
	xattrs, err := archivefs.GetXattrs(hdr)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"security.capability": "cap",
		"user.a":              "new",
		"user.b=c%":           "escaped",
		"user.50%41":          "unescaped",
		"user.d":              "d",
		"user.e%25":           "literal",
		"user.e%":             "literal too",
	}, xattrs)

	for _, sys := range []any{&tar.Header{PAXRecords: map[string]string{"mtime": "1"}}, nil} {
		xattrs, err := archivefs.GetXattrs(sys)
		require.NoError(t, err)
		require.Nil(t, xattrs)
	}

	xattrs, err = archivefs.GetXattrs(xattrSys{"user.x": "y"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"user.x": "y"}, xattrs)
}

func TestGetXattrsSameName(t *testing.T) {
	hdr := &tar.Header{PAXRecords: map[string]string{
		"SCHILY.xattr.user.a%3Db": "upper",
		"SCHILY.xattr.user.a%3db": "lower",
	}}
	for range 20 {
		xattrs, err := archivefs.GetXattrs(hdr)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"user.a=b": "lower"}, xattrs)
	}
}

func TestSetTarXattrs(t *testing.T) {
	hdr := &tar.Header{
		PAXRecords: map[string]string{"SCHILY.xattr.user.old": "old"},
		Xattrs:     map[string]string{"user.older": "older"}, //nolint:staticcheck // The test covers the deprecated field.
	}
	archivefs.SetTarXattrs(hdr, map[string]string{"user.b=c%": "v", "user.a": "a"})
	require.Equal(t, map[string]string{"SCHILY.xattr.user.b%3Dc%25": "v", "SCHILY.xattr.user.a": "a"}, hdr.PAXRecords)

	xattrs, err := archivefs.GetXattrs(hdr)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"user.b=c%": "v", "user.a": "a"}, xattrs)
}
