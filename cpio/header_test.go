// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026, Unikraft GmbH.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio_test

import (
	"testing"

	"github.com/unikraft/go-archivefs/cpio"
)

func TestFileModeType(t *testing.T) {
	tests := []struct {
		mode         cpio.FileMode
		dir, regular bool
	}{
		{mode: cpio.TypeRegular | 0o644, regular: true},
		{mode: cpio.TypeRegular | cpio.ModeSetuid | cpio.ModeSetgid | 0o755, regular: true},
		{mode: cpio.TypeDir | cpio.ModeSticky | 0o777, dir: true},
		{mode: cpio.TypeSymlink | 0o777},
		{mode: cpio.TypeDevice | 0o660},
		{mode: cpio.TypeCharDevice | 0o666},
		{mode: cpio.TypeNamedPipe | 0o644},
		{mode: cpio.TypeSocket | 0o755},
	}

	for _, tt := range tests {
		if got := tt.mode.IsDir(); got != tt.dir {
			t.Errorf("%v: IsDir() = %v, want %v", tt.mode, got, tt.dir)
		}
		if got := tt.mode.IsRegular(); got != tt.regular {
			t.Errorf("%v: IsRegular() = %v, want %v", tt.mode, got, tt.regular)
		}
	}
}
