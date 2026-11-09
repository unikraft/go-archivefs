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
	"os/exec"
	"testing"
)

// CheckEROFS checks the EROFS image at path with fsck.erofs in a subtest, which it skips if fsck.erofs is not installed.
func CheckEROFS(t *testing.T, path string) {
	t.Helper()
	t.Run("fsck.erofs", func(t *testing.T) {
		fsck, err := exec.LookPath("fsck.erofs")
		if err != nil {
			t.Skip("fsck.erofs is not installed")
		}
		if out, err := exec.Command(fsck, path).CombinedOutput(); err != nil {
			t.Fatalf("fsck.erofs %s: %v\n%s", path, err, out)
		}
	})
}
