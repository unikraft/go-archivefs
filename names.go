// SPDX-License-Identifier: MPL-2.0
/*
 * Copyright (C) 2026 Unikraft GmbH.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package archivefs

import (
	"fmt"
	"io/fs"
	"strings"
)

// ValidName reports whether name can be the name of a file in an archive: a
// single valid path element without a NUL.
func ValidName(name string) bool {
	return name != "." && !strings.ContainsAny(name, "/\x00") && fs.ValidPath(name)
}

// Names checks the paths that fs.WalkDir gives a writer from the root of a
// file system. The zero value is ready to use.
type Names struct {
	root bool
	seen map[string]bool
}

// Check returns an error that wraps fs.ErrInvalid if the path name with the
// entry d can't be in an archive: if the name of d isn't valid, or if name was
// already checked. Only the first "." is the root, as a child of the root
// named "." or "" also gets the path ".".
func (n *Names) Check(name string, d fs.DirEntry) error {
	if name == "." && !n.root {
		n.root = true
		return nil
	}
	if !ValidName(d.Name()) {
		return fs.ErrInvalid
	}
	if n.seen[name] {
		return fmt.Errorf("listed twice: %w", fs.ErrInvalid)
	}
	if n.seen == nil {
		n.seen = map[string]bool{}
	}
	n.seen[name] = true
	return nil
}
