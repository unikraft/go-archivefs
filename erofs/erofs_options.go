// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Unikraft GmbH and The KraftKit Authors.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.
package erofs

import (
	"fmt"
	"io/fs"
	"time"
)

type ErofsCreateOptions struct {
	// allRoot indicates whether all files in the Erofs archive should be
	// set to root:root (uid 0, gid 0) with default mode, regardless of the original
	allRoot bool

	// Map of file information for each file in the Erofs archive
	// This is used to set the uid, gid, and mode for each file.
	fInfoMap map[string]FileInfo

	// uuid is the volume UUID in the superblock.
	// If it is nil, the writer uses a random UUID.
	uuid *[16]byte

	// buildTime is the build time in the superblock.
	// If it is nil, the writer uses the current time.
	buildTime *time.Time
}

type ErofsCreateOption func(*ErofsCreateOptions) error

// WithAllRoot toggles whether all files permissions should be set to root:root
// instead of the original file permissions.
func WithAllRoot(allRoot bool) ErofsCreateOption {
	return func(eo *ErofsCreateOptions) error {
		eo.allRoot = allRoot
		return nil
	}
}

type FileInfo struct {
	Uid  int
	Gid  int
	Mode fs.FileMode
	Name string
}

// WithFileInfoMap sets the file information map for the Erofs archive.
func WithFileInfoMap(fInfoMap map[string]FileInfo) ErofsCreateOption {
	return func(eo *ErofsCreateOptions) error {
		eo.fInfoMap = fInfoMap
		return nil
	}
}

// WithUUID sets the volume UUID in the superblock. A zero UUID clears it.
// Use it with WithBuildTime to make the same input give the same image.
func WithUUID(uuid [16]byte) ErofsCreateOption {
	return func(eo *ErofsCreateOptions) error {
		eo.uuid = &uuid
		return nil
	}
}

// WithBuildTime sets the build time in the superblock. Compact inodes
// also use this time as their modification time.
func WithBuildTime(t time.Time) ErofsCreateOption {
	return func(eo *ErofsCreateOptions) error {
		if t.Unix() < 0 {
			return fmt.Errorf("build time %s is before the Unix epoch", t)
		}
		eo.buildTime = &t
		return nil
	}
}
