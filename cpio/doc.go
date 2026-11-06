// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2017, Ryan Armstrong.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

/*
Package cpio providers readers and writers for CPIO archives. Currently, only
the SVR4 (New ASCII) format is supported, both with and without checksums.

This package aims to be feel like Go's archive/tar package.

OpenFS gives read-only access to the files of an archive through the io/fs
interfaces, with owners, inodes, hard links, symbolic links and device nodes.

Create writes the files of an fs.FS to an archive, with owners, hard links,
symbolic links and device nodes. Symbolic links need an fs.ReadLinkFS. The
files must not change while Create runs, and after an error dst can hold part
of an archive. CPIO cannot keep extended attributes, thus Create does not write them.

See the CPIO man page: https://www.freebsd.org/cgi/man.cgi?query=cpio&sektion=5
*/
package cpio
