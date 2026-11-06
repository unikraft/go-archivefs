// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2017, Ryan Armstrong.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio_test

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"testing/fstest"

	"github.com/unikraft/go-archivefs/cpio"
)

func Example() {
	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)

	// Create a new cpio archive.
	w := cpio.NewWriter(buf)

	// Add some files to the archive.
	files := []struct {
		Name, Body string
	}{
		{"readme.txt", "This archive contains some text files."},
		{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
		{"todo.txt", "Get animal handling license."},
	}
	for _, file := range files {
		hdr := &cpio.Header{
			Name: file.Name,
			Mode: 0o600,
			Size: int64(len(file.Body)),
		}
		if err := w.WriteHeader(hdr); err != nil {
			log.Fatalln(err)
		}
		if _, err := w.Write([]byte(file.Body)); err != nil {
			log.Fatalln(err)
		}
	}

	// Make sure to check the error on Close.
	if err := w.Close(); err != nil {
		log.Fatalln(err)
	}

	// Open the cpio archive for reading.
	r := cpio.NewReader(buf)

	// Iterate through the files in the archive.
	for {
		hdr, err := r.Next()
		if err == io.EOF {
			// end of cpio archive
			break
		}
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Contents of %s:\n", hdr.Name)
		if _, err := io.Copy(os.Stdout, r); err != nil {
			log.Fatalln(err)
		}
		fmt.Println()
	}

	// Output:
	// Contents of readme.txt:
	// This archive contains some text files.
	// Contents of gopher.txt:
	// Gopher names:
	// George
	// Geoffrey
	// Gonzo
	// Contents of todo.txt:
	// Get animal handling license.
}

func ExampleOpenFS() {
	// Open the cpio archive for reading.
	f, err := os.Open("testdata/test_svr4.cpio")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	fsys, err := cpio.OpenFS(f)
	if err != nil {
		log.Fatalln(err)
	}

	// Iterate through the files in the archive.
	if err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fmt.Println(d.Type(), name)
		return nil
	}); err != nil {
		log.Fatalln(err)
	}

	// Read a file through a symbolic link.
	b, err := fs.ReadFile(fsys, "checklist.txt")
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("%s", b)

	// Output:
	// d--------- .
	// L--------- checklist.txt
	// ---------- gophers.txt
	// ---------- readme.txt
	// ---------- todo.txt
	// Get animal handling license.
}

func ExampleCreate() {
	src := fstest.MapFS{
		"bin":          {Mode: fs.ModeDir | 0o755},
		"bin/ash":      {Mode: fs.ModeSymlink | 0o777, Data: []byte("sh")},
		"bin/sh":       {Mode: 0o755, Data: []byte("#!/bin/sh\n")},
		"etc":          {Mode: fs.ModeDir | 0o755},
		"etc/hostname": {Mode: 0o644, Data: []byte("unikraft\n")},
	}

	// Write the files of src to an archive.
	var buf bytes.Buffer
	if err := cpio.Create(&buf, src); err != nil {
		log.Fatalln(err)
	}

	// Read the archive back.
	fsys, err := cpio.OpenFS(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatalln(err)
	}
	if err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		fmt.Println(fi.Mode(), name)
		return nil
	}); err != nil {
		log.Fatalln(err)
	}

	// Output:
	// drwxr-xr-x .
	// drwxr-xr-x bin
	// Lrwxrwxrwx bin/ash
	// -rwxr-xr-x bin/sh
	// drwxr-xr-x etc
	// -rw-r--r-- etc/hostname
}
