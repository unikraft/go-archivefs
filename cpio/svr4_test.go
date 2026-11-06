// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2017, Ryan Armstrong.
// Licensed under the BSD-3-Clause License (the "License").
// You may not use this file except in compliance with the License.

package cpio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRead(t *testing.T) {
	f, err := os.Open("testdata/test_svr4_crc.cpio")
	if err != nil {
		t.Fatalf("error opening test file: %v", err)
	}
	defer f.Close()
	r := NewReader(f)
	for {
		_, err := r.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			t.Errorf("error moving to next header: %v", err)
			return
		}
		// TODO: validate header fields
	}
}

func TestSVR4CRC(t *testing.T) {
	f, err := os.Open("testdata/test_svr4_crc.cpio")
	if err != nil {
		t.Fatalf("error opening test file: %v", err)
	}
	defer f.Close()
	w := NewHash()
	r := NewReader(f)
	for {
		hdr, err := r.Next()
		if err != nil {
			if err != io.EOF {
				t.Errorf("error moving to next header: %v", err)
			}
			return
		}
		if hdr.Mode.IsRegular() {
			w.Reset()
			_, err = io.CopyN(w, r, hdr.Size)
			if err != nil {
				t.Fatalf("error writing to checksum hash: %v", err)
			}
			sum := w.Sum32()
			if sum != hdr.Checksum {
				t.Errorf("expected checksum %v, got %v for %v", hdr.Checksum, sum, hdr.Name)
			}
		}
	}
}

func TestSVR4DeviceNumbers(t *testing.T) {
	want := Header{
		Name:      "dev/tty",
		Mode:      TypeCharDevice | 0o620,
		DevMajor:  0x08,
		DevMinor:  0x21,
		RdevMajor: 0x04,
		RdevMinor: 0x1234,
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	hdr := want
	if err := w.WriteHeader(&hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := NewReader(&buf).Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.DevMajor != want.DevMajor || got.DevMinor != want.DevMinor ||
		got.RdevMajor != want.RdevMajor || got.RdevMinor != want.RdevMinor {
		t.Errorf("device numbers = %d:%d %d:%d, want %d:%d %d:%d",
			got.DevMajor, got.DevMinor, got.RdevMajor, got.RdevMinor,
			want.DevMajor, want.DevMinor, want.RdevMajor, want.RdevMinor)
	}
}

func ExampleNewHash() {
	// Open the cpio archive for reading.
	f, err := os.Open("testdata/test_svr4_crc.cpio")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	r := NewReader(f)

	// Iterate through the files in the archive.
	for {
		hdr, err := r.Next()
		if err == io.EOF {
			// end of cpio archive
			return
		}
		if err != nil {
			log.Fatal(err)
		}

		// skip symlinks, directories, etc.
		if !hdr.Mode.IsRegular() {
			continue
		}

		// read file into hash
		h := NewHash()
		_, err = io.CopyN(h, r, hdr.Size)
		if err != nil {
			log.Fatal(err)
		}

		// check hash matches header checksum
		sum := h.Sum32()
		if sum == hdr.Checksum {
			fmt.Printf("Checksum OK: %s (%08X)\n", hdr.Name, hdr.Checksum)
		} else {
			fmt.Printf("Checksum FAIL: %s - expected %08X, got %08X\n", hdr.Name, hdr.Checksum, sum)
		}
	}

	// Output:
	// Checksum OK: gophers.txt (00000C98)
	// Checksum OK: readme.txt (00000E3D)
	// Checksum OK: todo.txt (00000A52)
}

func TestWriteHeaderRange(t *testing.T) {
	tests := []struct {
		desc string
		hdr  Header
	}{
		{"size too large", Header{Name: "file", Size: 1 << 32}},
		{"inode too large", Header{Name: "file", Inode: 1 << 32}},
		{"negative device number", Header{Name: "file", RdevMinor: -1}},
		{"time before 1970", Header{Name: "file", ModTime: time.Unix(-1, 0)}},
		{"time after 2106", Header{Name: "file", ModTime: time.Unix(1<<32, 0)}},
		{"name too long", Header{Name: strings.Repeat("a", 4096)}},
		{"NUL byte in name", Header{Name: "a\x00b"}},
		{"NUL byte in link target", Header{Name: "link", Mode: TypeSymlink, Size: 3, Linkname: "a\x00b"}},
		{"empty symlink target", Header{Name: "link", Mode: TypeSymlink}},
		{"symlink target too long", Header{Name: "link", Mode: TypeSymlink, Size: 4097}},
	}

	for _, tt := range tests {
		w := NewWriter(io.Discard)
		if err := w.WriteHeader(&tt.hdr); !errors.Is(err, ErrHeader) {
			t.Errorf("%s: WriteHeader error = %v, want %v", tt.desc, err, ErrHeader)
		}
	}
}
