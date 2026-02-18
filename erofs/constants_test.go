package erofs

import (
	"io/fs"
	"testing"
)

func TestStatModeFromFileModeCharDevice(t *testing.T) {
	// Go convention: char devices have ModeDevice | ModeCharDevice.
	goMode := fs.ModeDevice | fs.ModeCharDevice | 0o666
	stMode := statModeFromFileMode(goMode)

	if stMode&S_IFMT != S_IFCHR {
		t.Errorf("statModeFromFileMode(ModeDevice|ModeCharDevice) = 0o%o, want S_IFCHR (0o%o) in type bits", stMode&S_IFMT, S_IFCHR)
	}
}

func TestStatModeFromFileModeBlockDevice(t *testing.T) {
	goMode := fs.ModeDevice | 0o660
	stMode := statModeFromFileMode(goMode)

	if stMode&S_IFMT != S_IFBLK {
		t.Errorf("statModeFromFileMode(ModeDevice) = 0o%o, want S_IFBLK (0o%o) in type bits", stMode&S_IFMT, S_IFBLK)
	}
}

func TestFileTypeFromFileModeCharDevice(t *testing.T) {
	goMode := fs.ModeDevice | fs.ModeCharDevice
	ft := fileTypeFromFileMode(goMode)

	if ft != FT_CHRDEV {
		t.Errorf("fileTypeFromFileMode(ModeDevice|ModeCharDevice) = %d, want FT_CHRDEV (%d)", ft, FT_CHRDEV)
	}
}

func TestInodeModeCharDevice(t *testing.T) {
	// Simulate reading a char device inode.
	ino := Inode{mode: S_IFCHR | 0o666}
	mode := ino.Mode()

	if mode&fs.ModeDevice == 0 {
		t.Error("char device Mode() missing ModeDevice")
	}
	if mode&fs.ModeCharDevice == 0 {
		t.Error("char device Mode() missing ModeCharDevice")
	}
	if mode.Type() != fs.ModeDevice|fs.ModeCharDevice {
		t.Errorf("char device Mode().Type() = %v, want ModeDevice|ModeCharDevice", mode.Type())
	}
}

func TestInodeModeBlockDevice(t *testing.T) {
	ino := Inode{mode: S_IFBLK | 0o660}
	mode := ino.Mode()

	if mode&fs.ModeDevice == 0 {
		t.Error("block device Mode() missing ModeDevice")
	}
	if mode&fs.ModeCharDevice != 0 {
		t.Error("block device Mode() should not have ModeCharDevice")
	}
	if mode.Type() != fs.ModeDevice {
		t.Errorf("block device Mode().Type() = %v, want ModeDevice", mode.Type())
	}
}
