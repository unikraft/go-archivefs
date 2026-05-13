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

func TestInodeModeSetuid(t *testing.T) {
	ino := Inode{mode: S_IFREG | S_ISUID | 0o755}
	mode := ino.Mode()

	if mode&fs.ModeSetuid == 0 {
		t.Error("Mode() missing ModeSetuid")
	}
	if mode.Perm() != 0o755 {
		t.Errorf("Perm() = 0o%o, want 0o755", mode.Perm())
	}
}

func TestInodeModeSetgid(t *testing.T) {
	ino := Inode{mode: S_IFREG | S_ISGID | 0o755}
	mode := ino.Mode()

	if mode&fs.ModeSetgid == 0 {
		t.Error("Mode() missing ModeSetgid")
	}
}

func TestInodeModeSticky(t *testing.T) {
	ino := Inode{mode: S_IFDIR | S_ISVTX | 0o755}
	mode := ino.Mode()

	if mode&fs.ModeSticky == 0 {
		t.Error("Mode() missing ModeSticky")
	}
	if !mode.IsDir() {
		t.Error("sticky dir Mode() missing ModeDir")
	}
}

func TestStatModeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		mode fs.FileMode
	}{
		{"setuid", fs.ModeSetuid | 0o755},
		{"setgid", fs.ModeSetgid | 0o755},
		{"sticky dir", fs.ModeDir | fs.ModeSticky | 0o755},
		{"setuid+setgid", fs.ModeSetuid | fs.ModeSetgid | 0o755},
		{"all special bits dir", fs.ModeDir | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky | 0o755},
		{"regular no special", 0o644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stMode := statModeFromFileMode(tt.mode)
			ino := Inode{mode: stMode}
			got := ino.Mode()
			if got != tt.mode {
				t.Errorf("round-trip: statModeFromFileMode(0o%o) -> 0o%o -> Mode() = 0o%o, want 0o%o",
					tt.mode, stMode, got, tt.mode)
			}
		})
	}
}

func TestEncodeDeviceID(t *testing.T) {
	tests := []struct {
		name  string
		major uint32
		minor uint32
	}{
		{"null device (0, 0)", 0, 0},
		{"sda (8, 0)", 8, 0},
		{"sda1 (8, 1)", 8, 1},
		{"large minor (0, 256)", 0, 256},
		{"large major (255, 0)", 255, 0},
		{"both large (4095, 1048575)", 4095, 1048575},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeDeviceID(tt.major, tt.minor)
			gotMajor, gotMinor := decodeDeviceID(encoded)
			if gotMajor != tt.major || gotMinor != tt.minor {
				t.Errorf("encodeDeviceID(%d,%d)=0x%x; decodeDeviceID -> (%d,%d), want (%d,%d)",
					tt.major, tt.minor, encoded, gotMajor, gotMinor, tt.major, tt.minor)
			}
		})
	}
}

func TestIsSpecialFile(t *testing.T) {
	tests := []struct {
		name string
		mode uint16
		want bool
	}{
		{"regular", S_IFREG | 0o644, false},
		{"directory", S_IFDIR | 0o755, false},
		{"symlink", S_IFLNK | 0o777, false},
		{"char device", S_IFCHR | 0o660, true},
		{"block device", S_IFBLK | 0o660, true},
		{"fifo", S_IFIFO | 0o644, true},
		{"socket", S_IFSOCK | 0o600, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSpecialFile(tt.mode); got != tt.want {
				t.Errorf("isSpecialFile(0o%o) = %v, want %v", tt.mode, got, tt.want)
			}
		})
	}
}
