// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux darwin

package textutil

import (
	"syscall"
	"unsafe"
)

// TerminalSize returns the dimensions of the terminal, if it's available from
// the OS, otherwise returns an error.
func TerminalSize() (row, col int, _ error) {
	// Try getting the terminal size from stdout, stderr and stdin respectively.
	// We try each of these in turn because the mechanism we're using fails if any
	// of the fds is redirected on the command line.  E.g. "tool | less" redirects
	// the stdout of tool to the stdin of less, and will mean tool cannot retrieve
	// the terminal size from stdout.
	//
	// TODO(toddw): This probably only works on some linux / unix variants; add
	// build tags and support different platforms.
	if row, col, err := terminalSize(syscall.Stdout); err == nil {
		return row, col, err
	}
	if row, col, err := terminalSize(syscall.Stderr); err == nil {
		return row, col, err
	}
	return terminalSize(syscall.Stdin)
}

func terminalSize(fd int) (int, int, error) {
	var ws winsize
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws))); err != 0 {
		return 0, 0, err
	}
	return int(ws.row), int(ws.col), nil
}

// winsize must correspond to the struct defined in "sys/ioctl.h".  Do not
// export this struct; it's a platform-specific implementation detail.
type winsize struct {
	row, col, xpixel, ypixel uint16
}
