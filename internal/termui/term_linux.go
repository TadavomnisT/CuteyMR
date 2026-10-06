// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

//go:build linux

package termui

import (
	"fmt"
	"syscall"
	"unsafe"
)

type winsize struct {
	Row, Col, Xpixel, Ypixel uint16
}

// TerminalSize returns the size of stdout via the TIOCGWINSZ ioctl.
func TerminalSize() (w, h int, err error) {
	ws := &winsize{}
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(syscall.Stdout),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)),
	)
	if errno != 0 {
		return 0, 0, fmt.Errorf("stdout is not a terminal")
	}
	return int(ws.Col), int(ws.Row), nil
}
