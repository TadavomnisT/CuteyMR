// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

//go:build !linux

package termui

import "errors"

// TerminalSize is only implemented for Linux; other platforms fall back
// to a fixed size.
func TerminalSize() (w, h int, err error) {
	return 0, 0, errors.New("terminal size detection not supported on this platform")
}
