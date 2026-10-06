// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Package termui is a tiny dependency-free terminal canvas renderer.
// It draws unicode box art with 256 color ANSI escapes and supports a
// plain (ANSI-free) mode used for screenshots and logs.
package termui

import (
	"fmt"
	"strings"
	"time"
)

// Style describes colors of a cell. Fg/Bg are 256-color codes; -1 = default.
type Style struct {
	Fg   int
	Bg   int
	Bold bool
	Dim  bool
}

// Default is the terminal default style.
var Default = Style{Fg: -1, Bg: -1}

func (s Style) seq() string {
	if s.Fg < 0 && s.Bg < 0 && !s.Bold && !s.Dim {
		return ""
	}
	var parts []string
	if s.Fg >= 0 {
		parts = append(parts, fmt.Sprintf("38;5;%d", s.Fg))
	}
	if s.Bg >= 0 {
		parts = append(parts, fmt.Sprintf("48;5;%d", s.Bg))
	}
	if s.Bold {
		parts = append(parts, "1")
	}
	if s.Dim {
		parts = append(parts, "2")
	}
	return "\x1b[" + strings.Join(parts, ";") + "m"
}

// Cell is one character position on the canvas.
type Cell struct {
	Ch rune
	St Style
}

// Canvas is a rune grid that can be rendered to ANSI or plain text.
type Canvas struct {
	W, H  int
	cells []Cell
}

// NewCanvas creates a blank canvas of the given size.
func NewCanvas(w, h int) *Canvas {
	if w < 1 {
		w = 1
	}
	if h < 1 {
		h = 1
	}
	c := &Canvas{W: w, H: h, cells: make([]Cell, w*h)}
	for i := range c.cells {
		c.cells[i].Ch = ' '
		c.cells[i].St = Default
	}
	return c
}

func (c *Canvas) inBounds(x, y int) bool {
	return x >= 0 && y >= 0 && x < c.W && y < c.H
}

// Set writes a single rune.
func (c *Canvas) Set(x, y int, ch rune, st Style) {
	if !c.inBounds(x, y) {
		return
	}
	c.cells[y*c.W+x] = Cell{Ch: ch, St: st}
}

// Text writes a string (single-width runes assumed) and returns the x
// position just past the last rune.
func (c *Canvas) Text(x, y int, s string, st Style) int {
	i := 0
	for _, r := range s {
		if r == '\n' {
			break
		}
		if c.inBounds(x+i, y) {
			c.cells[y*c.W+x+i] = Cell{Ch: r, St: st}
		}
		i++
	}
	return x + i
}

// TextCenter writes s centered in the [x, x+w) band.
func (c *Canvas) TextCenter(x, y, w int, s string, st Style) {
	n := len([]rune(s))
	if n > w {
		s = string([]rune(s)[:w])
		n = w
	}
	return0 := x + (w-n)/2
	c.Text(return0, y, s, st)
}

// TextRight writes s right-aligned to end at x+w.
func (c *Canvas) TextRight(x, y, w int, s string, st Style) {
	n := len([]rune(s))
	if n > w {
		s = string([]rune(s)[n-w:])
		n = w
	}
	c.Text(x+w-n, y, s, st)
}

// HLine draws a horizontal line of runes.
func (c *Canvas) HLine(x, y, w int, ch rune, st Style) {
	for i := 0; i < w; i++ {
		c.Set(x+i, y, ch, st)
	}
}

// VLine draws a vertical line of runes.
func (c *Canvas) VLine(x, y, h int, ch rune, st Style) {
	for j := 0; j < h; j++ {
		c.Set(x, y+j, ch, st)
	}
}

// Box draws a single-line border rectangle.
func (c *Canvas) Box(x, y, w, h int, st Style) {
	if w < 2 || h < 2 {
		return
	}
	c.Set(x, y, '┌', st)
	c.Set(x+w-1, y, '┐', st)
	c.Set(x, y+h-1, '└', st)
	c.Set(x+w-1, y+h-1, '┘', st)
	for i := 1; i < w-1; i++ {
		c.Set(x+i, y, '─', st)
		c.Set(x+i, y+h-1, '─', st)
	}
	for j := 1; j < h-1; j++ {
		c.Set(x, y+j, '│', st)
		c.Set(x+w-1, y+j, '│', st)
	}
}

// TitledBox draws a border rectangle with an embedded title.
func (c *Canvas) TitledBox(x, y, w, h int, title string, border, titleSt Style) {
	c.Box(x, y, w, h, border)
	if title == "" || w < 6 {
		return
	}
	t := " " + title + " "
	r := []rune(t)
	if len(r) > w-4 {
		r = append([]rune(" …"), r[len(r)-(w-6):]...)
	}
	c.Text(x+2, y, string(r), titleSt)
}

// Bar draws a progress bar (filled with '█', empty with '░').
func (c *Canvas) Bar(x, y, w int, frac float64, st, empty Style) {
	if w <= 0 {
		return
	}
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := int(frac*float64(w) + 0.5)
	for i := 0; i < w; i++ {
		if i < filled {
			c.Set(x+i, y, '█', st)
		} else {
			c.Set(x+i, y, '░', empty)
		}
	}
}

var blocks = []rune(" ▁▂▃▄▅▆▇█")

// Spark draws a sparkline of the last w values, right aligned.
func (c *Canvas) Spark(x, y, w int, vals []float64, st Style) {
	if w <= 0 {
		return
	}
	v := vals
	if len(v) > w {
		v = v[len(v)-w:]
	}
	max := 0.0
	for _, f := range v {
		if f > max {
			max = f
		}
	}
	off := w - len(v)
	for i := 0; i < off; i++ {
		c.Set(x+i, y, ' ', Default)
	}
	for i, f := range v {
		idx := 0
		if max > 0 {
			idx = int(f / max * 7.999)
			if idx > 7 {
				idx = 7
			}
		}
		c.Set(x+off+i, y, blocks[idx], st)
	}
}

// Render produces the full-frame ANSI string (cursor home, styled runs,
// clear-to-end-of-line and clear-below so smaller frames erase leftovers).
func (c *Canvas) Render() string {
	var b strings.Builder
	b.Grow(c.W * c.H * 6)
	b.WriteString("\x1b[H")
	for y := 0; y < c.H; y++ {
		var cur *Style
		for x := 0; x < c.W; x++ {
			cell := c.cells[y*c.W+x]
			st := cell.St
			if cur == nil || *cur != st {
				b.WriteString("\x1b[0m")
				if s := st.seq(); s != "" {
					b.WriteString(s)
				}
				cur = &st
			}
			b.WriteRune(cell.Ch)
		}
		b.WriteString("\x1b[0m\x1b[K")
		if y < c.H-1 {
			b.WriteByte('\n')
		}
	}
	b.WriteString("\x1b[0m\x1b[J")
	return b.String()
}

// RenderPlain produces the frame without any ANSI escapes
// (used for screenshots and log files).
func (c *Canvas) RenderPlain() string {
	var b strings.Builder
	b.Grow(c.W * (c.H + 1))
	line := make([]rune, c.W)
	for y := 0; y < c.H; y++ {
		for x := 0; x < c.W; x++ {
			line[x] = c.cells[y*c.W+x].Ch
		}
		b.WriteString(strings.TrimRight(string(line), " "))
		if y < c.H-1 {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// Spinner returns the n-th braille spinner frame.
func Spinner(n int) rune {
	frames := []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}
	return frames[n%len(frames)]
}

// Truncate shortens s to at most w runes, appending '…' when cut.
func Truncate(s string, w int) string {
	r := []rune(s)
	if len(r) <= w {
		return s
	}
	if w <= 1 {
		return string(r[:w])
	}
	return string(r[:w-1]) + "…"
}

// PadRight pads s with spaces to exactly w runes (truncating if longer).
func PadRight(s string, w int) string {
	r := []rune(s)
	if len(r) >= w {
		return string(r[:w])
	}
	return s + strings.Repeat(" ", w-len(r))
}

// FmtDur formats a duration as MM:SS (or H:MM:SS when long).
func FmtDur(d time.Duration) string {
	total := int(d.Seconds())
	h := total / 3600
	m := (total % 3600) / 60
	s := total % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

// EnterAltScreen switches to the alternate screen buffer and hides the cursor.
func EnterAltScreen() { fmt.Print("\x1b[?1049h\x1b[?25l") }

// ExitAltScreen restores the primary buffer and cursor.
func ExitAltScreen() { fmt.Print("\x1b[?25h\x1b[?1049l\x1b[0m") }
