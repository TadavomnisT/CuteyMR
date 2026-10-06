// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package common

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// EventLog is a fixed size ring buffer of timestamped events.
// Dashboards render the tail of it; in headless mode lines are also
// printed to stdout.
type EventLog struct {
	mu    sync.Mutex
	lines []string
	max   int
	print func(line string)
}

// NewEventLog creates a log keeping at most max lines.
func NewEventLog(max int) *EventLog {
	return &EventLog{max: max}
}

// SetPrint installs a callback invoked for every pushed line
// (used for headless output).
func (l *EventLog) SetPrint(fn func(line string)) {
	l.mu.Lock()
	l.print = fn
	l.mu.Unlock()
}

// Push appends a formatted event.
func (l *EventLog) Push(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	line := time.Now().Format("15:04:05") + " " + msg
	l.mu.Lock()
	l.lines = append(l.lines, line)
	if len(l.lines) > l.max {
		l.lines = l.lines[len(l.lines)-l.max:]
	}
	fn := l.print
	l.mu.Unlock()
	if fn != nil {
		fn(line)
	}
}

// Last returns up to n most recent lines, oldest first.
func (l *EventLog) Last(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n > len(l.lines) {
		n = len(l.lines)
	}
	out := make([]string, n)
	copy(out, l.lines[len(l.lines)-n:])
	return out
}

// TotalMemoryMB reads total RAM from /proc/meminfo (Linux); 0 when unknown.
func TotalMemoryMB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			var kb int64
			if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &kb); err == nil {
				return kb / 1024
			}
		}
	}
	return 0
}
