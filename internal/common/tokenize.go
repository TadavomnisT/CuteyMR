// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package common

import (
	"fmt"
	"strconv"
	"strings"
)

// The tokenizer is the single source of truth for "what is a word".
// Both the distributed mappers and the sequential reference verifier use it,
// which is what makes the end-to-end verification possible.
//
// A word is a maximal run of [a-z0-9'-] inside the lowercased text.
// ("don't", "well-known" and "e-mail" count as single words; punctuation
// alone is ignored.)

func isWordByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '\'' || b == '-'
}

func hasAlnum(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') {
			return true
		}
	}
	return false
}

// TokenizeStream lowercases text and emits every word together with the
// byte offset just after it (used for live progress reporting).
func TokenizeStream(text string, emit func(word string, endPos int)) {
	low := strings.ToLower(text)
	n := len(low)
	i := 0
	for i < n {
		for i < n && !isWordByte(low[i]) {
			i++
		}
		if i >= n {
			break
		}
		j := i + 1
		for j < n && isWordByte(low[j]) {
			j++
		}
		word := low[i:j]
		if hasAlnum(word) {
			emit(word, j)
		}
		i = j
	}
}

// Tokenize returns all words of the text in order.
func Tokenize(text string) []string {
	var out []string
	TokenizeStream(text, func(word string, _ int) { out = append(out, word) })
	return out
}

// Fnv32a is the classic FNV-1a 32 bit hash, used to partition words across
// reduce tasks. Same word -> same hash on every machine, always.
func Fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// PartitionOf maps a word to a partition index in [0, n).
func PartitionOf(word string, n int) int {
	if n <= 1 {
		return 0
	}
	return int(Fnv32a(word) % uint32(n))
}

// Humanize renders a count in a compact human readable form (123456 -> 123k).
func Humanize(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1e6)
	case n >= 10_000:
		return fmt.Sprintf("%.1fk", float64(n)/1e3)
	case n >= 1_000:
		return fmt.Sprintf("%.2fk", float64(n)/1e3)
	default:
		return strconv.FormatInt(n, 10)
	}
}

// HumanBytes renders a byte count (e.g. 1.4MB).
func HumanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fGB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fkB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
