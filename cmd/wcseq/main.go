// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Command mr-wcseq is the sequential reference implementation of the
// wordcount. It uses exactly the same tokenizer as the distributed mappers,
// so its output must match the MapReduce result byte for byte.
//
// Modes:
//
//	mr-wcseq                          count data/input, print summary
//	mr-wcseq --out ref.json           also write the counts as JSON
//	mr-wcseq --check out.json         verify a master output against the input
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cuteymr/internal/common"
)

func main() {
	var (
		input = flag.String("input", "data/input", "input file(s) or directory (comma separated)")
		out   = flag.String("out", "", "write reference counts to this JSON file")
		check = flag.String("check", "", "verify this JSON result against the recomputed reference")
	)
	flag.Parse()

	files := gather(strings.Split(*input, ","))
	counts := make(map[string]int64, 1<<16)
	var total int64
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "warning: cannot read", f, err)
			continue
		}
		common.TokenizeStream(string(data), func(word string, _ int) {
			counts[word]++
			total++
		})
	}

	if *check != "" {
		verify(counts, total, *check)
		return
	}

	if *out != "" {
		data, err := json.MarshalIndent(counts, "", "  ")
		if err != nil || os.WriteFile(*out, data, 0o644) != nil {
			fmt.Fprintln(os.Stderr, "fatal: cannot write", *out)
			os.Exit(1)
		}
		fmt.Println("reference counts written to", *out)
	}
	report(counts, total)
}

func gather(paths []string) []string {
	var files []string
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		if info.IsDir() {
			entries, _ := os.ReadDir(p)
			var names []string
			for _, e := range entries {
				if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
					names = append(names, e.Name())
				}
			}
			sort.Strings(names)
			for _, n := range names {
				files = append(files, filepath.Join(p, n))
			}
		} else {
			files = append(files, p)
		}
	}
	return files
}

func report(counts map[string]int64, total int64) {
	fmt.Printf("reference wordcount: %s total words, %s unique words\n",
		common.Humanize(total), common.Humanize(int64(len(counts))))
	type kv struct {
		w string
		c int64
	}
	arr := make([]kv, 0, len(counts))
	for w, c := range counts {
		arr = append(arr, kv{w, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].c != arr[j].c {
			return arr[i].c > arr[j].c
		}
		return arr[i].w < arr[j].w
	})
	limit := 10
	if len(arr) < limit {
		limit = len(arr)
	}
	fmt.Println("top words:")
	for i := 0; i < limit; i++ {
		fmt.Printf("  %-16s %d\n", arr[i].w, arr[i].c)
	}
}

func verify(ref map[string]int64, refTotal int64, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL: cannot read", path, err)
		os.Exit(1)
	}
	var got map[string]int64
	if err := json.Unmarshal(data, &got); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL: invalid JSON in", path, err)
		os.Exit(1)
	}
	var bad int
	var gotTotal int64
	for w, c := range got {
		gotTotal += c
		if rc, ok := ref[w]; !ok || rc != c {
			bad++
			if bad <= 20 {
				fmt.Printf("  MISMATCH %q: got %d, expected %d\n", w, c, ref[w])
			}
		}
	}
	for w, c := range ref {
		if _, ok := got[w]; !ok {
			bad++
			if bad <= 20 {
				fmt.Printf("  MISSING  %q: expected %d\n", w, c)
			}
		}
	}
	if bad > 0 {
		fmt.Printf("FAIL: %d mismatching words between %s and the reference\n", bad, path)
		os.Exit(1)
	}
	if gotTotal != refTotal {
		fmt.Printf("FAIL: total words %d != reference %d\n", gotTotal, refTotal)
		os.Exit(1)
	}
	fmt.Printf("PASS: %s matches the sequential reference exactly (%s total words, %s unique)\n",
		path, common.Humanize(refTotal), common.Humanize(int64(len(ref))))
}
