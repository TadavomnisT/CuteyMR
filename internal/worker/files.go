// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package worker

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

// The worker file server is the peer-to-peer data plane of CuteyMR:
// during the shuffle, reduce tasks fetch intermediate partition files
// directly from the map workers that produced them.

func validID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if !(b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-') {
			return false
		}
	}
	return true
}

func (w *Worker) handleRoot(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(rw, r)
		return
	}
	rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(rw, "CuteyMR worker %s — data plane at /files/map/{task}/{partition} and /files/reduce/{partition}\n", w.ID)
}

// handleMapFile serves one intermediate partition file:
// GET /files/map/{taskID}/{partition}
func (w *Worker) handleMapFile(rw http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/files/map/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || !validID(parts[0]) {
		http.Error(rw, "bad request", http.StatusBadRequest)
		return
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil || p < 0 || p > 4096 {
		http.Error(rw, "bad partition", http.StatusBadRequest)
		return
	}
	path := w.mapFilePath(parts[0], p)
	if !strings.HasPrefix(filepath.Clean(path), filepath.Clean(w.dataDir)) {
		http.Error(rw, "bad path", http.StatusBadRequest)
		return
	}
	http.ServeFile(rw, r, path)
}

// handleReduceFile serves one final reduce result:
// GET /files/reduce/{partition}
func (w *Worker) handleReduceFile(rw http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/files/reduce/")
	p, err := strconv.Atoi(rest)
	if err != nil || p < 0 || p > 4096 {
		http.Error(rw, "bad partition", http.StatusBadRequest)
		return
	}
	path := w.reduceFilePath(p)
	if !strings.HasPrefix(filepath.Clean(path), filepath.Clean(w.dataDir)) {
		http.Error(rw, "bad path", http.StatusBadRequest)
		return
	}
	http.ServeFile(rw, r, path)
}
