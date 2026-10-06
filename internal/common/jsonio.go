// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package common

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
)

// WriteGzJSON marshals v to JSON, gzips it and writes it to path (creating
// parent directories as needed). The write is atomic: data lands in a
// temporary file first and is renamed into place, so a crash mid-write can
// never leave a half-written file for peers to download. Returns the
// compressed size.
func WriteGzJSON(path string, v any) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return int64(buf.Len()), nil
}

// ReadGzJSON gunzips and unmarshals data into v.
func ReadGzJSON(data []byte, v any) error {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(zr)
	return dec.Decode(v)
}
