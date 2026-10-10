// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

package common

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// ErrNoContent is returned by client helpers when the server answers 204.
var ErrNoContent = errors.New("no content")

// HTTPClient is the shared client used by master and workers.
//
// The tuned Transport matters more than it looks: Go's default transport
// keeps only 2 idle connections per host, so a large shuffle (tens of
// thousands of peer-to-peer partition fetches) churns through TCP
// connections — each closed socket lingers in TIME_WAIT for ~60 s and the
// ephemeral port range can run dry, after which dials start failing and even
// heartbeats time out. A real per-host idle pool lets the shuffle reuse
// connections instead of re-dialing for every file.
var HTTPClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

func backoff(attempt int) time.Duration {
	d := time.Duration(150*(attempt+1)) * time.Millisecond
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// WriteJSON writes a JSON HTTP response.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ReadJSON decodes a JSON request body (bounded to 64 MiB).
func ReadJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<20))
	return dec.Decode(v)
}

// doWithRetriesCtx performs an HTTP request with retries, carrying ctx so
// in-flight requests unblock immediately on cancellation and backoff sleeps
// abort early (same shutdown semantics as GetBytesCtx).
func doWithRetriesCtx(ctx context.Context, method, url string, body []byte, out any, attempts int) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var req *http.Request
		var err error
		if body == nil {
			req, err = http.NewRequestWithContext(ctx, method, url, nil)
		} else {
			req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		}
		if err != nil {
			return err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			if !sleepCtx(ctx, backoff(i)) {
				return ctx.Err()
			}
			continue
		}
		if resp.StatusCode == http.StatusNoContent {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return ErrNoContent
		}
		if resp.StatusCode >= 500 {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("server error %d from %s: %s", resp.StatusCode, url, bytes.TrimSpace(data))
			if !sleepCtx(ctx, backoff(i)) {
				return ctx.Err()
			}
			continue
		}
		if resp.StatusCode >= 400 {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return fmt.Errorf("client error %d from %s: %s", resp.StatusCode, url, bytes.TrimSpace(data))
		}
		if out == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil
		}
		err = json.NewDecoder(resp.Body).Decode(out)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("decode response from %s: %w", url, err)
			if !sleepCtx(ctx, backoff(i)) {
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	return lastErr
}

// PostJSON posts body as JSON and decodes the response into out (may be nil).
func PostJSON(url string, body any, out any) error {
	return PostJSONCtx(context.Background(), url, body, out)
}

// PostJSONCtx is PostJSON with cancellation.
func PostJSONCtx(ctx context.Context, url string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return doWithRetriesCtx(ctx, http.MethodPost, url, b, out, 4)
}

// GetJSON performs GET and decodes the JSON response into out.
func GetJSON(url string, out any) error {
	return GetJSONCtx(context.Background(), url, out)
}

// GetJSONCtx is GetJSON with cancellation.
func GetJSONCtx(ctx context.Context, url string, out any) error {
	return doWithRetriesCtx(ctx, http.MethodGet, url, nil, out, 4)
}

// GetBytes downloads a URL (with retries) and returns the raw payload.
func GetBytes(url string) ([]byte, error) {
	return GetBytesCtx(context.Background(), url)
}

// GetBytesCtx is GetBytes with cancellation: retries and backoff sleeps
// abort as soon as ctx is done, and in-flight requests carry the context so
// they unblock immediately on shutdown instead of waiting for the 60s
// client timeout.
func GetBytesCtx(ctx context.Context, url string) ([]byte, error) {
	var lastErr error
	for i := 0; i < 4; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			if !sleepCtx(ctx, backoff(i)) {
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("GET %s -> %s", url, resp.Status)
			if !sleepCtx(ctx, backoff(i)) {
				return nil, ctx.Err()
			}
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			if !sleepCtx(ctx, backoff(i)) {
				return nil, ctx.Err()
			}
			continue
		}
		return data, nil
	}
	return nil, lastErr
}

// sleepCtx waits for d, returning false if ctx was canceled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
