// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Command mr-worker starts one CuteyMR worker which connects to a master,
// reports its resources (CPU cores / RAM) and then runs map and reduce tasks
// on one goroutine per CPU core until the job is finished.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"cuteymr/internal/worker"
)

func main() {
	var (
		masterURL = flag.String("master", "http://127.0.0.1:8080", "master address (host or URL)")
		filePort  = flag.Int("file-port", 0, "port for this worker's peer-to-peer file server (0 = pick a free port)")
		advertise = flag.String("advertise", "", "host/IP other machines should use to reach this worker's file server (default: auto)")
		dataDir   = flag.String("data-dir", "", "scratch directory for intermediate files (default: a fresh temp dir)")
		heartbeat = flag.Duration("heartbeat", 1*time.Second, "how often to tell the master we are alive (raise on slow/WAN links)")
		headless  = flag.Bool("headless", false, "no terminal dashboard, log to stdout")
		slowRed   = flag.Duration("slow-reduce", 0, "testing aid: hold every reduce task in-progress for this long (e.g. 900ms) — widens the kill-during-REDUCE window for fault-injection tests")
	)
	flag.Parse()

	w, err := worker.New(worker.Config{
		MasterURL:         *masterURL,
		FilePort:          *filePort,
		Advertise:         *advertise,
		DataDir:           *dataDir,
		HeartbeatInterval: *heartbeat,
		Headless:          *headless,
		SlowReduce:        *slowRed,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
	if *headless {
		w.Log().SetPrint(func(line string) { fmt.Println("worker " + line) })
	}
	if err := w.StartFileServer(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal: file server:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Register(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "fatal: registration:", err)
		os.Exit(1)
	}
	w.Run()
	fmt.Println(w.Summary())
}
