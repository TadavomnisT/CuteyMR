// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Command mr-master starts the CuteyMR master on a port and waits for
// workers. The wordcount job starts automatically once the target number of
// workers has registered; the final result is stored in the output directory.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cuteymr/internal/master"
)

func main() {
	var (
		port       = flag.Int("port", 8080, "master listen port")
		workers    = flag.Int("workers", 2, "number of workers to wait for before the job auto-starts")
		input      = flag.String("input", "data/input", "input file(s) or directory (comma separated)")
		output     = flag.String("output", "data/output", "directory for the final result files")
		chunkKB    = flag.Int64("chunk-size", 512, "map input chunk size in KiB (chunks never split words)")
		partitions = flag.Int("partitions", 0, "number of reduce partitions R (0 = auto: 2*workers, min 4)")
		hbTimeout  = flag.Duration("heartbeat-timeout", 10*time.Second, "declare a worker dead after this long without a heartbeat")
		headless   = flag.Bool("headless", false, "no terminal dashboard, log to stdout")
		exitAfter  = flag.Duration("exit-after-done", 0, "exit some duration after the job completes (e.g. 5s); 0 = stay until Ctrl+C")
	)
	flag.Parse()

	cfg := master.Config{
		Port:          *port,
		TargetWorkers: *workers,
		InputPaths:    strings.Split(*input, ","),
		OutputDir:     *output,
		ChunkSize:     *chunkKB << 10,
		NumPartitions: *partitions,
		WorkerTimeout: *hbTimeout,
		ExitAfterDone: *exitAfter,
		Headless:      *headless,
	}
	m := master.New(cfg)
	if cfg.Headless {
		m.Log().SetPrint(func(line string) { fmt.Println("master " + line) })
	}
	if err := m.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal: cannot listen on port", cfg.Port, "-", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go m.RunDashboard(stop, &wg)

	select {
	case <-sigCh:
		fmt.Println() // newline after ^C
	case <-m.Done():
		if cfg.ExitAfterDone > 0 {
			select {
			case <-time.After(cfg.ExitAfterDone):
			case <-sigCh:
			}
		} else {
			<-sigCh // stay up so the final dashboard can be admired
		}
	}
	close(stop)
	wg.Wait()
	m.Stop()
}
