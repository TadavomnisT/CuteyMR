// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Package common defines the wire protocol and helpers shared by the
// CuteyMR master and workers.
package common

import "time"

// Protocol / cluster behaviour defaults. Both heartbeat timings are
// configurable per process (worker -heartbeat, master -heartbeat-timeout)
// so clusters on slow or wide-area networks can relax them.
const (
	// DefaultHeartbeatInterval is how often a worker tells the master it
	// is alive by default.
	DefaultHeartbeatInterval = 1 * time.Second
	// DefaultWorkerTimeout is how long the master waits without a
	// heartbeat before declaring a worker dead and re-queuing its tasks.
	DefaultWorkerTimeout = 10 * time.Second
	// MaxTaskFailures is the number of failures tolerated per task before
	// the whole job is declared failed.
	MaxTaskFailures = 3
)

// Task types.
const (
	TaskMap      = "map"
	TaskReduce   = "reduce"
	TaskNone     = "none"     // no work available right now
	TaskShutdown = "shutdown" // job finished (or failed) - worker may exit
)

// Phase is the coarse job phase shown on dashboards.
type Phase string

const (
	PhaseWaiting Phase = "waiting" // waiting for the target number of workers
	PhaseMap     Phase = "map"     // map tasks being processed
	PhaseShuffle Phase = "shuffle" // reduce workers are pulling intermediate files
	PhaseReduce  Phase = "reduce"  // reduce tasks are aggregating
	PhaseMerge   Phase = "merge"   // master is downloading + merging results
	PhaseDone    Phase = "done"
	PhaseFailed  Phase = "failed"
)

// TaskState is the lifecycle state of a single task on the master.
type TaskState string

const (
	TaskIdle       TaskState = "idle"
	TaskInProgress TaskState = "in_progress"
	TaskCompleted  TaskState = "completed"
)

// RegisterRequest is sent by a worker right after connecting so the master
// learns about the worker's resources and its peer-to-peer file address.
type RegisterRequest struct {
	WorkerID string `json:"worker_id"`
	Host     string `json:"host"`      // advertised host for the file server
	FilePort int    `json:"file_port"` // port of the worker file server
	Cores    int    `json:"cores"`     // logical CPU cores
	MemoryMB int64  `json:"memory_mb"` // total system memory
	Hostname string `json:"hostname"`  // machine hostname
}

// RegisterResponse acknowledges a registration.
type RegisterResponse struct {
	Accepted      bool   `json:"accepted"`
	Message       string `json:"message"`
	TargetWorkers int    `json:"target_workers"`
	AliveWorkers  int    `json:"alive_workers"`
	NumPartitions int    `json:"num_partitions"` // 0 = not known yet (job not started)
	Phase         Phase  `json:"phase"`
}

// ActiveTaskInfo describes one task currently running inside a worker slot.
// It is carried inside heartbeats so the master can draw live progress.
type ActiveTaskInfo struct {
	TaskID     string  `json:"task_id"`
	Type       string  `json:"type"`
	Progress   float64 `json:"progress"`    // 0..1
	Detail     string  `json:"detail"`      // e.g. "scan 62%" or "fetch 4/12"
	Words      int64   `json:"words"`       // words processed so far
	FetchDone  int     `json:"fetch_done"`  // reduce: sources fetched
	FetchTotal int     `json:"fetch_total"` // reduce: total sources
	FetchBytes int64   `json:"fetch_bytes"` // reduce: bytes fetched so far
}

// HeartbeatRequest is the periodic "I am alive" message of a worker,
// enriched with live statistics used by the master dashboard.
type HeartbeatRequest struct {
	WorkerID        string           `json:"worker_id"`
	TasksCompleted  int              `json:"tasks_completed"`
	TasksMapDone    int              `json:"tasks_map_done"`
	TasksReduceDone int              `json:"tasks_reduce_done"`
	WordsEmitted    int64            `json:"words_emitted"` // map side: tokens seen
	WordsMerged     int64            `json:"words_merged"`  // reduce side: pairs summed
	BytesRead       int64            `json:"bytes_read"`
	BytesWritten    int64            `json:"bytes_written"`
	Active          []ActiveTaskInfo `json:"active"`
}

// HeartbeatResponse tells the worker about the cluster state.
type HeartbeatResponse struct {
	OK            bool    `json:"ok"`
	Message       string  `json:"message"`
	Phase         Phase   `json:"phase"`
	Shutdown      bool    `json:"shutdown"`
	AliveWorkers  int     `json:"alive_workers"`
	TargetWorkers int     `json:"target_workers"`
	ClusterWords  int64   `json:"cluster_words"`
	ClusterRate   float64 `json:"cluster_rate"`
}

// FileSource points at one intermediate partition file living on a
// peer worker. Reduce workers fetch these directly (peer-to-peer shuffle).
type FileSource struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	URL      string `json:"url"`
	Bytes    int64  `json:"bytes"`
}

// TaskAssignment is handed to a worker when it asks for work.
// InputURL is a path relative to the master; Source URLs are absolute.
type TaskAssignment struct {
	Type   string `json:"type"`
	TaskID string `json:"task_id"`
	// map fields
	InputURL      string `json:"input_url,omitempty"` // path on master
	InputBytes    int64  `json:"input_bytes,omitempty"`
	SourceFile    string `json:"source_file,omitempty"`
	NumPartitions int    `json:"num_partitions,omitempty"` // R
	// reduce fields
	Partition int          `json:"partition,omitempty"`
	Sources   []FileSource `json:"sources,omitempty"`
}

// TaskStats summarizes one executed task.
type TaskStats struct {
	DurationMS   int64 `json:"duration_ms"`
	Words        int64 `json:"words"`
	UniqueWords  int64 `json:"unique_words"`
	BytesRead    int64 `json:"bytes_read"`
	BytesWritten int64 `json:"bytes_written"`
}

// TaskDoneRequest reports a finished (or failed) task to the master.
type TaskDoneRequest struct {
	WorkerID       string    `json:"worker_id"`
	TaskID         string    `json:"task_id"`
	Type           string    `json:"type"`
	Success        bool      `json:"success"`
	Error          string    `json:"error,omitempty"`
	FailedSource   string    `json:"failed_source,omitempty"` // reduce: worker whose file could not be fetched
	Stats          TaskStats `json:"stats"`
	PartitionSizes []int64   `json:"partition_sizes,omitempty"` // map: size per partition
	ResultURL      string    `json:"result_url,omitempty"`      // reduce: where to fetch result
}

// TaskDoneResponse acknowledges a task report.
type TaskDoneResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}
