// Package metrics holds the in-process production dashboard for cellar run
// control.
//
// The registry is deliberately lock-free on the write path: request outcomes
// and snapshot commit observations are recorded with atomics only, so metric
// collection never blocks or slows down an observation write. Reads return a
// fixed snapshot value and never mutate business records.
//
// Every series carries the interval over which it was accumulated
// (started_at/ended_at). Cumulative counters start when the process starts and
// restart at zero on a new process, which prevents double counting or rollback
// across restarts; rolling window statistics are recomputed from a bounded ring
// of recent samples, so long runs never let the numbers run backwards.
package metrics

import "time"

// IntervalView reports the accumulation window for a single series.
type IntervalView struct {
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

// CounterView is a monotonically increasing total with its accumulation
// interval. Observed is false before the first matching event.
type CounterView struct {
	Total    uint64       `json:"total"`
	Observed bool         `json:"observed"`
	Interval IntervalView `json:"interval"`
}

// GaugeView is a point-in-time value sampled from one consistent snapshot. The
// interval brackets the single sample that produced it.
type GaugeView struct {
	Name     string       `json:"name"`
	Value    int          `json:"value"`
	Interval IntervalView `json:"interval"`
}

// RequestCountersView reports process HTTP request outcomes since startup.
type RequestCountersView struct {
	Succeeded CounterView `json:"succeeded"`
	Failed    CounterView `json:"failed"`
	Total     CounterView `json:"total"`
}

// CommitEntryView is the duration of one snapshot commit attempt.
type CommitEntryView struct {
	Revision   int64     `json:"revision"`
	Success    bool      `json:"success"`
	DurationMS float64   `json:"duration_ms"`
	At         time.Time `json:"at"`
}

// CommitDurationView reports per-commit durations, the rolling window derived
// from recent commits, and cumulative attempt counts since startup.
type CommitDurationView struct {
	// Recent holds the most recent commit attempts (newest last) within the
	// bounded rolling window. Min/max/average are computed from this window, so
	// the numbers only move as new commits replace older samples.
	Recent     []CommitEntryView `json:"recent"`
	WindowSize int               `json:"window_size"`
	// WindowInterval spans the oldest through the newest sample in the rolling
	// window, and is absent before the first commit.
	WindowInterval IntervalView     `json:"window_interval"`
	MinMS          *float64         `json:"min_ms"`
	MaxMS          *float64         `json:"max_ms"`
	AverageMS      *float64         `json:"average_ms"`
	LastDuration   *CommitEntryView `json:"last"`
	Attempts       CounterView      `json:"attempts"`
	Succeeded      CounterView      `json:"succeeded"`
	Failed         CounterView      `json:"failed"`
}

// CellarView holds the business gauges sampled from one fixed snapshot.
type CellarView struct {
	FermentingRuns GaugeView   `json:"fermenting_runs"`
	VesselsByState []GaugeView `json:"vessels_by_state"`
	Revision       int64       `json:"snapshot_revision"`
}

// Snapshot is the fixed, read-only dashboard value returned for one scrape.
type Snapshot struct {
	GeneratedAt    time.Time           `json:"generated_at"`
	Process        RequestCountersView `json:"process"`
	CommitDuration CommitDurationView  `json:"snapshot_commit_duration"`
	Cellar         CellarView          `json:"cellar"`
}
