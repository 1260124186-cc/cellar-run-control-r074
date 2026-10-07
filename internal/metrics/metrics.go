// Package metrics collects process and cellar business metrics for the
// local production dashboard. Counters are monotonic within a statistics
// window, and every metric group reports the window it covers.
package metrics

import (
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// Window bounds the statistics period of one metric group. StartedAt is
// when collection began; EndedAt is when the last counted event happened
// (or StartedAt when nothing has been recorded yet).
type Window struct {
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

// RequestMetrics counts completed HTTP requests by outcome. Total always
// equals Succeeded plus Failed.
type RequestMetrics struct {
	Window    Window `json:"window"`
	Total     uint64 `json:"total"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`
}

// CommitMetrics describes snapshot commit attempts. Every attempt adds
// exactly one duration sample and increments exactly one of Succeeded or
// Failed, so Attempts always equals Succeeded plus Failed.
type CommitMetrics struct {
	Window              Window `json:"window"`
	Attempts            uint64 `json:"attempts"`
	Succeeded           uint64 `json:"succeeded"`
	Failed              uint64 `json:"failed"`
	LastDurationMicros  uint64 `json:"last_duration_micros"`
	MaxDurationMicros   uint64 `json:"max_duration_micros"`
	TotalDurationMicros uint64 `json:"total_duration_micros"`
	AvgDurationMicros   uint64 `json:"avg_duration_micros"`
}

// CellarMetrics is a consistent sample of the business state taken from a
// single snapshot revision. The per-state maps always contain every known
// state, and their values sum to the matching total.
type CellarMetrics struct {
	Window         Window                     `json:"window"`
	Revision       int64                      `json:"revision"`
	FermentingRuns int                        `json:"fermenting_runs"`
	TotalRuns      int                        `json:"total_runs"`
	RunsByState    map[domain.RunState]int    `json:"runs_by_state"`
	TotalVessels   int                        `json:"total_vessels"`
	VesselsByState map[domain.VesselState]int `json:"vessels_by_state"`
}

// Dashboard is a fixed point-in-time view of every metric group. Each
// group is internally consistent; GeneratedAt is the moment the view was
// assembled.
type Dashboard struct {
	GeneratedAt     time.Time      `json:"generated_at"`
	Requests        RequestMetrics `json:"requests"`
	SnapshotCommits CommitMetrics  `json:"snapshot_commits"`
	Cellar          CellarMetrics  `json:"cellar"`
}
