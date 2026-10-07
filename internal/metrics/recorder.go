package metrics

import (
	"math"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
)

// Recorder collects process metrics in memory. Recording an event is a
// constant-time counter update under a short-lived lock, so observation
// writes are never slowed by collection. Counters only move forward:
// saturating adds cannot roll over, and a restart begins a new statistics
// window whose StartedAt makes the reset visible instead of looking like
// a backwards count.
type Recorder struct {
	mu        sync.Mutex
	clock     clock.Clock
	startedAt time.Time

	requestsTotal     uint64
	requestsSucceeded uint64
	requestsFailed    uint64
	requestsLastAt    time.Time

	commitAttempts   uint64
	commitSucceeded  uint64
	commitFailed     uint64
	commitLastMicros uint64
	commitMaxMicros  uint64
	commitSumMicros  uint64
	commitLastAt     time.Time
}

func NewRecorder(timeSource clock.Clock) *Recorder {
	if timeSource == nil {
		timeSource = clock.System{}
	}
	return &Recorder{
		clock:     timeSource,
		startedAt: timeSource.Now().UTC(),
	}
}

// ObserveRequest counts one completed HTTP request. Statuses below 400
// are successes; 400 and above are failures.
func (r *Recorder) ObserveRequest(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestsTotal = saturatingAdd(r.requestsTotal, 1)
	if status >= 400 {
		r.requestsFailed = saturatingAdd(r.requestsFailed, 1)
	} else {
		r.requestsSucceeded = saturatingAdd(r.requestsSucceeded, 1)
	}
	r.requestsLastAt = r.clock.Now().UTC()
}

// ObserveCommit records exactly one snapshot commit attempt with its
// duration and outcome. It is safe to call while the storage lock is held.
func (r *Recorder) ObserveCommit(elapsed time.Duration, commitErr error) {
	if elapsed < 0 {
		elapsed = 0
	}
	micros := uint64(elapsed.Microseconds())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitAttempts = saturatingAdd(r.commitAttempts, 1)
	if commitErr != nil {
		r.commitFailed = saturatingAdd(r.commitFailed, 1)
	} else {
		r.commitSucceeded = saturatingAdd(r.commitSucceeded, 1)
	}
	r.commitLastMicros = micros
	if micros > r.commitMaxMicros {
		r.commitMaxMicros = micros
	}
	r.commitSumMicros = saturatingAdd(r.commitSumMicros, micros)
	r.commitLastAt = r.clock.Now().UTC()
}

// Dashboard assembles a fixed snapshot of every metric group. The cellar
// sample must come from a single consistent state view; this method only
// attaches windows and copies values, it never mutates business records.
func (r *Recorder) Dashboard(cellar CellarSample) Dashboard {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now().UTC()

	average := uint64(0)
	if r.commitAttempts > 0 {
		average = r.commitSumMicros / r.commitAttempts
	}
	return Dashboard{
		GeneratedAt: now,
		Requests: RequestMetrics{
			Window:    r.windowLocked(r.requestsLastAt),
			Total:     r.requestsTotal,
			Succeeded: r.requestsSucceeded,
			Failed:    r.requestsFailed,
		},
		SnapshotCommits: CommitMetrics{
			Window:              r.windowLocked(r.commitLastAt),
			Attempts:            r.commitAttempts,
			Succeeded:           r.commitSucceeded,
			Failed:              r.commitFailed,
			LastDurationMicros:  r.commitLastMicros,
			MaxDurationMicros:   r.commitMaxMicros,
			TotalDurationMicros: r.commitSumMicros,
			AvgDurationMicros:   average,
		},
		Cellar: CellarMetrics{
			Window:         Window{StartedAt: r.startedAt, EndedAt: now},
			Revision:       cellar.Revision,
			FermentingRuns: cellar.FermentingRuns,
			TotalRuns:      cellar.TotalRuns,
			RunsByState:    copyRunStates(cellar.RunsByState),
			TotalVessels:   cellar.TotalVessels,
			VesselsByState: copyVesselStates(cellar.VesselsByState),
		},
	}
}

// windowLocked bounds a counter group by the recorder start and its last
// counted event. The caller must hold the recorder lock.
func (r *Recorder) windowLocked(lastEvent time.Time) Window {
	ended := lastEvent
	if ended.IsZero() {
		ended = r.startedAt
	}
	return Window{StartedAt: r.startedAt, EndedAt: ended}
}

// saturatingAdd never wraps, so a counter cannot run backwards even after
// unrealistic uptime or event volume.
func saturatingAdd(value, delta uint64) uint64 {
	if math.MaxUint64-value < delta {
		return math.MaxUint64
	}
	return value + delta
}
