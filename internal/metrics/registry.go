package metrics

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// CommitWindow is the bounded number of recent commit attempts retained for
// rolling duration statistics. Once the window fills, each new commit replaces
// the oldest sample, so a long-running process never grows without bound and a
// statistic can never be dragged backwards by stale history.
const CommitWindow = 64

// CommitResult describes one snapshot commit attempt after persistence has
// either succeeded or failed. Validation rejections that never reach the
// storage layer are not reported.
type CommitResult struct {
	Revision int64
	Success  bool
	Duration time.Duration
}

// commitSample is one immutable ring entry. Seq is the zero-based global
// commit sequence, which lets a reader restore ordering even though the ring
// slots are overwritten in place.
type commitSample struct {
	seq        uint64
	revision   int64
	success    bool
	durationMS float64
	at         time.Time
}

// counters is an immutable bundle of related totals. The whole bundle is
// swapped atomically, so a snapshot reader always sees a self-consistent set
// (total always equals success plus failure) instead of tearing across
// separate increments.
type counters struct {
	total     uint64
	succeeded uint64
	failed    uint64
}

// Registry collects process and commit metrics. All writes use atomics and
// never take a mutex, so recording an observation cannot block business
// writes. Reads assemble a fixed value snapshot and mutate nothing.
type Registry struct {
	startedAt time.Time
	now       func() time.Time

	requests          atomic.Pointer[counters]
	lastRequestOKAt   atomic.Pointer[time.Time]
	lastRequestFailAt atomic.Pointer[time.Time]

	commits        atomic.Pointer[counters]
	lastAttemptAt  atomic.Pointer[time.Time]
	lastSuccessAt  atomic.Pointer[time.Time]
	lastFailureAt  atomic.Pointer[time.Time]
	commitWriteSeq atomic.Uint64
	commitRing     [CommitWindow]atomic.Pointer[commitSample]
}

// NewRegistry builds a registry. The accumulation window for every cumulative
// counter begins at construction time, which is the process start. A nil clock
// falls back to wall-clock UTC.
func NewRegistry(now func() time.Time) *Registry {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	registry := &Registry{
		startedAt: now().UTC(),
		now:       now,
	}
	registry.requests.Store(&counters{})
	registry.commits.Store(&counters{})
	return registry
}

// StartedAt reports when cumulative counters began accumulating.
func (r *Registry) StartedAt() time.Time {
	return r.startedAt
}

// RecordRequest counts one served HTTP request. A status below 400 is a
// success; 400 and above is a failure.
func (r *Registry) RecordRequest(success bool) {
	stamp := r.now().UTC()
	r.bump(&r.requests, success)
	if success {
		r.lastRequestOKAt.Store(&stamp)
	} else {
		r.lastRequestFailAt.Store(&stamp)
	}
}

// RecordCommit records one finished snapshot commit attempt and appends its
// duration to the bounded rolling ring. Counters advance once per real attempt
// and are never decremented, so a failed commit cannot make a total go
// backwards and a retried commit is counted as its own attempt rather than
// double counted as a success correction.
func (r *Registry) RecordCommit(result CommitResult) {
	stamp := r.now().UTC()
	seq := r.commitWriteSeq.Add(1) - 1
	r.bump(&r.commits, result.Success)
	r.lastAttemptAt.Store(&stamp)
	if result.Success {
		r.lastSuccessAt.Store(&stamp)
	} else {
		r.lastFailureAt.Store(&stamp)
	}
	sample := &commitSample{
		seq:        seq,
		revision:   result.Revision,
		success:    result.Success,
		durationMS: durationMillis(result.Duration),
		at:         stamp,
	}
	r.commitRing[seq%CommitWindow].Store(sample)
}

// bump replaces the immutable counter bundle with a single atomic swap,
// preserving the total == succeeded + failed invariant for readers.
func (r *Registry) bump(slot *atomic.Pointer[counters], success bool) {
	for {
		current := slot.Load()
		next := &counters{
			total:     current.total + 1,
			succeeded: current.succeeded,
			failed:    current.failed,
		}
		if success {
			next.succeeded++
		} else {
			next.failed++
		}
		if slot.CompareAndSwap(current, next) {
			return
		}
	}
}

// ProcessSnapshot returns a fixed snapshot of request outcome counters.
func (r *Registry) ProcessSnapshot() RequestCountersView {
	values := r.requests.Load()
	succeeded := r.counterView(values.succeeded > 0, values.succeeded, r.lastRequestOKAt.Load())
	failed := r.counterView(values.failed > 0, values.failed, r.lastRequestFailAt.Load())
	total := CounterView{
		Total:    values.total,
		Observed: values.total > 0,
		Interval: IntervalView{StartedAt: r.startedAt},
	}
	total.Interval.EndedAt = laterPointer(succeeded.Interval.EndedAt, failed.Interval.EndedAt)
	return RequestCountersView{Succeeded: succeeded, Failed: failed, Total: total}
}

// CommitSnapshot returns a fixed snapshot of per-commit durations, rolling
// window statistics, and cumulative attempt totals.
func (r *Registry) CommitSnapshot() CommitDurationView {
	samples := make([]commitSample, 0, CommitWindow)
	for slot := range r.commitRing {
		if sample := r.commitRing[slot].Load(); sample != nil {
			samples = append(samples, *sample)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].seq < samples[j].seq })
	if len(samples) > CommitWindow {
		samples = samples[len(samples)-CommitWindow:]
	}

	values := r.commits.Load()
	view := CommitDurationView{
		Recent:     []CommitEntryView{},
		WindowSize: len(samples),
		Attempts:   r.counterView(values.total > 0, values.total, r.lastAttemptAt.Load()),
		Succeeded:  r.counterView(values.succeeded > 0, values.succeeded, r.lastSuccessAt.Load()),
		Failed:     r.counterView(values.failed > 0, values.failed, r.lastFailureAt.Load()),
	}
	if len(samples) == 0 {
		return view
	}

	var minimum, maximum, sum float64
	for index, sample := range samples {
		if index == 0 {
			minimum, maximum = sample.durationMS, sample.durationMS
		} else {
			minimum = math.Min(minimum, sample.durationMS)
			maximum = math.Max(maximum, sample.durationMS)
		}
		sum += sample.durationMS
		entry := CommitEntryView{
			Revision:   sample.revision,
			Success:    sample.success,
			DurationMS: sample.durationMS,
			At:         sample.at,
		}
		view.Recent = append(view.Recent, entry)
	}
	average := roundMillis(sum / float64(len(samples)))
	minimum = roundMillis(minimum)
	maximum = roundMillis(maximum)
	view.MinMS = &minimum
	view.MaxMS = &maximum
	view.AverageMS = &average

	oldest := samples[0].at
	newest := samples[len(samples)-1].at
	view.WindowInterval = IntervalView{StartedAt: oldest, EndedAt: &newest}
	last := view.Recent[len(view.Recent)-1]
	view.LastDuration = &last
	return view
}

func (r *Registry) counterView(observed bool, total uint64, lastAt *time.Time) CounterView {
	view := CounterView{
		Total:    total,
		Observed: observed,
		Interval: IntervalView{StartedAt: r.startedAt},
	}
	if lastAt != nil {
		end := *lastAt
		view.Interval.EndedAt = &end
	}
	return view
}

// PointInterval builds the degenerate interval for a gauge sampled at a single
// instant: the sample starts and ends at the same timestamp.
func PointInterval(at time.Time) IntervalView {
	stamp := at.UTC()
	return IntervalView{StartedAt: stamp, EndedAt: &stamp}
}

func durationMillis(duration time.Duration) float64 {
	return roundMillis(float64(duration.Nanoseconds()) / 1e6)
}

func roundMillis(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func laterPointer(a, b *time.Time) *time.Time {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case b.After(*a):
		return b
	default:
		return a
	}
}
