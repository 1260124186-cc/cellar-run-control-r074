package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/metrics"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// productionDashboard drives success and failure traffic, samples the
// dashboard from fixed snapshots, verifies the counters never roll back or
// double count, and finally reopens the data directory to prove request and
// commit totals restart from zero while persisted business state survives.
func productionDashboard(api *client, dataDir string) error {
	// Baseline: no completed business activity yet. The scrape in flight is
	// counted only after its response is written, so an initial scrape sees
	// zero completed requests; every cumulative counter still carries its
	// process-start time and an empty end time.
	initial, err := readDashboard(api)
	if err != nil {
		return err
	}
	if initial.Process.Total.Total != 0 {
		return fmt.Errorf("initial scrape must not count the request still in flight, got %d",
			initial.Process.Total.Total)
	}
	// The follow-up scrape observes the completion of the first one.
	second, err := readDashboard(api)
	if err != nil {
		return err
	}
	if second.Process.Succeeded.Total != 1 || second.Process.Failed.Total != 0 ||
		second.Process.Total.Total != 1 {
		return fmt.Errorf("second scrape should observe exactly one prior success, got total %d/%d/%d",
			second.Process.Succeeded.Total, second.Process.Failed.Total, second.Process.Total.Total)
	}
	if second.Process.Total.Interval.EndedAt == nil ||
		second.Process.Total.Interval.StartedAt.IsZero() {
		return errInvalidWorkflow("request counter interval was malformed")
	}
	if initial.Cellar.FermentingRuns.Value != 0 ||
		initial.Cellar.Revision != 0 {
		return errInvalidWorkflow("empty cellar dashboard was not zero")
	}
	if err := requireGaugeStates(initial, map[string]int{
		"vessels_available": 0, "vessels_reserved": 0, "vessels_in_use": 0,
		"vessels_cleaning": 0, "vessels_retired": 0,
	}); err != nil {
		return err
	}
	if initial.CommitDuration.Attempts.Total != 0 || initial.CommitDuration.MinMS != nil {
		return errInvalidWorkflow("commit statistics were reported before any commit")
	}

	formula, err := ensureApprovedFormula(api, "Dashboard Formula")
	if err != nil {
		return err
	}
	vessel, err := createCheckVessel(api, "VSL-303", 320)
	if err != nil {
		return err
	}
	started, err := createStartedRun(api, formula.ID, vessel.ID, "RUN-DASHBOARD")
	if err != nil {
		return err
	}

	// Issue a deterministic mix: one rejected request (missing vessel on an
	// unknown run is a 404, a duplicate run code is a 409). Both must be
	// counted as failed requests but never as failed commits.
	duplicate, err := api.call("POST", "/v1/runs",
		createRunPayload("RUN-DASHBOARD", formula.ID), nil)
	if err != nil {
		return err
	}
	if err := requireCode(duplicate, 409, string(domain.CodeRunConflict),
		"reject duplicate run code"); err != nil {
		return err
	}
	missing, err := api.call("GET", "/v1/runs/run_does_not_exist", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(missing, 404, string(domain.CodeRunNotFound),
		"read missing run"); err != nil {
		return err
	}

	active, err := readDashboard(api)
	if err != nil {
		return err
	}
	// Gauges come from one fixed snapshot: exactly one fermenting run and one
	// in-use vessel, with no double counting despite rapid state transitions.
	if active.Cellar.FermentingRuns.Value != 1 {
		return fmt.Errorf("expected 1 fermenting run, got %d", active.Cellar.FermentingRuns.Value)
	}
	if err := requireGaugeStates(active, map[string]int{
		"vessels_available": 0, "vessels_reserved": 0, "vessels_in_use": 1,
		"vessels_cleaning": 0, "vessels_retired": 0,
	}); err != nil {
		return err
	}
	if active.Cellar.Revision == 0 {
		return errInvalidWorkflow("dashboard revision did not advance with writes")
	}
	// Successful writes: formula create+steps+approve, vessel, run create,
	// reserve, start = 7 durable commits. The two rejected requests add none.
	if active.CommitDuration.Attempts.Total != 7 {
		return fmt.Errorf("expected 7 commit attempts, got %d",
			active.CommitDuration.Attempts.Total)
	}
	if active.CommitDuration.Failed.Total != 0 {
		return fmt.Errorf("rejected requests must not count as failed commits, got %d",
			active.CommitDuration.Failed.Total)
	}
	if active.CommitDuration.Succeeded.Total != 7 ||
		active.CommitDuration.WindowSize != 7 || len(active.CommitDuration.Recent) != 7 {
		return errInvalidWorkflow("successful commit window did not match commits")
	}
	if active.CommitDuration.MinMS == nil || active.CommitDuration.MaxMS == nil ||
		active.CommitDuration.AverageMS == nil || active.CommitDuration.Last == nil {
		return errInvalidWorkflow("commit duration statistics were not populated")
	}
	if *active.CommitDuration.MinMS > *active.CommitDuration.MaxMS ||
		*active.CommitDuration.AverageMS < *active.CommitDuration.MinMS ||
		*active.CommitDuration.AverageMS > *active.CommitDuration.MaxMS {
		return errInvalidWorkflow("commit duration ordering was inconsistent")
	}
	if !active.CommitDuration.Last.Success ||
		active.CommitDuration.Last.Revision != active.Cellar.Revision {
		return errInvalidWorkflow("last commit did not describe the current revision")
	}
	if active.CommitDuration.WindowInterval.EndedAt == nil ||
		active.CommitDuration.WindowInterval.StartedAt.IsZero() {
		return errInvalidWorkflow("commit window interval was malformed")
	}
	// At least the two deliberate failures must be reflected, and total must be
	// internally consistent so outcomes are never lost or double counted.
	if active.Process.Failed.Total < 2 {
		return fmt.Errorf("expected at least 2 failed requests, got %d",
			active.Process.Failed.Total)
	}
	if active.Process.Total.Total !=
		active.Process.Succeeded.Total+active.Process.Failed.Total {
		return errInvalidWorkflow("request total did not equal success plus failure")
	}
	// Prior to this scrape: two baseline scrapes plus seven successful business
	// writes, with the two rejected requests contributing only failures.
	if active.Process.Succeeded.Total != 9 {
		return fmt.Errorf("expected 9 successful requests before this scrape, got %d",
			active.Process.Succeeded.Total)
	}

	// Monotonicity: counters must never move backwards and a later scrape must
	// not mutate business state (revision stays fixed across repeated reads).
	revisionBefore := active.Cellar.Revision
	commitTotal := active.CommitDuration.Attempts.Total
	successTotal := active.Process.Succeeded.Total
	for range 3 {
		later, err := readDashboard(api)
		if err != nil {
			return err
		}
		if later.Cellar.Revision != revisionBefore {
			return errInvalidWorkflow("reading the dashboard changed business records")
		}
		if later.Process.Succeeded.Total < successTotal ||
			later.CommitDuration.Attempts.Total != commitTotal {
			return errInvalidWorkflow("counters rolled backwards or commits were double counted")
		}
		successTotal = later.Process.Succeeded.Total
	}

	if err := verifyCommitRingRollover(); err != nil {
		return err
	}
	if err := verifyRestart(dataDir, started.Run.ID); err != nil {
		return err
	}
	return nil
}

func requireGaugeStates(dashboard dashboardView, expected map[string]int) error {
	if len(dashboard.Cellar.VesselsByState) != len(expected) {
		return fmt.Errorf("expected %d vessel state gauges, got %d",
			len(expected), len(dashboard.Cellar.VesselsByState))
	}
	for name, want := range expected {
		got, err := vesselGauge(dashboard, name)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("gauge %s expected %d, got %d", name, want, got)
		}
	}
	return nil
}

// verifyCommitRingRollover pushes more commits than the rolling window and
// confirms only the newest bounded samples remain while the cumulative attempt
// counter keeps increasing past the window size.
func verifyCommitRingRollover() error {
	registry := metrics.NewRegistry(func() time.Time { return time.Now().UTC() })
	const total = metrics.CommitWindow + 10
	for i := range total {
		registry.RecordCommit(metrics.CommitResult{
			Revision: int64(i + 1),
			Success:  true,
			Duration: time.Millisecond,
		})
	}
	view := registry.CommitSnapshot()
	if view.Attempts.Total != total {
		return fmt.Errorf("attempts total should be %d after rollover, got %d",
			total, view.Attempts.Total)
	}
	if view.WindowSize != metrics.CommitWindow || len(view.Recent) != metrics.CommitWindow {
		return fmt.Errorf("rolling window should hold %d samples, got %d",
			metrics.CommitWindow, view.WindowSize)
	}
	if view.Recent[0].Revision != total-metrics.CommitWindow+1 {
		return fmt.Errorf("oldest retained revision was %d, expected %d",
			view.Recent[0].Revision, total-metrics.CommitWindow+1)
	}
	if view.LastDuration == nil || view.LastDuration.Revision != total {
		return errInvalidWorkflow("newest rolling sample was not the last commit")
	}
	return nil
}

// verifyRestart reopens the same data directory in a fresh process-equivalent
// context. Persisted business state and the revision survive, while process
// counters restart at zero and no commit is replayed.
func verifyRestart(dataDir, runID string) error {
	appClock := clock.System{}
	registry := metrics.NewRegistry(appClock.Now)
	store, err := storage.Open(dataDir, storage.WithCommitObserver(func(outcome storage.CommitOutcome) {
		registry.RecordCommit(metrics.CommitResult{
			Revision: outcome.Revision,
			Success:  outcome.Success,
			Duration: outcome.Duration,
		})
	}))
	if err != nil {
		return err
	}
	app := service.New(store, appClock, registry)

	dashboard := app.Dashboard()
	if dashboard.Cellar.Revision == 0 {
		return errInvalidWorkflow("restart lost the persisted revision")
	}
	if dashboard.Process.Total.Total != 0 || dashboard.CommitDuration.Attempts.Total != 0 {
		return errInvalidWorkflow("process counters did not reset across restart")
	}
	if dashboard.Cellar.FermentingRuns.Value != 1 {
		return fmt.Errorf("restart should retain 1 fermenting run, got %d",
			dashboard.Cellar.FermentingRuns.Value)
	}
	if _, err := app.GetRun(runID); err != nil {
		return fmt.Errorf("restart lost the fermenting run: %w", err)
	}
	// One new write after restart commits at the next revision without ever
	// reusing or rolling back a prior revision.
	before := dashboard.Cellar.Revision
	vessel, err := app.CreateVessel(service.CreateVesselInput{
		Code:      "VSL-AFTER-RESTART",
		CapacityL: 100,
	})
	if err != nil {
		return err
	}
	after := app.Dashboard()
	if after.Cellar.Revision != before+1 {
		return fmt.Errorf("post-restart revision should be %d, got %d",
			before+1, after.Cellar.Revision)
	}
	if after.CommitDuration.Attempts.Total != 1 ||
		after.CommitDuration.Succeeded.Total != 1 ||
		after.CommitDuration.Failed.Total != 0 {
		return errInvalidWorkflow("restart replayed historical commits")
	}
	if after.Process.Total.Total != 0 {
		return errInvalidWorkflow("direct service sampling was counted as an HTTP request")
	}
	if _, err := app.RetireVessel(vessel.ID, service.VersionInput{ExpectedVersion: &vessel.Version}); err != nil {
		return err
	}
	return nil
}
