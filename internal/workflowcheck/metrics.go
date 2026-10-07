package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

type windowView struct {
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

type requestMetricsView struct {
	Window    windowView `json:"window"`
	Total     uint64     `json:"total"`
	Succeeded uint64     `json:"succeeded"`
	Failed    uint64     `json:"failed"`
}

type commitMetricsView struct {
	Window              windowView `json:"window"`
	Attempts            uint64     `json:"attempts"`
	Succeeded           uint64     `json:"succeeded"`
	Failed              uint64     `json:"failed"`
	LastDurationMicros  uint64     `json:"last_duration_micros"`
	MaxDurationMicros   uint64     `json:"max_duration_micros"`
	TotalDurationMicros uint64     `json:"total_duration_micros"`
	AvgDurationMicros   uint64     `json:"avg_duration_micros"`
}

type cellarMetricsView struct {
	Window         windowView     `json:"window"`
	Revision       int64          `json:"revision"`
	FermentingRuns int            `json:"fermenting_runs"`
	TotalRuns      int            `json:"total_runs"`
	RunsByState    map[string]int `json:"runs_by_state"`
	TotalVessels   int            `json:"total_vessels"`
	VesselsByState map[string]int `json:"vessels_by_state"`
}

type dashboardView struct {
	GeneratedAt     time.Time          `json:"generated_at"`
	Requests        requestMetricsView `json:"requests"`
	SnapshotCommits commitMetricsView  `json:"snapshot_commits"`
	Cellar          cellarMetricsView  `json:"cellar"`
}

func metricsDashboard(api *client) error {
	formula, err := ensureApprovedFormula(api, "Metrics Formula")
	if err != nil {
		return err
	}
	vessel, err := createCheckVessel(api, "VSL-301", 320)
	if err != nil {
		return err
	}
	started, err := createStartedRun(api, formula.ID, vessel.ID, "RUN-METRICS")
	if err != nil {
		return err
	}

	base := time.Now().UTC().Add(10 * time.Second)
	observationResponse, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations",
		map[string]any{
			"sequence":      1,
			"gravity":       1.050,
			"temperature_c": 20.0,
			"observed_at":   verifiedAt(base, 1),
			"note":          "metrics observation",
		}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(observationResponse, 200, "append metrics observation"); err != nil {
		return err
	}

	rejected, err := api.call("POST", "/v1/runs/"+started.Run.ID+"/observations",
		map[string]any{
			"sequence":      2,
			"gravity":       1.4,
			"temperature_c": 20,
			"observed_at":   verifiedAt(base, 2),
			"note":          "out of range",
		}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(rejected, 400, string(domain.CodeInvalidInput),
		"reject out-of-range observation"); err != nil {
		return err
	}

	revisionBefore, err := readRevision(api)
	if err != nil {
		return err
	}
	dashboard, err := readDashboard(api)
	if err != nil {
		return err
	}
	revisionAfter, err := readRevision(api)
	if err != nil {
		return err
	}
	if revisionAfter != revisionBefore {
		return errInvalidWorkflow("dashboard read changed the snapshot revision")
	}
	if dashboard.Cellar.Revision != revisionBefore {
		return errInvalidWorkflow("cellar metrics do not name the sampled revision")
	}

	if err := requireCellarMetrics(dashboard.Cellar); err != nil {
		return err
	}
	if err := requireRequestMetrics(dashboard.Requests); err != nil {
		return err
	}
	if err := requireCommitMetrics(dashboard.SnapshotCommits); err != nil {
		return err
	}
	if dashboard.GeneratedAt.IsZero() {
		return errInvalidWorkflow("dashboard was not stamped with a generation time")
	}
	if err := requireWindow(dashboard.Requests.Window, dashboard.GeneratedAt, "requests"); err != nil {
		return err
	}
	if err := requireWindow(dashboard.SnapshotCommits.Window, dashboard.GeneratedAt,
		"snapshot commits"); err != nil {
		return err
	}
	if err := requireWindow(dashboard.Cellar.Window, dashboard.GeneratedAt, "cellar"); err != nil {
		return err
	}

	runBefore, err := readRun(api, started.Run.ID)
	if err != nil {
		return err
	}
	second, err := readDashboard(api)
	if err != nil {
		return err
	}
	runAfter, err := readRun(api, started.Run.ID)
	if err != nil {
		return err
	}
	if runAfter.Version != runBefore.Version {
		return errInvalidWorkflow("dashboard read changed the run record")
	}
	if second.Requests.Total < dashboard.Requests.Total ||
		second.SnapshotCommits.Attempts < dashboard.SnapshotCommits.Attempts {
		return errInvalidWorkflow("metrics counters moved backwards between reads")
	}
	if second.GeneratedAt.Before(dashboard.GeneratedAt) {
		return errInvalidWorkflow("dashboard generation time moved backwards")
	}
	if second.Cellar.Revision != dashboard.Cellar.Revision {
		return errInvalidWorkflow("dashboard read changed the sampled revision")
	}
	return nil
}

func readDashboard(api *client) (dashboardView, error) {
	response, err := api.call("GET", "/v1/metrics/dashboard", nil, nil)
	if err != nil {
		return dashboardView{}, err
	}
	if err := requireStatus(response, 200, "read metrics dashboard"); err != nil {
		return dashboardView{}, err
	}
	var dashboard dashboardView
	if err := response.decode(&dashboard); err != nil {
		return dashboardView{}, err
	}
	return dashboard, nil
}

func readRevision(api *client) (int64, error) {
	response, err := api.call("GET", "/healthz", nil, nil)
	if err != nil {
		return 0, err
	}
	if err := requireStatus(response, 200, "read health"); err != nil {
		return 0, err
	}
	var health struct {
		Revision int64 `json:"revision"`
	}
	if err := response.decode(&health); err != nil {
		return 0, err
	}
	return health.Revision, nil
}

func readRun(api *client, id string) (runView, error) {
	response, err := api.call("GET", "/v1/runs/"+id, nil, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, 200, "read run"); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func requireCellarMetrics(cellar cellarMetricsView) error {
	if cellar.FermentingRuns != 1 {
		return fmt.Errorf("expected 1 fermenting run, got %d", cellar.FermentingRuns)
	}
	if cellar.VesselsByState[string(domain.VesselInUse)] != 1 {
		return errInvalidWorkflow("expected exactly one in-use vessel")
	}
	if err := requireStates(cellar.RunsByState, "planned", "reserved", "fermenting",
		"conditioning", "completed", "aborted"); err != nil {
		return err
	}
	if err := requireStates(cellar.VesselsByState, "available", "reserved", "in_use",
		"cleaning", "retired"); err != nil {
		return err
	}
	if sumStates(cellar.RunsByState) != cellar.TotalRuns {
		return errInvalidWorkflow("run state counts do not sum to the run total")
	}
	if sumStates(cellar.VesselsByState) != cellar.TotalVessels {
		return errInvalidWorkflow("vessel state counts do not sum to the vessel total")
	}
	if cellar.FermentingRuns != cellar.RunsByState[string(domain.RunFermenting)] {
		return errInvalidWorkflow("fermenting run count disagrees with the state breakdown")
	}
	return nil
}

func requireRequestMetrics(requests requestMetricsView) error {
	if requests.Total != requests.Succeeded+requests.Failed {
		return errInvalidWorkflow("request outcomes do not sum to the request total")
	}
	if requests.Succeeded < 1 {
		return errInvalidWorkflow("successful requests were not counted")
	}
	if requests.Failed < 1 {
		return errInvalidWorkflow("the rejected request was not counted as a failure")
	}
	return nil
}

func requireCommitMetrics(commits commitMetricsView) error {
	if commits.Attempts != commits.Succeeded+commits.Failed {
		return errInvalidWorkflow("commit outcomes do not sum to the commit attempts")
	}
	if commits.Attempts < 1 {
		return errInvalidWorkflow("snapshot commits were not counted")
	}
	if commits.Failed != 0 {
		return errInvalidWorkflow("unexpected snapshot commit failure")
	}
	if commits.MaxDurationMicros < commits.LastDurationMicros {
		return errInvalidWorkflow("max commit duration is below the last commit duration")
	}
	if commits.TotalDurationMicros < commits.MaxDurationMicros {
		return errInvalidWorkflow("total commit duration is below the max commit duration")
	}
	if commits.AvgDurationMicros != commits.TotalDurationMicros/commits.Attempts {
		return errInvalidWorkflow("average commit duration is inconsistent with the totals")
	}
	return nil
}

func requireWindow(window windowView, generatedAt time.Time, label string) error {
	if window.StartedAt.IsZero() {
		return fmt.Errorf("%s: statistics window has no start time", label)
	}
	if window.EndedAt.Before(window.StartedAt) {
		return fmt.Errorf("%s: statistics window ends before it starts", label)
	}
	if window.EndedAt.After(generatedAt) {
		return fmt.Errorf("%s: statistics window ends after the read was assembled", label)
	}
	return nil
}

func requireStates(counts map[string]int, states ...string) error {
	for _, state := range states {
		if _, found := counts[state]; !found {
			return fmt.Errorf("state %q is missing from the metrics breakdown", state)
		}
	}
	return nil
}

func sumStates(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}
