package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

type formulaView struct {
	ID      string               `json:"id"`
	State   domain.FormulaState  `json:"state"`
	Version int64                `json:"version"`
	Steps   []domain.FormulaStep `json:"steps"`
}

type vesselView struct {
	ID          string             `json:"id"`
	State       domain.VesselState `json:"state"`
	Version     int64              `json:"version"`
	ActiveRunID *string            `json:"active_run_id"`
}

type runView struct {
	ID           string                      `json:"id"`
	State        domain.RunState             `json:"state"`
	Version      int64                       `json:"version"`
	VesselID     *string                     `json:"vessel_id"`
	Observations []domain.GravityObservation `json:"observations"`
	Metrics      *domain.RunMetrics          `json:"metrics"`
}

type pairView struct {
	Run    runView    `json:"run"`
	Vessel vesselView `json:"vessel"`
}

type dashboardInterval struct {
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

type dashboardCounter struct {
	Total    uint64            `json:"total"`
	Observed bool              `json:"observed"`
	Interval dashboardInterval `json:"interval"`
}

type dashboardGauge struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type dashboardCommitEntry struct {
	Revision   int64   `json:"revision"`
	Success    bool    `json:"success"`
	DurationMS float64 `json:"duration_ms"`
}

type dashboardCommit struct {
	Recent         []dashboardCommitEntry `json:"recent"`
	WindowSize     int                    `json:"window_size"`
	WindowInterval dashboardInterval      `json:"window_interval"`
	MinMS          *float64               `json:"min_ms"`
	MaxMS          *float64               `json:"max_ms"`
	AverageMS      *float64               `json:"average_ms"`
	Last           *dashboardCommitEntry  `json:"last"`
	Attempts       dashboardCounter       `json:"attempts"`
	Succeeded      dashboardCounter       `json:"succeeded"`
	Failed         dashboardCounter       `json:"failed"`
}

type dashboardView struct {
	GeneratedAt time.Time `json:"generated_at"`
	Process     struct {
		Succeeded dashboardCounter `json:"succeeded"`
		Failed    dashboardCounter `json:"failed"`
		Total     dashboardCounter `json:"total"`
	} `json:"process"`
	CommitDuration dashboardCommit `json:"snapshot_commit_duration"`
	Cellar         struct {
		Revision       int64            `json:"snapshot_revision"`
		FermentingRuns dashboardGauge   `json:"fermenting_runs"`
		VesselsByState []dashboardGauge `json:"vessels_by_state"`
	} `json:"cellar"`
}

func readDashboard(api *client) (dashboardView, error) {
	response, err := api.call("GET", "/v1/metrics", nil, nil)
	if err != nil {
		return dashboardView{}, err
	}
	if err := requireStatus(response, 200, "read dashboard"); err != nil {
		return dashboardView{}, err
	}
	var dashboard dashboardView
	if err := response.decode(&dashboard); err != nil {
		return dashboardView{}, err
	}
	return dashboard, nil
}

func vesselGauge(dashboard dashboardView, name string) (int, error) {
	for _, gauge := range dashboard.Cellar.VesselsByState {
		if gauge.Name == name {
			return gauge.Value, nil
		}
	}
	return 0, fmt.Errorf("dashboard is missing vessel gauge %q", name)
}

func createFormulaPayload(name string) map[string]any {
	return map[string]any{
		"name":                    name,
		"style":                   "Cellar pilot",
		"target_original_gravity": 1.052,
		"target_final_gravity":    1.012,
		"max_fermentation_temp_c": 22.0,
		"minimum_days":            7,
		"maximum_days":            21,
	}
}

func formulaStepPayload() map[string]any {
	return map[string]any{
		"steps": []map[string]any{
			{
				"sequence":      1,
				"label":         "Heat and combine",
				"temperature_c": 66.0,
				"hold_minutes":  60,
			},
			{
				"sequence":      2,
				"label":         "Transfer and cool",
				"temperature_c": 20.0,
				"hold_minutes":  30,
			},
			{
				"sequence":      3,
				"label":         "Hold for primary activity",
				"temperature_c": 19.0,
				"hold_minutes":  240,
			},
		},
	}
}

func createRunPayload(code, formulaID string) map[string]any {
	return map[string]any{
		"code":                  code,
		"formula_id":            formulaID,
		"planned_volume_liters": 180.0,
		"notes":                 "pilot run",
	}
}

func verifiedAt(base time.Time, sequence int) time.Time {
	return base.Add(time.Duration(sequence) * 10 * time.Second)
}

func requireRunState(run runView, state domain.RunState) error {
	if run.State != state {
		return fmt.Errorf("expected run state %q, got %q", state, run.State)
	}
	return nil
}

func requireVesselState(vessel vesselView, state domain.VesselState) error {
	if vessel.State != state {
		return fmt.Errorf("expected vessel state %q, got %q", state, vessel.State)
	}
	return nil
}
