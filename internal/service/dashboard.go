package service

import (
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/metrics"
)

// Vessel state gauge names exposed on the dashboard. They stay aligned with
// the domain VesselState constants but use stable dashboard identifiers.
const (
	gaugeVesselAvailable = "vessels_available"
	gaugeVesselReserved  = "vessels_reserved"
	gaugeVesselInUse     = "vessels_in_use"
	gaugeVesselCleaning  = "vessels_cleaning"
	gaugeVesselRetired   = "vessels_retired"
	gaugeFermentingRuns  = "fermenting_runs"
)

// vesselBucket fixes the order and identity of every vessel state bucket so
// all states are reported even when empty.
var vesselBucket = []struct {
	name  string
	state domain.VesselState
}{
	{gaugeVesselAvailable, domain.VesselAvailable},
	{gaugeVesselReserved, domain.VesselReserved},
	{gaugeVesselInUse, domain.VesselInUse},
	{gaugeVesselCleaning, domain.VesselCleaning},
	{gaugeVesselRetired, domain.VesselRetired},
}

// Dashboard returns a fixed snapshot of process, commit, and cellar business
// metrics. The business gauges are derived from a single cloned repository
// snapshot, so a vessel or run that changes state during sampling cannot be
// counted twice or missed: fermenting runs and every vessel state bucket are
// read from the same consistent view. The read path never calls an update, so
// scraping the dashboard cannot change business records or the revision.
func (s *Service) Dashboard() metrics.Snapshot {
	sampledAt := s.clock.Now().UTC()
	cellar := s.sampleCellar(sampledAt)
	return metrics.Snapshot{
		GeneratedAt:    sampledAt,
		Process:        s.metrics.ProcessSnapshot(),
		CommitDuration: s.metrics.CommitSnapshot(),
		Cellar:         cellar,
	}
}

func (s *Service) sampleCellar(at time.Time) metrics.CellarView {
	var revision int64
	fermenting := 0
	stateCounts := map[domain.VesselState]int{}

	// One View call clones the repository snapshot once under its read lock;
	// all counting happens against that single fixed copy.
	_ = s.store.View(func(snapshot domain.Snapshot) error {
		revision = snapshot.Revision
		for _, run := range snapshot.Runs {
			if run.State == domain.RunFermenting {
				fermenting++
			}
		}
		for _, vessel := range snapshot.Vessels {
			stateCounts[vessel.State]++
		}
		return nil
	})

	buckets := make([]metrics.GaugeView, 0, len(vesselBucket))
	for _, bucket := range vesselBucket {
		buckets = append(buckets, metrics.GaugeView{
			Name:     bucket.name,
			Value:    stateCounts[bucket.state],
			Interval: metrics.PointInterval(at),
		})
	}

	return metrics.CellarView{
		Revision: revision,
		FermentingRuns: metrics.GaugeView{
			Name:     gaugeFermentingRuns,
			Value:    fermenting,
			Interval: metrics.PointInterval(at),
		},
		VesselsByState: buckets,
	}
}
