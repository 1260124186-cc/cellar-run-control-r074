package service

import (
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/metrics"
)

// Metrics returns the recorder that collects process and cellar metrics
// for this service.
func (s *Service) Metrics() *metrics.Recorder {
	return s.metrics
}

// Dashboard assembles a fixed point-in-time metrics view. The cellar
// gauges are sampled from one consistent snapshot clone, so vessel or run
// state changes during the read cannot tear or double-count the per-state
// totals. The read never mutates business records: it takes no write lock,
// bumps no revision, and commits no snapshot.
func (s *Service) Dashboard() (metrics.Dashboard, error) {
	var sample metrics.CellarSample
	err := s.store.View(func(snapshot domain.Snapshot) error {
		sample = metrics.SampleCellar(snapshot)
		return nil
	})
	if err != nil {
		return metrics.Dashboard{}, err
	}
	return s.metrics.Dashboard(sample), nil
}
