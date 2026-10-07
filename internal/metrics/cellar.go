package metrics

import "github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"

// CellarSample is a consistent point-in-time count of the business state.
// It must be built from one snapshot clone so state changes that happen
// while sampling cannot tear or double-count the per-state totals.
type CellarSample struct {
	Revision       int64
	FermentingRuns int
	TotalRuns      int
	RunsByState    map[domain.RunState]int
	TotalVessels   int
	VesselsByState map[domain.VesselState]int
}

// SampleCellar counts runs and vessels by state from one snapshot. Every
// known state is present in the result maps, and the per-state values
// always sum to the matching total.
func SampleCellar(snapshot domain.Snapshot) CellarSample {
	sample := CellarSample{
		Revision: snapshot.Revision,
		RunsByState: map[domain.RunState]int{
			domain.RunPlanned:      0,
			domain.RunReserved:     0,
			domain.RunFermenting:   0,
			domain.RunConditioning: 0,
			domain.RunCompleted:    0,
			domain.RunAborted:      0,
		},
		VesselsByState: map[domain.VesselState]int{
			domain.VesselAvailable: 0,
			domain.VesselReserved:  0,
			domain.VesselInUse:     0,
			domain.VesselCleaning:  0,
			domain.VesselRetired:   0,
		},
	}
	for _, run := range snapshot.Runs {
		sample.RunsByState[run.State]++
		sample.TotalRuns++
	}
	sample.FermentingRuns = sample.RunsByState[domain.RunFermenting]
	for _, vessel := range snapshot.Vessels {
		sample.VesselsByState[vessel.State]++
		sample.TotalVessels++
	}
	return sample
}

func copyRunStates(source map[domain.RunState]int) map[domain.RunState]int {
	copied := make(map[domain.RunState]int, len(source))
	for state, count := range source {
		copied[state] = count
	}
	return copied
}

func copyVesselStates(source map[domain.VesselState]int) map[domain.VesselState]int {
	copied := make(map[domain.VesselState]int, len(source))
	for state, count := range source {
		copied[state] = count
	}
	return copied
}
