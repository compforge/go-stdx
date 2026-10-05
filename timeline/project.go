package timeline

import (
	"cmp"
	"slices"
	"time"
)

// Snapshot returns detached read-time data. Storage success is set by the
// reader, never persisted as a claim about future collection completeness.
func (d Document) Snapshot(capturedAt time.Time) Snapshot {
	d = cloneDocument(d)
	stages := make([]Stage, len(d.Stages))
	for i := range d.Stages {
		stages[i] = d.Stages[i].Stage
	}
	sortStages(stages)
	status := d.Status
	if status == "" {
		status = Running
	}
	return Snapshot{ID: d.ID, RootStageID: d.RootStageID, Operation: d.Operation,
		StartedAt: d.StartedAt, FinishedAt: d.FinishedAt, CapturedAt: capturedAt,
		Status: status, Error: d.Error, Attributes: d.Attributes, Stages: stages}
}
func sortStages(stages []Stage) {
	slices.SortFunc(stages, func(a, b Stage) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})

}

func sortStageUpdates(stages []StageUpdate) {
	slices.SortFunc(stages, func(a, b StageUpdate) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
}
