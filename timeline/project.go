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
	sortStages(d.Stages)
	status := d.Status
	if status == "" {
		status = Running
	}
	return Snapshot{ID: d.ID, RootStageID: d.RootStageID, Operation: d.Operation,
		StartedAt: d.StartedAt, FinishedAt: d.FinishedAt, CapturedAt: capturedAt,
		Status: status, Error: d.Error, Fields: d.Fields, Stages: d.Stages}
}
func sortStages(stages []StageRecord) {
	slices.SortFunc(stages, func(a, b StageRecord) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})

}
