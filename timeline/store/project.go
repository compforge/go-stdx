package store

import (
	"cmp"
	"slices"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
)

// Snapshot returns detached read-time data. Storage success is set by the
// reader, never persisted as a claim about future collection completeness.
func (d Document) Snapshot(capturedAt time.Time) model.Snapshot {
	d = d.Clone()
	stages := make([]model.Stage, len(d.Stages))
	for i := range d.Stages {
		stages[i] = d.Stages[i].Stage
	}
	sortStages(stages)
	status := d.Status
	if status == "" {
		status = model.Unknown
	}
	return model.Snapshot{ID: d.ID, RootStageID: d.RootStageID, Operation: d.Operation,
		StartedAt: d.StartedAt, FinishedAt: d.FinishedAt, CapturedAt: capturedAt,
		Status: status, Error: d.Error, Attributes: d.Attributes, Stages: stages}
}
func sortStages(stages []model.Stage) {
	slices.SortFunc(stages, func(a, b model.Stage) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		if order := cmp.Compare(a.ID, b.ID); order != 0 {
			return order
		}
		return cmp.Compare(a.Actor.Key(), b.Actor.Key())
	})

}

func sortStageUpdates(stages []StageUpdate) {
	slices.SortFunc(stages, func(a, b StageUpdate) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		if order := cmp.Compare(a.ID, b.ID); order != 0 {
			return order
		}
		return cmp.Compare(a.Actor.Key(), b.Actor.Key())
	})
}
