package timeline

import (
	"cmp"
	"slices"
	"time"
)

// Project folds records in Store read order into detached data. Transport
// duplicates are ignored. Stage revisions prevent delayed running updates from
// resurrecting a completed stage. It makes no claim about unseen producers.
func Project(id string, records []Record, capturedAt time.Time) Snapshot {
	snapshot := Snapshot{ID: id, RootStageID: rootID(id), CapturedAt: capturedAt, Status: Running}
	stages := make(map[StageID]Record)
	seen := make(map[string]bool)
	for _, record := range records {
		if seen[record.ID] {
			continue
		}
		seen[record.ID] = true
		switch record.Kind {
		case OperationStarted:
			if snapshot.StartedAt.IsZero() {
				snapshot.StartedAt, snapshot.Operation = record.At, record.Operation
				snapshot.Fields = mergeFields(snapshot.Fields, record.Fields)
			}
		case OperationFields:
			snapshot.Fields = mergeFields(snapshot.Fields, record.Fields)
		case OperationFinished:
			if snapshot.FinishedAt.IsZero() {
				snapshot.FinishedAt, snapshot.Status, snapshot.Error = record.At, record.Status, record.Error
			}
		case StageUpdated:
			if record.Stage == nil {
				continue
			}
			previous, ok := stages[record.Stage.ID]
			if !ok || record.Revision > previous.Revision {
				stages[record.Stage.ID] = record
			}
		}
	}
	for _, record := range stages {
		stage := *record.Stage
		stage.Fields = cloneJSONFields(stage.Fields)
		snapshot.Stages = append(snapshot.Stages, stage)
	}
	slices.SortFunc(snapshot.Stages, func(a, b StageRecord) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return snapshot
}
