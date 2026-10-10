package store

import "github.com/compforge/go-stdx/timeline/model"

// CoalesceUpdates combines full-state updates into one immutable submission.
// Conflicting equal revisions are rejected before reaching storage.
func CoalesceUpdates(updates []Update) (Update, error) {
	// Coalesce intermediate states before IO; only latest revisions are durable.
	update := Update{}
	stages := make(map[model.StageID]StageUpdate)
	for _, pending := range updates {
		update.Completed = append(update.Completed, pending.Completed...)
		if pending.Operation != nil {
			update.Operation = pending.Operation
		}
		for _, stage := range pending.Stages {
			if old, ok := stages[stage.ID]; ok && old.Revision == stage.Revision && !model.SameJSON(old, stage) {
				return Update{}, ErrConflict
			}
			stages[stage.ID] = stage
		}
	}
	for _, stage := range stages {
		update.Stages = append(update.Stages, stage)
	}
	sortStageUpdates(update.Stages)
	return update, nil
}
