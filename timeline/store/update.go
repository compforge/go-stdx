package store

import "github.com/compforge/go-stdx/timeline/model"

// CoalesceUpdates keeps the last accepted state for each actor-scoped stage.
func CoalesceUpdates(updates []Update) (Update, error) {
	doc := Overlay(Document{}, updates...)
	result := Update{Stages: doc.Stages}
	if !doc.StartedAt.IsZero() || !doc.FinishedAt.IsZero() {
		op := doc.OperationRecord
		result.Operation = &op
	}
	return result, nil
}

// Overlay applies already accepted documents and local pending updates. Historical
// empty Actors remain readable; admission validates only newly recorded stages.
func Overlay(base Document, updates ...Update) Document {
	doc := base.Clone()
	for _, u := range updates {
		if u.Operation != nil {
			if op, _, err := mergeOperation(doc.OperationRecord, *u.Operation); err == nil {
				doc.OperationRecord = op
			}
		}
		indexes := make(map[model.StageKey]int, len(doc.Stages))
		for i, s := range doc.Stages {
			indexes[s.Stage.Key()] = i
		}
		stages := append([]StageUpdate(nil), u.Stages...)
		for _, s := range u.Completed {
			stages = append(stages, StageUpdate{Stage: s, Revision: 2})
		}
		for _, s := range stages {
			s.Attributes = model.CloneJSONAttributes(s.Attributes)
			if i, ok := indexes[s.Stage.Key()]; ok {
				doc.Stages[i] = s
			} else {
				indexes[s.Stage.Key()] = len(doc.Stages)
				doc.Stages = append(doc.Stages, s)
			}
		}
	}
	sortStageUpdates(doc.Stages)
	return doc
}

func (d Document) Update() Update {
	u := Update{Stages: d.Stages}
	if !d.StartedAt.IsZero() || !d.FinishedAt.IsZero() {
		op := d.OperationRecord
		u.Operation = &op
	}
	return u
}
