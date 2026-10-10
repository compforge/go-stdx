package store

import "github.com/compforge/go-stdx/timeline/model"

// MergeDocument merges actor-scoped states in acceptance order without mutating
// its inputs. Revisions are local bookkeeping, never distributed arbitration.
// +spec=`Different actors may update the same logical stage independently. Identical states are no-ops; competing states for one actor use last accepted write.`
func MergeDocument(id string, current Document, update Update) (Document, bool, error) {
	if id == "" {
		return Document{}, false, model.ErrEmptyID
	}
	if current.ID != "" && current.ID != id {
		return Document{}, false, ErrConflict
	}
	doc := current.Clone()
	doc.ID, doc.RootStageID = id, model.RootID(id)
	changed := false
	if update.Operation != nil {
		op, c, err := mergeOperation(doc.OperationRecord, *update.Operation)
		if err != nil {
			return Document{}, false, err
		}
		doc.OperationRecord, changed = op, c
	}
	indexes := make(map[model.StageKey]int, len(doc.Stages))
	for i, s := range doc.Stages {
		indexes[s.Stage.Key()] = i
	}
	accept := func(in StageUpdate) {
		key := in.Stage.Key()
		if i, ok := indexes[key]; ok {
			if model.SameJSON(doc.Stages[i].Stage, in.Stage) {
				return
			}
			in.Attributes = model.CloneJSONAttributes(in.Attributes)
			doc.Stages[i] = in
		} else {
			in.Attributes = model.CloneJSONAttributes(in.Attributes)
			indexes[key] = len(doc.Stages)
			doc.Stages = append(doc.Stages, in)
		}
		changed = true
	}
	for _, in := range update.Stages {
		if in.ID == "" || in.Name == "" || in.StartedAt.IsZero() || in.ID == in.ParentID || in.ID == model.RootID(id) {
			return Document{}, false, model.ErrInvalidStage
		}
		accept(in)
	}
	for _, in := range update.Completed {
		if err := model.ValidateCompleted(id, in); err != nil {
			return Document{}, false, err
		}
		accept(StageUpdate{Stage: in, Revision: 2})
	}
	sortStageUpdates(doc.Stages)
	return doc, changed, nil
}

func (doc Document) Clone() Document {
	doc.Attributes = model.CloneJSONAttributes(doc.Attributes)
	doc.Stages = append([]StageUpdate(nil), doc.Stages...)
	for i := range doc.Stages {
		doc.Stages[i].Attributes = model.CloneJSONAttributes(doc.Stages[i].Attributes)
	}
	return doc
}
