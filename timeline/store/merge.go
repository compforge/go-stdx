package store

import "github.com/compforge/go-stdx/timeline/model"

// MergeDocument is the shared merge contract for storage implementations. It
// never mutates its inputs. A conflict rejects the entire update. changed=false
// lets durable stores acknowledge duplicate delivery without rewriting the row.
func MergeDocument(id string, current Document, update Update) (doc Document, changed bool, err error) {
	if id == "" {
		return Document{}, false, model.ErrEmptyID
	}
	if current.ID != "" && current.ID != id {
		return Document{}, false, ErrConflict
	}
	doc = current.Clone()
	doc.ID, doc.RootStageID = id, model.RootID(id)
	if incoming := update.Operation; incoming != nil {
		operation, operationChanged, err := mergeOperation(doc.OperationRecord, *incoming)
		if err != nil {
			return Document{}, false, err
		}
		doc.OperationRecord = operation
		changed = operationChanged
	}
	indexes := make(map[model.StageID]int, len(doc.Stages))
	for i, stage := range doc.Stages {
		indexes[stage.ID] = i
	}
	for _, incoming := range update.Stages {
		if incoming.ID == "" || incoming.Revision == 0 || incoming.StartedAt.IsZero() {
			return Document{}, false, ErrConflict
		}
		if i, ok := indexes[incoming.ID]; ok {
			old := doc.Stages[i]
			if old.ParentID != incoming.ParentID || old.Name != incoming.Name || old.Actor != incoming.Actor || !old.StartedAt.Equal(incoming.StartedAt) {
				return Document{}, false, ErrConflict
			}
			if incoming.Revision < old.Revision {
				continue
			}
			if incoming.Revision == old.Revision {
				if !model.SameJSON(old, incoming) {
					return Document{}, false, ErrConflict
				}
				continue
			}
			if !old.FinishedAt.IsZero() {
				return Document{}, false, ErrConflict
			}
			incoming.Attributes = model.CloneJSONAttributes(incoming.Attributes)
			doc.Stages[i] = incoming
		} else {
			incoming.Attributes = model.CloneJSONAttributes(incoming.Attributes)
			indexes[incoming.ID] = len(doc.Stages)
			doc.Stages = append(doc.Stages, incoming)
		}
		changed = true
	}
	for _, incoming := range update.Completed {
		if err := model.ValidateCompleted(id, incoming); err != nil {
			return Document{}, false, err
		}
		revision := uint64(2)
		if i, ok := indexes[incoming.ID]; ok {
			old := doc.Stages[i]
			if !old.FinishedAt.IsZero() {
				if !model.SameJSON(old.Stage, incoming) {
					return Document{}, false, ErrConflict
				}
				continue
			}
			if old.ParentID != incoming.ParentID || old.Name != incoming.Name || old.Actor != incoming.Actor || !old.StartedAt.Equal(incoming.StartedAt) {
				return Document{}, false, ErrConflict
			}
			revision = old.Revision + 1
			incoming.Attributes = model.CloneJSONAttributes(incoming.Attributes)
			doc.Stages[i] = StageUpdate{Stage: incoming, Revision: revision}
		} else {
			incoming.Attributes = model.CloneJSONAttributes(incoming.Attributes)
			indexes[incoming.ID] = len(doc.Stages)
			doc.Stages = append(doc.Stages, StageUpdate{Stage: incoming, Revision: revision})
		}
		changed = true
	}
	sortStageUpdates(doc.Stages)
	return doc, changed, nil
}

// Clone returns a detached document, including all JSON attribute values.
func (doc Document) Clone() Document {
	doc.Attributes = model.CloneJSONAttributes(doc.Attributes)
	doc.Stages = append([]StageUpdate(nil), doc.Stages...)
	for i := range doc.Stages {
		doc.Stages[i].Attributes = model.CloneJSONAttributes(doc.Stages[i].Attributes)
	}
	return doc
}
