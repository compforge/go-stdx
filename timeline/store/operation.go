package store

import "github.com/compforge/go-stdx/timeline/model"

// mergeOperation merges independently optional boundaries. A finish-only writer
// must not erase a start, and an older late start must not reopen a finished root.
// Revisions order the start writer's attributes; boundary identity is independent
// of revision so another process may contribute the missing operation boundary.
func mergeOperation(old, incoming OperationRecord) (OperationRecord, bool, error) {
	hasStart, hasFinish := !incoming.StartedAt.IsZero(), !incoming.FinishedAt.IsZero()
	if incoming.Revision == 0 || (!hasStart && !hasFinish) {
		return old, false, ErrConflict
	}
	next := old
	if hasStart {
		if !old.StartedAt.IsZero() {
			if old.Operation != incoming.Operation || !old.StartedAt.Equal(incoming.StartedAt) {
				return old, false, ErrConflict
			}
			if incoming.Revision == old.Revision && !model.SameJSON(old.Attributes, incoming.Attributes) {
				return old, false, ErrConflict
			}
		}
		if old.StartedAt.IsZero() || incoming.Revision > old.Revision {
			next.Operation, next.StartedAt = incoming.Operation, incoming.StartedAt
			next.Attributes = model.CloneJSONAttributes(incoming.Attributes)
		}
	}
	if hasFinish {
		if incoming.Status != model.Succeeded && incoming.Status != model.Failed && incoming.Status != model.Canceled {
			return old, false, ErrConflict
		}
		if !old.FinishedAt.IsZero() && (!old.FinishedAt.Equal(incoming.FinishedAt) || old.Status != incoming.Status || old.Error != incoming.Error) {
			return old, false, ErrConflict
		}
		next.FinishedAt, next.Status, next.Error = incoming.FinishedAt, incoming.Status, incoming.Error
	}
	if next.FinishedAt.IsZero() {
		next.Status = model.Unknown
	}
	next.Revision = max(old.Revision, incoming.Revision)
	return next, !model.SameJSON(old, next), nil
}
