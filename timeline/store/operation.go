package store

import "github.com/compforge/go-stdx/timeline/model"

// Boundaries are independently optional. A last accepted boundary replaces the
// corresponding value, without clearing the other boundary or consulting a
// process-local revision. The result is an observation, not business arbitration.
func mergeOperation(old, in OperationRecord) (OperationRecord, bool, error) {
	start, finish := !in.StartedAt.IsZero(), !in.FinishedAt.IsZero()
	if !start && !finish {
		return old, false, ErrConflict
	}
	next := old
	if start {
		next.Operation, next.StartedAt = in.Operation, in.StartedAt
		next.Attributes = model.CloneJSONAttributes(in.Attributes)
	}
	if finish {
		if in.Status != model.Succeeded && in.Status != model.Failed && in.Status != model.Canceled {
			return old, false, ErrConflict
		}
		next.FinishedAt, next.Status, next.Error = in.FinishedAt, in.Status, in.Error
	}
	if next.FinishedAt.IsZero() {
		next.Status = model.Unknown
	}
	// Repeated identical facts must not update database timestamps due to a
	// writer's private sequence number alone.
	if model.SameJSON(old, next) {
		return old, false, nil
	}
	next.Revision = in.Revision
	return next, true, nil
}
