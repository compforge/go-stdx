package manager

import (
	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

// Restore resumes operation facts from a detached document without recording a
// new start or querying storage. It does not claim ownership over other writers.
func restoreRecorder(doc store.Document, options ...Option) (*recorder, error) {
	r, err := newRecorder(doc.ID, options...)
	if err != nil {
		return nil, err
	}
	r.operation = doc.OperationRecord
	r.operation.Attributes = model.CloneJSONAttributes(doc.Attributes)
	r.started = !doc.StartedAt.IsZero()
	r.finished = !doc.FinishedAt.IsZero()
	return r, nil
}

// RestoreStage resumes a stage read from this recorder's document. Revisions and
// terminal state are preserved. Restored intervals use their recorded wall times;
// a monotonic clock cannot be recovered from persistent data.
func (r *recorder) RestoreStage(record store.StageUpdate) StageHandle {
	record.Attributes = model.CloneJSONAttributes(record.Attributes)
	return &recordedStage{owner: r, id: record.ID, record: record, ended: !record.FinishedAt.IsZero()}
}
