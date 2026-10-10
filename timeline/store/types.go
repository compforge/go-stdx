// Package store defines the timeline persistence protocol and its in-memory implementation.
// spec: Recording depends on this contract; storage depends only on shared values,
// never on Recorder, manager, or their process-local lifecycle.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
)

var ErrConflict = errors.New("timeline: conflicting revision or immutable boundary")
var ErrNotFound = errors.New("timeline: document not found")

// OperationRecord holds independently optional start and finish facts. Accepted
// boundaries are immutable; Revision orders attributes from the start writer.
type OperationRecord struct {
	Revision   uint64                     `json:"revision"`
	Operation  string                     `json:"operation"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Status     model.Status               `json:"status"`
	Error      string                     `json:"error,omitempty"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
}

// Document is the durable, current state of one timeline, without read-time
// collection metadata. It retains one state per stage, not an event history.
// JSON uses a document-local actors table and 1-based stage actor_ref values;
// the Go data always contains full Actors.
type Document struct {
	ID          string        `json:"id"`
	RootStageID model.StageID `json:"root_stage_id"`
	OperationRecord
	Stages []StageUpdate `json:"stages,omitempty"`
}

// StageUpdate is the storage envelope. Revision belongs to the writer protocol,
// not to the public stage data returned in snapshots.
type StageUpdate struct {
	model.Stage
	Revision uint64 `json:"revision"`
}

// Update contains only this writer's changes. Each value is a complete state
// at its revision. Operation boundaries may arrive independently; stage owners write Stages.
type Update struct {
	Operation *OperationRecord
	Stages    []StageUpdate
	// Completed contains immutable imported intervals, with no caller-managed revision.
	Completed []model.Stage
}

// Store atomically merges updates into one document per ID. Older revisions are
// ignored; equal revisions with different content and changed immutable boundaries
// return ErrConflict. On error acceptance may be uncertain, so retry the same update.
// Read returns detached data or ErrNotFound. Implementations support concurrent
// callers and cancellation. Connections, schema and retention belong to the caller.
type Store interface {
	Merge(context.Context, string, Update) error
	Read(context.Context, string) (Document, error)
}
