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

var ErrConflict = errors.New("timeline: invalid document identity or operation boundary")
var ErrNotFound = errors.New("timeline: document not found")

// OperationRecord holds independently optional start and finish observations.
// Revision is writer-local metadata, not a cross-process ordering authority.
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
// collection metadata. It retains one state per (StageID, Actor.Key), not an event history.
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

// Update contains only locally accepted changes. Stages carry full states;
// operation start and finish boundaries may arrive independently.
type Update struct {
	Operation *OperationRecord
	Stages    []StageUpdate
	// Completed contains imported intervals, with no caller-managed revision.
	Completed []model.Stage
}

// Store atomically merges actor-scoped updates into one document per ID. The last
// accepted state replaces the same stage/actor; identical content is a no-op.
// On error acceptance may be uncertain. Retrying unchanged content does not
// duplicate records, but may overwrite a competing same-actor write.
// Reads return detached data. Connections, schema and retention belong to callers.
type Store interface {
	Merge(context.Context, string, Update) error
	Read(context.Context, string) (Document, error)
	// MGet omits missing IDs and returns each found document once; order is unspecified.
	MGet(context.Context, []string) ([]Document, error)
	// Latest returns documents updated strictly after the time, ordered by
	// updated_at DESC, id DESC. A nonpositive limit returns no documents.
	Latest(context.Context, time.Time, int) ([]Document, error)
}
