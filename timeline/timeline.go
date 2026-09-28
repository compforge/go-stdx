// Package timeline defines a queryable timeline of an operation and its stages.
// Implementations own recording and snapshot synchronization; callers own the
// operation's business result and decide where snapshots are exported.
package timeline

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Timeline is a recording handle for one business operation. Multiple handles
// may contribute through a shared Store. Methods and returned Stages are safe
// for concurrent use. Business code owns completion and stage attribution.
type Timeline interface {
	// Flush confirms this handle's preceding records reached its backend.
	// It does not wait for records buffered by other handles.
	Flush(context.Context) error
	// ID is the immutable, caller-supplied identity of this operation.
	// Its meaning and uniqueness scope belong to the caller.
	ID() string
	// SetFields records operation attributes; the coordinator owns these keys.
	SetFields(fields ...Field)
	// Begin starts a stage beneath the stage carried by ctx, or beneath the
	// operation when ctx has no stage from this timeline. The returned context
	// retains the caller's values, cancellation and deadline. Shared handles
	// accept late stages after Finish; sealing local backends may return no-ops.
	Begin(ctx context.Context, name string, fields ...Field) (context.Context, Stage)
	// Snapshot flushes this handle and collects its backend's current facts.
	// Collection reports local flush and backend read success, never global
	// completeness. On error it returns best-effort data alongside the error.
	Snapshot(ctx context.Context) (Snapshot, error)
	// Finish records the operation result and returns a snapshot. Shared stores
	// accept late stage updates; a local backend may seal and return
	// ErrActiveStages until its stages end. The returned error is a collection
	// error, independent of operationErr. Finish never closes a shared Store.
	Finish(ctx context.Context, operationErr error) (Snapshot, error)
}

// Stage represents running work or a wait. It may end on a different goroutine
// from the one that began it. A parent may end before its children.
type Stage interface {
	// SetFields merges attributes; the last value for a key wins.
	SetFields(fields ...Field)
	// End atomically records final fields and the result exactly once. Later End and SetFields calls have
	// no effect. A nil error means success.
	End(err error, fields ...Field)
}

var ErrEmptyID = errors.New("timeline: ID must not be empty")

var ErrInvalidField = errors.New("timeline: field is not JSON serializable")

var ErrActiveStages = errors.New("timeline: operation still has active stages")

// Field is recording input. Values must be JSON serializable and must not be
// mutated during the recording call. Implementations encode them before returning.
type Field struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// StageID is an opaque identity. Shared recorders generate UUIDs independently
// in each process; local backends namespace their backend-local IDs.
type StageID string

type Status string

const (
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// StageRecord retains the interval and result of a stage. ParentID refers to
// another stage or Snapshot.RootStageID. FinishedAt is zero while work is running.
type StageRecord struct {
	Revision   uint64                     `json:"revision,omitempty"`
	ID         StageID                    `json:"id"`
	ParentID   StageID                    `json:"parent_id"`
	Actor      Actor                      `json:"actor,omitzero"`
	Elapsed    time.Duration              `json:"elapsed_ns,omitempty"`
	Name       string                     `json:"name"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Status     Status                     `json:"status"`
	Error      string                     `json:"error,omitempty"`
	Fields     map[string]json.RawMessage `json:"fields,omitempty"`
}

// Duration measures a finished interval, or a running interval at capturedAt.
func (s StageRecord) Duration(capturedAt time.Time) time.Duration {
	if !s.FinishedAt.IsZero() && s.Elapsed != 0 {
		return s.Elapsed
	}
	return interval(s.StartedAt, s.FinishedAt, capturedAt)
}

// Snapshot is a detached view. Stages are ordered by start time, then ID;
// overlapping intervals stay overlapping rather than being added together.
// Collection describes the observations this reader can confirm. A successful
// read does not prove all processes have flushed or that no late stage remains.
// All slices and JSON field values belong to this snapshot. Standard json.Marshal
// and json.Unmarshal persist it without a backend or a caller-defined DTO.
// Times use time.Time JSON encoding (RFC 3339); zero FinishedAt means running.
type Snapshot struct {
	ID          string                     `json:"id"`
	RootStageID StageID                    `json:"root_stage_id"`
	Operation   string                     `json:"operation"`
	StartedAt   time.Time                  `json:"started_at"`
	FinishedAt  time.Time                  `json:"finished_at,omitempty"`
	CapturedAt  time.Time                  `json:"captured_at"`
	Status      Status                     `json:"status"`
	Error       string                     `json:"error,omitempty"`
	Fields      map[string]json.RawMessage `json:"fields,omitempty"`
	Stages      []StageRecord              `json:"stages,omitempty"`
	Collection  Collection                 `json:"collection"`
}

// Collection is deliberately scoped: neither field asserts that all distributed
// participants have reported. Remote crashed/unflushed producers are unknown.
type Collection struct {
	LocalFlushed bool `json:"local_flushed"`
	StoreRead    bool `json:"store_read"`
}

func (s Snapshot) Duration() time.Duration {
	return interval(s.StartedAt, s.FinishedAt, s.CapturedAt)
}

func interval(start, end, captured time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	if end.IsZero() {
		end = captured
	}
	return end.Sub(start)
}

type contextKey struct{}

// NewContext optionally carries t without changing ctx's lifetime.
// Passing Timeline directly as a function argument needs no context binding.
func NewContext(ctx context.Context, t Timeline) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}

// FromContext retrieves a Timeline explicitly attached with NewContext.
// Constructors and Begin do not attach or replace this value.
func FromContext(ctx context.Context) (Timeline, bool) {
	if ctx == nil {
		return nil, false
	}
	t, ok := ctx.Value(contextKey{}).(Timeline)
	return t, ok && t != nil
}
