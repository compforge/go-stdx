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
// may contribute through a shared Store. Methods and returned handles are safe
// for concurrent use. Business code owns completion and stage attribution.
type Timeline interface {
	// Record copies and buffers a completed stage. It performs no IO; Flush
	// confirms persistence. ID, name, actual start/end times and a terminal status
	// must be supplied. An omitted ParentID defaults to the root; a supplied parent
	// need not exist and is preserved for later association. Actor is preserved.
	// Repeated identical IDs are idempotent; conflicting persisted facts fail Flush.
	Record(Stage) error
	// Flush confirms this handle's preceding records reached its backend.
	// It does not wait for records buffered by other handles.
	Flush(context.Context) error
	// ID is the immutable, caller-supplied identity of this operation.
	// Its meaning and uniqueness scope belong to the caller.
	ID() string
	// SetAttributes records operation attributes; the coordinator owns these keys.
	SetAttributes(attributes ...Attribute)
	// Begin starts a stage using the local clock unless source times are supplied.
	// Parentage is explicit; BeginContext is an optional context convenience.
	Begin(name string, opts ...StageOption) StageHandle
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

// StageHandle represents running work or a wait. A leaf stage is the basic
// recording unit of one component's execution flow: callers should begin and end
// it in the same goroutine. Parallel execution flows contribute separate stages.
// A parent stage may group parallel child stages across components; its own
// lifecycle remains owned by the coordinating component. A parent may end before
// its children.
type StageHandle interface {
	ID() StageID
	// SetAttributes merges attributes; the last value for a key wins.
	SetAttributes(attributes ...Attribute)
	// End atomically records final attributes and the result exactly once. Later End and SetAttributes calls have
	// no effect. A nil error means success.
	End(err error, opts ...EndOption)
}

var ErrInvalidStage = errors.New("timeline: invalid stage")

var ErrEmptyID = errors.New("timeline: ID must not be empty")

var ErrInvalidAttribute = errors.New("timeline: attribute is not JSON serializable")

var ErrActiveStages = errors.New("timeline: operation still has active stages")

// Attribute is recording input. Values must be JSON serializable and must not be
// mutated during the recording call. Implementations encode them before returning.
type Attribute struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// StageID is an opaque identity. Begin generates a UUID by default; callers
// may supply a stable ID for a specific externally observed execution.
type StageID string

type Status string

const (
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// Stage retains the interval and result of work or a wait. Leaf stages represent
// one component's execution flow; parent stages may group parallel child stages.
// ParentID refers to another stage or Snapshot.RootStageID. FinishedAt is zero
// while work is running.
type Stage struct {
	ID         StageID                    `json:"id"`
	ParentID   StageID                    `json:"parent_id"`
	Actor      Actor                      `json:"actor,omitzero"`
	Elapsed    time.Duration              `json:"elapsed_ns,omitempty"`
	Name       string                     `json:"name"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Status     Status                     `json:"status"`
	Error      string                     `json:"error,omitempty"`
	Attributes map[string]json.RawMessage `json:"attributes,omitempty"`
	// Code is an optional caller-defined value, independent of Status and Error.
	// Timeline records it without inferring or interpreting its meaning.
	Code string `json:"code,omitempty"`
}

// Duration measures a finished interval, or a running interval at capturedAt.
func (s Stage) Duration(capturedAt time.Time) time.Duration {
	if !s.FinishedAt.IsZero() && s.Elapsed != 0 {
		return s.Elapsed
	}
	return interval(s.StartedAt, s.FinishedAt, capturedAt)
}

// Snapshot is a detached view. Stages are ordered by start time, then ID;
// overlapping intervals stay overlapping rather than being added together.
// Collection describes the observations this reader can confirm. A successful
// read does not prove all processes have flushed or that no late stage remains.
// All slices and JSON attribute values belong to this snapshot. Standard json.Marshal
// and json.Unmarshal persist it without a backend or a caller-defined DTO.
// JSON stores actors once in a payload-local table; stage actor_ref values are
// resolved back to full Actors when decoding.
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
	Attributes  map[string]json.RawMessage `json:"attributes,omitempty"`
	Stages      []Stage                    `json:"stages,omitempty"`
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

// BeginContext is an optional adapter; Timeline itself does not require context
// binding. It preserves cancellation and only inherits a parent from this timeline.
func BeginContext(ctx context.Context, t Timeline, name string, opts ...StageOption) (context.Context, StageHandle) {
	if ref, ok := StageFromContext(ctx); ok && ref.TimelineID == t.ID() && ref.StageID != "" {
		opts = append([]StageOption{WithParent(ref.StageID)}, opts...)
	}
	stage := t.Begin(name, opts...)
	if stage.ID() == "" {
		return ctx, stage
	}
	return NewStageContext(ctx, StageRef{TimelineID: t.ID(), StageID: stage.ID()}), stage
}
