// Package timeline defines a queryable timeline of an operation and its stages.
// Implementations own recording and snapshot synchronization; callers own the
// operation's business result and decide where snapshots are exported.
package timeline

import (
	"context"
	"errors"
	"time"
)

// Timeline represents one operation. Methods and returned Stages are safe for
// concurrent use. The completion owner must end every stage and call Finish,
// even after the original request has been canceled.
type Timeline interface {
	// Begin starts a stage beneath the stage carried by ctx, or beneath the
	// operation when ctx has no stage from this timeline. The returned context
	// retains the caller's values, cancellation and deadline. After Finish,
	// Begin returns an inert Stage and leaves ctx unchanged.
	Begin(ctx context.Context, name string, fields ...Field) (context.Context, Stage)
	// Snapshot waits for preceding records to be collected without ending any
	// work. ctx bounds the collection wait. On error the returned snapshot is
	// best effort and Complete is false. Later records may also be included.
	Snapshot(ctx context.Context) (Snapshot, error)
	// Finish freezes the operation's result and releases recording resources.
	// ErrActiveStages leaves the operation open so its owner can end the stages
	// and retry. Otherwise the first result wins, including when ctx expires
	// waiting for collection; a subsequent call can await the final snapshot.
	// The returned error describes recording, independently of operationErr.
	Finish(ctx context.Context, operationErr error) (Snapshot, error)
}

// Stage represents running work or a wait. It may end on a different goroutine
// from the one that began it. A parent may end before its children.
type Stage interface {
	// SetFields merges attributes; the last value for a key wins.
	SetFields(fields ...Field)
	// End records the result exactly once. Later End and SetFields calls have
	// no effect. A nil error means success.
	End(err error)
}

var ErrActiveStages = errors.New("timeline: operation still has active stages")

// Field attaches a value to an operation or stage. Field slices are copied;
// referenced values must be immutable after recording (copies are shallow).
type Field struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// StageID is unique within one Timeline, not across processes or operations.
type StageID int64

type Status string

const (
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// StageRecord retains the interval and result of a stage. ParentID refers to
// another stage or Snapshot.ID. FinishedAt is zero while work is running.
type StageRecord struct {
	ID         StageID   `json:"id"`
	ParentID   StageID   `json:"parent_id"`
	Name       string    `json:"name"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Status     Status    `json:"status"`
	Error      string    `json:"error,omitempty"`
	Fields     []Field   `json:"fields,omitempty"`
}

// Duration measures a finished interval, or a running interval at capturedAt.
func (s StageRecord) Duration(capturedAt time.Time) time.Duration {
	return interval(s.StartedAt, s.FinishedAt, capturedAt)
}

// Snapshot is a detached view. Stages are ordered by start time, then ID;
// overlapping intervals stay overlapping rather than being added together.
// Complete means preceding records were collected, not that the work ended.
// CapturedAt is frozen at FinishedAt once a final snapshot is complete.
// Field values retain the shallow-copy contract described by Field.
type Snapshot struct {
	ID         StageID       `json:"id"`
	Operation  string        `json:"operation"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at,omitempty"`
	CapturedAt time.Time     `json:"captured_at"`
	Status     Status        `json:"status"`
	Error      string        `json:"error,omitempty"`
	Fields     []Field       `json:"fields,omitempty"`
	Stages     []StageRecord `json:"stages,omitempty"`
	Complete   bool          `json:"complete"`
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

// NewContext carries t without changing ctx's lifetime.
func NewContext(ctx context.Context, t Timeline) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}

func FromContext(ctx context.Context) (Timeline, bool) {
	if ctx == nil {
		return nil, false
	}
	t, ok := ctx.Value(contextKey{}).(Timeline)
	return t, ok && t != nil
}
