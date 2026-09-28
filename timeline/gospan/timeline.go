// Package gospantimeline implements timeline.Timeline with gospan spans and an
// in-memory projection. No collector, database, or process-global tracer is used.
package gospantimeline

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/akmadian/gospan"
	"github.com/compforge/go-stdx/timeline"
	"github.com/google/uuid"
)

type recorder struct {
	mu       sync.Mutex
	tracer   *gospan.Tracer
	root     *gospan.Span
	rootCtx  context.Context
	sink     *projection
	active   int
	sequence uint64
	fieldErr error // first encoding error; lost input keeps subsequent snapshots incomplete
	finished bool
	imports  *timeline.Recorder
	stages   map[timeline.StageID]*stage
}

type stage struct {
	owner   *recorder
	span    *gospan.Span
	data    timeline.Stage
	started time.Time
	spanCtx context.Context
	ended   bool // protected by owner.mu, including Fail + End as one transition
}

var _ timeline.Timeline = (*recorder)(nil)
var _ timeline.StageHandle = (*stage)(nil)

// New begins an operation with a non-empty, immutable caller-supplied ID.
// It does not bind the returned Timeline to context. The construction context
// does not own the operation lifetime; the completion owner must call Finish.
func New(_ context.Context, id, operation string, fields ...timeline.Field) (timeline.Timeline, error) {
	if id == "" {
		return nil, timeline.ErrEmptyID
	}
	initialAttrs, err := attrs(fields)
	if err != nil {
		return nil, err
	}
	sink := newProjection(id)
	// This writer only projects into memory. Blocking on its bounded queue
	// preserves stage boundaries without coupling producers to external IO.
	tracer, err := gospan.New(sink, gospan.WithBufferSize(128), gospan.WithBlockingPolicy())
	if err != nil {
		return nil, err
	}
	// Span IDs are local to a tracer. Never inherit a foreign gospan parent.
	rootCtx, root := tracer.Start(context.Background(), operation, initialAttrs...)
	imports, _ := timeline.New(id)
	t := &recorder{tracer: tracer, root: root, rootCtx: rootCtx, sink: sink, imports: imports, stages: make(map[timeline.StageID]*stage)}
	return t, nil
}

func (t *recorder) ID() string { return t.sink.id }

func (t *recorder) Begin(name string, opts ...timeline.StageOption) timeline.StageHandle {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return inertStage{}
	}
	now := time.Now()
	data := timeline.Stage{ID: timeline.StageID(uuid.NewString()), ParentID: t.sink.rootID,
		Name: name, StartedAt: now.UTC(), Status: timeline.Running}
	for _, opt := range opts {
		if err := opt(&data); err != nil {
			t.fieldErr = errors.Join(t.fieldErr, err)
			return inertStage{}
		}
	}
	if data.ID == "" || data.ID == data.ParentID || data.ID == t.sink.rootID || data.StartedAt.IsZero() {
		t.fieldErr = errors.Join(t.fieldErr, timeline.ErrInvalidStage)
		return inertStage{}
	}
	if _, exists := t.stages[data.ID]; exists {
		t.fieldErr = errors.Join(t.fieldErr, timeline.ErrConflict)
		return inertStage{}
	}
	// Parent IDs are timeline data; only a local live handle supplies a native parent.
	spanCtx := t.rootCtx
	if parent := t.stages[data.ParentID]; parent != nil {
		spanCtx = parent.spanCtx
	}
	data.Fields = cloneFields(data.Fields)
	spanCtx, span := t.tracer.Start(spanCtx, name, slog.Any(boundaryKey, boundary{data}))
	s := &stage{owner: t, span: span, data: data, started: now, spanCtx: spanCtx}
	if !data.StartedAt.Equal(now) {
		s.started = time.Time{}
	}
	t.stages[data.ID] = s
	t.active++
	return s
}

func (s *stage) ID() timeline.StageID { return s.data.ID }

// Record buffers source intervals separately from native spans: replaying a
// historical interval through gospan's wall clock would invent its timestamps.
func (t *recorder) Record(data timeline.Stage) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing := t.stages[data.ID]; existing != nil {
		return timeline.ErrConflict
	}
	if err := t.imports.Record(data); err != nil {
		return err
	}
	t.stages[data.ID] = nil
	return nil
}

func (t *recorder) SetFields(fields ...timeline.Field) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished {
		t.root.SetAttrs(t.attrs(fields)...)
	}
}

func (s *stage) SetFields(fields ...timeline.Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if !s.ended {
		s.span.SetAttrs(s.owner.attrs(fields)...)
	}
}

func (s *stage) End(err error, opts ...timeline.EndOption) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	now := time.Now()
	data := s.data
	data.FinishedAt = now.UTC()
	data.Fields = nil // End options add final fields; prior SetFields stay in the projection.
	for _, opt := range opts {
		if optionErr := opt(&data); optionErr != nil {
			s.owner.fieldErr = errors.Join(s.owner.fieldErr, optionErr)
		}
	}
	if data.FinishedAt.IsZero() || data.FinishedAt.Before(data.StartedAt) {
		s.owner.fieldErr = errors.Join(s.owner.fieldErr, timeline.ErrInvalidStage)
	}
	if !s.started.IsZero() && data.FinishedAt.Equal(now) {
		data.Elapsed = now.Sub(s.started)
	}
	s.span.SetAttrs(slog.Any(boundaryKey, boundary{data}))
	s.span.Fail(err)
	s.span.End()
	s.ended = true
	s.owner.active--
}

func (t *recorder) Snapshot(ctx context.Context) (timeline.Snapshot, error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return t.closedSnapshot(ctx)
	}
	t.sequence++
	seq := t.sequence
	// A typed private marker cannot be confused with caller attributes. Its
	// consumption fences every mutation completed before this Snapshot call.
	t.root.SetAttrs(slog.Any(checkpointKey, checkpoint{sequence: seq}))
	t.mu.Unlock()
	err := t.sink.wait(ctx, seq)
	return t.snapshot(err)
}

func (t *recorder) Finish(ctx context.Context, operationErr error) (timeline.Snapshot, error) {
	t.mu.Lock()
	if t.active != 0 {
		t.mu.Unlock()
		snapshot, err := t.Snapshot(ctx)
		if err != nil {
			return snapshot, errors.Join(timeline.ErrActiveStages, err)
		}
		return snapshot, timeline.ErrActiveStages
	}
	if !t.finished {
		t.root.Fail(operationErr)
		t.root.End()
		t.finished = true
	}
	t.mu.Unlock()
	return t.closedSnapshot(ctx)
}

func (t *recorder) closedSnapshot(ctx context.Context) (timeline.Snapshot, error) {
	// Close initiates shutdown even if ctx expires. The writer drains in the
	// background; another Finish/Snapshot can await the same terminal result.
	err := t.tracer.Close(ctx)
	return t.snapshot(err)
}

// snapshot combines transport collection and field encoding failures without
// changing the operation result. Invalid observations must not look complete.
func (t *recorder) snapshot(collectionErr error) (timeline.Snapshot, error) {
	t.mu.Lock()
	err := errors.Join(collectionErr, t.fieldErr)
	t.mu.Unlock()
	imports, importErr := t.imports.Snapshot(context.Background())
	if errors.Is(importErr, timeline.ErrNotFound) && imports.Collection.LocalFlushed {
		importErr = nil
	}
	err = errors.Join(err, importErr)
	result := t.sink.snapshot(err == nil)
	result.Stages = append(result.Stages, imports.Stages...)
	if len(imports.Stages) != 0 {
		result.CapturedAt = imports.CapturedAt
	}
	slices.SortFunc(result.Stages, func(a, b timeline.Stage) int {
		if order := a.StartedAt.Compare(b.StartedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return result, err
}

// Caller holds mu; encoding happens before events are queued so later caller
// mutations cannot change recorded values or race with the projection writer.
func (t *recorder) attrs(fields []timeline.Field) []slog.Attr {
	result, err := attrs(fields)
	if t.fieldErr == nil {
		t.fieldErr = err
	}
	return result
}

func attrs(fields []timeline.Field) ([]slog.Attr, error) {
	result := make([]slog.Attr, 0, len(fields))
	var firstErr error
	for _, field := range fields {
		data, err := json.Marshal(field.Value)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: %q: %v", timeline.ErrInvalidField, field.Key, err)
			}
			continue
		}
		result = append(result, slog.Any(field.Key, fieldValue{value: data}))
	}
	return result, firstErr
}

type inertStage struct{}

type fieldValue struct{ value json.RawMessage }

func (inertStage) SetFields(...timeline.Field)      {}
func (inertStage) End(error, ...timeline.EndOption) {}
func (inertStage) ID() timeline.StageID             { return "" }

func (t *recorder) Flush(ctx context.Context) error {
	_, err := t.Snapshot(ctx)
	return err
}
