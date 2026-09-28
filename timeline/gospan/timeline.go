// Package gospantimeline implements timeline.Timeline with gospan spans and an
// in-memory projection. No collector, database, or process-global tracer is used.
package gospantimeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/akmadian/gospan"
	"github.com/compforge/go-stdx/timeline"
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
}

type stage struct {
	owner *recorder
	span  *gospan.Span
	ended bool // protected by owner.mu, including Fail + End as one transition
}

type parentKey struct{}

type parent struct {
	owner   *recorder
	spanCtx context.Context
}

var _ timeline.Timeline = (*recorder)(nil)
var _ timeline.Stage = (*stage)(nil)

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
	t := &recorder{tracer: tracer, root: root, rootCtx: rootCtx, sink: sink}
	return t, nil
}

func (t *recorder) ID() string { return t.sink.id }

func (t *recorder) context(ctx, spanCtx context.Context) context.Context {
	return context.WithValue(ctx, parentKey{}, parent{owner: t, spanCtx: spanCtx})
}

func (t *recorder) Begin(ctx context.Context, name string, fields ...timeline.Field) (context.Context, timeline.Stage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return ctx, inertStage{}
	}
	spanCtx := t.rootCtx
	if p, ok := ctx.Value(parentKey{}).(parent); ok && p.owner == t {
		spanCtx = p.spanCtx
	}
	spanCtx, span := t.tracer.Start(spanCtx, name, t.attrs(fields)...)
	t.active++
	return t.context(ctx, spanCtx), &stage{owner: t, span: span}
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

func (s *stage) End(err error, fields ...timeline.Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.span.SetAttrs(s.owner.attrs(fields)...)
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
	return t.sink.snapshot(err == nil), err
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

func (inertStage) SetFields(...timeline.Field)  {}
func (inertStage) End(error, ...timeline.Field) {}

func (t *recorder) Flush(ctx context.Context) error {
	_, err := t.Snapshot(ctx)
	return err
}
