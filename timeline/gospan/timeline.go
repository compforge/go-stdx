// Package gospantimeline implements timeline.Timeline with gospan spans and an
// in-memory projection. No collector, database, or process-global tracer is used.
package gospantimeline

import (
	"context"
	"errors"
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

// New begins one operation and returns a context carrying it. Cancellation of
// ctx does not finish the operation: the completion owner must call Finish with
// a context that permits collection. Business contexts retain their own lifetime.
func New(ctx context.Context, operation string, fields ...timeline.Field) (context.Context, timeline.Timeline, error) {
	sink := newProjection()
	// This writer only projects into memory. Blocking on its bounded queue
	// preserves stage boundaries without coupling producers to external IO.
	tracer, err := gospan.New(sink, gospan.WithBufferSize(128), gospan.WithBlockingPolicy())
	if err != nil {
		return ctx, nil, err
	}
	// Span IDs are local to a tracer. Never inherit a foreign gospan parent.
	rootCtx, root := tracer.Start(context.Background(), operation, attrs(fields)...)
	t := &recorder{tracer: tracer, root: root, rootCtx: rootCtx, sink: sink}
	return t.context(ctx, rootCtx), t, nil
}

func (t *recorder) context(ctx, spanCtx context.Context) context.Context {
	ctx = timeline.NewContext(ctx, t)
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
	spanCtx, span := t.tracer.Start(spanCtx, name, attrs(fields)...)
	t.active++
	return t.context(ctx, spanCtx), &stage{owner: t, span: span}
}

func (t *recorder) SetFields(fields ...timeline.Field) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished {
		t.root.SetAttrs(attrs(fields)...)
	}
}

func (s *stage) SetFields(fields ...timeline.Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if !s.ended {
		s.span.SetAttrs(attrs(fields)...)
	}
}

func (s *stage) End(err error, fields ...timeline.Field) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.ended {
		return
	}
	s.span.SetAttrs(attrs(fields)...)
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
	return t.sink.snapshot(err == nil), err
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
	return t.sink.snapshot(err == nil), err
}

func attrs(fields []timeline.Field) []slog.Attr {
	result := make([]slog.Attr, len(fields))
	for i, field := range fields {
		// Keep caller value types (slog otherwise normalizes int to int64).
		result[i] = slog.Any(field.Key, fieldValue{value: field.Value})
	}
	return result
}

type inertStage struct{}

type fieldValue struct{ value any }

func (inertStage) SetFields(...timeline.Field)  {}
func (inertStage) End(error, ...timeline.Field) {}
