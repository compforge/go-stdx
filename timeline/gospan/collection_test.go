package gospantimeline

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/akmadian/gospan"
	"github.com/compforge/go-stdx/timeline"
)

// Gate the writer, not the projection lock: this models delayed event delivery
// while leaving snapshots free to return the facts collected so far.
type gatedSink struct {
	*projection
	gate <-chan struct{}
}

func (s gatedSink) WriteBatch(batch gospan.Batch) error {
	<-s.gate
	return s.projection.WriteBatch(batch)
}

func TestCollectionDeadlineAndFinishRetry(t *testing.T) {
	sink := newProjection()
	gate := make(chan struct{})
	tracer, err := gospan.New(gatedSink{projection: sink, gate: gate}, gospan.WithBlockingPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Also release the writer if an assertion exits the test early.
		select {
		case <-gate:
		default:
			close(gate)
		}
		_ = tracer.Close(context.Background())
	}()
	rootCtx, root := tracer.Start(context.Background(), "delayed")
	tl := &recorder{tracer: tracer, root: root, rootCtx: rootCtx, sink: sink}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	partial, err := tl.Snapshot(canceled)
	if !errors.Is(err, context.Canceled) || partial.Complete {
		t.Fatalf("uncollected snapshot claimed success: %+v err=%v", partial, err)
	}
	operationErr := errors.New("business failure")
	partial, err = tl.Finish(canceled, operationErr)
	if !errors.Is(err, context.Canceled) || partial.Complete {
		t.Fatalf("uncollected finish claimed success: %+v err=%v", partial, err)
	}
	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	final, err := tl.Finish(ctx, nil)
	if err != nil || !final.Complete || final.Status != timeline.Failed || final.Error != operationErr.Error() {
		t.Fatalf("retry lost the first finish result: %+v err=%v", final, err)
	}
	again, err := tl.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(final, again) {
		t.Fatal("final snapshot was not stable after writer shutdown")
	}
}

func TestCheckpointAttributeDoesNotCollideWithUserFields(t *testing.T) {
	ctx, tl, err := New(context.Background(), "fields", timeline.Field{Key: checkpointKey, Value: uint64(999)})
	if err != nil {
		t.Fatal(err)
	}
	_, stage := tl.Begin(ctx, "work", timeline.Field{Key: checkpointKey, Value: "user value"})
	stage.End(nil)
	for range 3 {
		if s, err := tl.Snapshot(context.Background()); err != nil || len(s.Fields) != 1 || s.Fields[0].Value != uint64(999) {
			t.Fatalf("checkpoint leaked into user fields: %+v err=%v", s, err)
		}
	}
	s, err := tl.Finish(context.Background(), nil)
	if err != nil || s.Stages[0].Fields[0].Value != "user value" {
		t.Fatalf("user field was consumed as a checkpoint: %+v err=%v", s, err)
	}
}

func TestConstructorIsolatesForeignGospanContext(t *testing.T) {
	foreign, err := gospan.New(newProjection())
	if err != nil {
		t.Fatal(err)
	}
	// Use no attributes: this foreign tracer only tests context isolation.
	ctx, root := foreign.Start(context.Background(), "foreign")
	defer func() { root.End(); _ = foreign.Close(context.Background()) }()
	ctx, tl, err := New(ctx, "own")
	if err != nil {
		t.Fatal(err)
	}
	_, stage := tl.Begin(ctx, "child")
	stage.End(nil)
	s, err := tl.Finish(context.Background(), nil)
	if err != nil || s.Operation != "own" || len(s.Stages) != 1 || s.Stages[0].ParentID != s.ID {
		t.Fatalf("foreign tracer corrupted root: %+v err=%v", s, err)
	}
	stats := tl.(*recorder).tracer.Stats()
	if stats.Dropped != 0 || stats.WriteErrors != 0 || stats.SpansInFlight != 0 || stats.TracesInFlight != 0 {
		t.Fatalf("unfinished or lost span events: %+v", stats)
	}
}

func TestProjectionCopiesBatchFields(t *testing.T) {
	p := newProjection()
	attrs := []slog.Attr{slog.Any("key", fieldValue{value: "before"})}
	if err := p.WriteBatch(gospan.Batch{Events: []gospan.Event{{Kind: gospan.EventStart, SpanID: 1, Name: "root", StartNS: time.Now().UnixNano(), Attrs: attrs}}}); err != nil {
		t.Fatal(err)
	}
	attrs[0] = slog.Any("key", fieldValue{value: "reused buffer"})
	if got := p.snapshot(true).Fields[0].Value; got != "before" {
		t.Fatalf("retained gospan batch memory: %v", got)
	}
}
