package gospantimeline

import (
	"context"
	"encoding/json"
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
	sink := newProjection(t.Name())
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
	registry := timeline.NewRegistry(func(context.Context, string, string, ...timeline.Attribute) (timeline.Timeline, error) {
		imports, _ := timeline.New(t.Name())
		return &recorder{tracer: tracer, root: root, rootCtx: rootCtx, sink: sink, imports: imports, stages: make(map[timeline.StageID]*stage)}, nil
	})
	tl, err := registry.Create(context.Background(), t.Name(), "delayed")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	partial, err := tl.Snapshot(canceled)
	if !errors.Is(err, context.Canceled) || (partial.Collection.LocalFlushed && partial.Collection.StoreRead) || partial.ID != tl.ID() {
		t.Fatalf("uncollected snapshot claimed success: %+v err=%v", partial, err)
	}
	_, stage := timeline.BeginContext(context.Background(), tl, "active")
	if _, err := tl.Finish(canceled, nil); !errors.Is(err, timeline.ErrActiveStages) || !errors.Is(err, context.Canceled) {
		t.Fatalf("active stage and collection errors = %v", err)
	}
	if got, ok := registry.Lookup(tl.ID()); !ok || got != tl {
		t.Fatal("active timeline was removed")
	}
	stage.End(nil)
	operationErr := errors.New("business failure")
	partial, err = tl.Finish(canceled, operationErr)
	if !errors.Is(err, context.Canceled) || (partial.Collection.LocalFlushed && partial.Collection.StoreRead) {
		t.Fatalf("uncollected finish claimed success: %+v err=%v", partial, err)
	}
	if _, ok := registry.Lookup(tl.ID()); ok {
		t.Fatal("sealed timeline retained after collection timeout")
	}
	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	final, err := tl.Finish(ctx, nil)
	if err != nil || !(final.Collection.LocalFlushed && final.Collection.StoreRead) || final.Status != timeline.Failed || final.Error != operationErr.Error() {
		t.Fatalf("retry lost the first finish result: %+v err=%v", final, err)
	}
	again, err := tl.Snapshot(ctx)
	if err != nil || !reflect.DeepEqual(final, again) {
		t.Fatal("final snapshot was not stable after writer shutdown")
	}
}

func TestCheckpointAttributeDoesNotCollideWithUserAttributes(t *testing.T) {
	ctx := context.Background()
	tl, err := New(ctx, t.Name(), "attributes", timeline.Attribute{Key: checkpointKey, Value: uint64(999)})
	if err != nil {
		t.Fatal(err)
	}
	_, stage := timeline.BeginContext(ctx, tl, "work", timeline.WithAttributes(timeline.Attribute{Key: checkpointKey, Value: "user value"}))
	stage.End(nil)
	for range 3 {
		if s, err := tl.Snapshot(context.Background()); err != nil || len(s.Attributes) != 1 || string(s.Attributes[checkpointKey]) != "999" {
			t.Fatalf("checkpoint leaked into user attributes: %+v err=%v", s, err)
		}
	}
	s, err := tl.Finish(context.Background(), nil)
	if err != nil || string(s.Stages[0].Attributes[checkpointKey]) != `"user value"` {
		t.Fatalf("user attribute was consumed as a checkpoint: %+v err=%v", s, err)
	}
}

func TestConstructorIsolatesForeignGospanContext(t *testing.T) {
	foreign, err := gospan.New(newProjection(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	// Use no attributes: this foreign tracer only tests context isolation.
	ctx, root := foreign.Start(context.Background(), "foreign")
	defer func() { root.End(); _ = foreign.Close(context.Background()) }()
	tl, err := New(ctx, t.Name(), "own")
	if err != nil {
		t.Fatal(err)
	}
	_, stage := timeline.BeginContext(ctx, tl, "child")
	stage.End(nil)
	s, err := tl.Finish(context.Background(), nil)
	if err != nil || s.Operation != "own" || len(s.Stages) != 1 || s.Stages[0].ParentID != s.RootStageID {
		t.Fatalf("foreign tracer corrupted root: %+v err=%v", s, err)
	}
	stats := tl.(*recorder).tracer.Stats()
	if stats.Dropped != 0 || stats.WriteErrors != 0 || stats.SpansInFlight != 0 || stats.TracesInFlight != 0 {
		t.Fatalf("unfinished or lost span events: %+v", stats)
	}
}

func TestProjectionCopiesBatchAttributes(t *testing.T) {
	p := newProjection(t.Name())
	attrs := []slog.Attr{slog.Any("key", attributeValue{value: json.RawMessage(`"before"`)})}
	if err := p.WriteBatch(gospan.Batch{Events: []gospan.Event{{Kind: gospan.EventStart, SpanID: 1, Name: "root", StartNS: time.Now().UnixNano(), Attrs: attrs}}}); err != nil {
		t.Fatal(err)
	}
	attrs[0] = slog.Any("key", attributeValue{value: json.RawMessage(`"reused buffer"`)})
	if got := p.snapshot(true).Attributes["key"]; string(got) != `"before"` {
		t.Fatalf("retained gospan batch memory: %v", got)
	}
}
