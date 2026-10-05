package timeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func TestSnapshotQueriesPreserveParallelFacts(t *testing.T) {
	at := time.Now()
	s := timeline.Snapshot{Stages: []timeline.Stage{
		{ID: "1", Status: timeline.Running},
		{ID: "2", Status: timeline.Failed, FinishedAt: at.Add(time.Second)},
		{ID: "3", Status: timeline.Succeeded, FinishedAt: at.Add(3 * time.Second)},
		{ID: "4", Status: timeline.Running},
		{ID: "5", Status: timeline.Canceled, FinishedAt: at},
	}}
	if got := s.RunningStages(); len(got) != 2 || got[0].ID != "1" || got[1].ID != "4" {
		t.Fatalf("running = %+v", got)
	}
	if got, ok := s.LatestFailedStage(); !ok || got.ID != "2" {
		t.Fatalf("failed = %+v, %v", got, ok)
	}
	if _, ok := (timeline.Snapshot{}).LatestFailedStage(); ok {
		t.Fatal("empty snapshot has failure")
	}
}

func TestAttributeValueLastValueAndType(t *testing.T) {
	attributes := map[string]json.RawMessage{"key": json.RawMessage(`42`)}
	if v, ok := timeline.AttributeValue[int](attributes, "key"); !ok || v != 42 {
		t.Fatalf("value=%v ok=%v", v, ok)
	}
	if _, ok := timeline.AttributeValue[string](attributes, "key"); ok {
		t.Fatal("must not fall back to an older value with a matching type")
	}
	if _, ok := timeline.AttributeValue[int](attributes, "missing"); ok {
		t.Fatal("missing attribute is present")
	}
}

func TestFinalAttributesAndResultAreAtomic(t *testing.T) {
	ctx := context.Background()
	tl, err := gospantimeline.New(ctx, t.Name(), "operation")
	if err != nil {
		t.Fatal(err)
	}
	tl.SetAttributes(timeline.Attribute{Key: "runtime", Value: "pod"})
	_, stage := timeline.BeginContext(ctx, tl, "work")
	var wg sync.WaitGroup
	for _, value := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stage.End(errors.New(value), timeline.WithCode(value), timeline.WithEndAttributes(timeline.Attribute{Key: "result", Value: value}))
		}()
	}
	wg.Wait()
	final, err := tl.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r := final.Stages[0]
	if v, _ := timeline.AttributeValue[string](r.Attributes, "result"); v != r.Error || r.Code != r.Error {
		t.Fatalf("mixed winners: %+v", r)
	}
	tl.SetAttributes(timeline.Attribute{Key: "runtime", Value: "bed"})
	stage.End(nil, timeline.WithEndAttributes(timeline.Attribute{Key: "result", Value: "late"}))
	again, err := tl.Snapshot(context.Background())
	if err != nil || !reflect.DeepEqual(final, again) {
		t.Fatalf("final mutated: %+v, %v", again, err)
	}
	if v, _ := timeline.AttributeValue[string](final.Attributes, "runtime"); v != "pod" {
		t.Fatalf("runtime=%q", v)
	}
}

func TestNoopRetainsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, stage := timeline.BeginContext(ctx, timeline.Noop("disabled-operation"), "disabled")
	if got != ctx || got.Err() != context.Canceled {
		t.Fatal("changed context")
	}
	stage.End(nil)
	snapshot, err := timeline.Noop("disabled-operation").Finish(ctx, nil)
	if err != nil || snapshot.ID != "disabled-operation" || !(snapshot.Collection.LocalFlushed && snapshot.Collection.StoreRead) || len(snapshot.Stages) != 0 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}
