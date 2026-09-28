package gospantimeline_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func newTimeline(t *testing.T) (context.Context, timeline.Timeline) {
	t.Helper()
	ctx, tl, err := gospantimeline.New(context.Background(), "operation", timeline.Field{Key: "attempt", Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := tl.Finish(ctx, nil); err != nil {
			t.Errorf("finish during cleanup: %v", err)
		}
	})
	return ctx, tl
}

func snapshot(t *testing.T, tl timeline.Timeline) timeline.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := tl.Snapshot(ctx)
	if err != nil || !s.Complete {
		t.Fatalf("snapshot: complete=%v err=%v", s.Complete, err)
	}
	return s
}

func TestNestedAndOverlappingStages(t *testing.T) {
	ctx, tl := newTimeline(t)
	parentCtx, parent := tl.Begin(ctx, "prepare")
	_, child := tl.Begin(parentCtx, "workspace")
	_, sibling := tl.Begin(ctx, "capacity")
	partial := snapshot(t, tl)
	if partial.Status != timeline.Running || len(partial.Stages) != 3 {
		t.Fatalf("running snapshot = %+v", partial)
	}
	p, c, s := partial.Stages[0], partial.Stages[1], partial.Stages[2]
	if p.ParentID != partial.ID || c.ParentID != p.ID || s.ParentID != partial.ID {
		t.Fatalf("lost parent relationships: %+v", partial.Stages)
	}
	child.End(nil)
	sibling.End(nil)
	parent.End(nil)
	final, err := tl.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if final.Stages[2].StartedAt.After(final.Stages[1].FinishedAt) {
		t.Fatal("overlapping work was serialized in the projection")
	}
	if final.Stages[0].FinishedAt.IsZero() || final.Duration() <= 0 {
		t.Fatal("missing real interval")
	}
	if partial.Stages[0].Status != timeline.Running || !partial.Stages[0].FinishedAt.IsZero() {
		t.Fatal("a later finish mutated the earlier snapshot")
	}
}

func TestStageResultsAndDetachedFields(t *testing.T) {
	ctx, tl := newTimeline(t)
	fields := []timeline.Field{{Key: "count", Value: 1}}
	_, failed := tl.Begin(ctx, "failed", fields...)
	fields[0].Value = 99
	failed.SetFields(timeline.Field{Key: "count", Value: 2})
	before := snapshot(t, tl)
	failure := errors.New("metadata commit failed")
	failed.End(failure)
	failed.End(nil)
	failed.SetFields(timeline.Field{Key: "count", Value: 3})
	_, canceled := tl.Begin(ctx, "canceled")
	canceled.End(fmt.Errorf("waiting: %w", context.DeadlineExceeded))
	final, err := tl.Finish(context.Background(), failure)
	if err != nil || final.Status != timeline.Failed || final.Error != failure.Error() {
		t.Fatalf("business failure must not become a collection error: %+v, %v", final, err)
	}
	if final.Stages[0].Status != timeline.Failed || final.Stages[0].Error != failure.Error() || final.Stages[1].Status != timeline.Canceled {
		t.Fatalf("lost stage result: %+v", final.Stages)
	}
	if before.Stages[0].Fields[0].Value != 2 || final.Stages[0].Fields[0].Value != 2 || final.Fields[0].Value != 1 {
		t.Fatal("field values changed or their Go types were normalized")
	}
	final.Fields[0].Value = 44
	final.Stages[0].Fields[0].Value = 55
	final.Stages[0].Name = "changed"
	fresh := snapshot(t, tl)
	if fresh.Fields[0].Value != 1 || fresh.Stages[0].Fields[0].Value != 2 || fresh.Stages[0].Name != "failed" {
		t.Fatal("caller mutation reached retained records")
	}
}

func TestFinishRejectsActiveStagesThenFreezesFirstResult(t *testing.T) {
	ctx, tl := newTimeline(t)
	_, work := tl.Begin(ctx, "work")
	partial, err := tl.Finish(context.Background(), errors.New("premature result"))
	if !errors.Is(err, timeline.ErrActiveStages) || partial.Status != timeline.Running || !partial.FinishedAt.IsZero() {
		t.Fatalf("premature finish = %+v, %v", partial, err)
	}
	work.End(nil)
	first, err := tl.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	returnedCtx, ignored := tl.Begin(ctx, "too late")
	ignored.SetFields(timeline.Field{Key: "ignored", Value: true})
	ignored.End(errors.New("ignored"))
	second, err := tl.Finish(context.Background(), errors.New("too late"))
	if err != nil || returnedCtx != ctx || !reflect.DeepEqual(first, second) || !reflect.DeepEqual(first, snapshot(t, tl)) {
		t.Fatalf("finished snapshot changed: first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestCancellationDoesNotOwnOperationLifetime(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	ctx, tl, err := gospantimeline.New(requestCtx, "detached")
	if err != nil {
		t.Fatal(err)
	}
	stageCtx, stage := tl.Begin(ctx, "background")
	cancel()
	if stageCtx.Err() != context.Canceled {
		t.Fatal("stage context lost request cancellation")
	}
	if got, ok := timeline.FromContext(stageCtx); !ok || got != tl {
		t.Fatal("stage context lost timeline identity")
	}
	if got := snapshot(t, tl); got.Status != timeline.Running || got.Stages[0].Status != timeline.Running {
		t.Fatal("request cancellation prematurely finished the operation")
	}
	done := make(chan error, 1)
	go func() {
		stage.End(nil)
		_, err := tl.Finish(context.Background(), nil)
		done <- err
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestForeignTimelineContextCreatesIndependentRoot(t *testing.T) {
	ctx, first := newTimeline(t)
	foreignCtx, parent := first.Begin(ctx, "foreign")
	_, second := newTimeline(t)
	childCtx, child := second.Begin(foreignCtx, "own")
	child.End(nil)
	parent.End(nil)
	got := snapshot(t, second)
	if len(got.Stages) != 1 || got.Stages[0].ParentID != got.ID {
		t.Fatalf("foreign IDs contaminated timeline: %+v", got)
	}
	if owner, _ := timeline.FromContext(childCtx); owner != second {
		t.Fatal("child carries the wrong timeline")
	}
}

func TestConcurrentStagesAndSnapshots(t *testing.T) {
	ctx, tl := newTimeline(t)
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parentCtx, parent := tl.Begin(ctx, fmt.Sprintf("parent-%d", i))
			_, child := tl.Begin(parentCtx, "child")
			child.SetFields(timeline.Field{Key: "worker", Value: i})
			child.End(nil)
			_ = snapshot(t, tl)
			parent.End(nil)
		}()
	}
	wg.Wait()
	final, err := tl.Finish(context.Background(), nil)
	if err != nil || len(final.Stages) != 400 {
		t.Fatalf("lost concurrent stages: count=%d err=%v", len(final.Stages), err)
	}
	parents := make(map[timeline.StageID]bool)
	parents[final.ID] = true
	for _, stage := range final.Stages {
		if stage.Status != timeline.Succeeded || !parents[stage.ParentID] {
			t.Fatalf("unfinished or orphaned stage: %+v", stage)
		}
		parents[stage.ID] = true
	}
}

func TestConcurrentEndKeepsOneResult(t *testing.T) {
	ctx, tl := newTimeline(t)
	_, stage := tl.Begin(ctx, "contended")
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stage.End(fmt.Errorf("failure-%d", i))
		}()
	}
	wg.Wait()
	first := snapshot(t, tl)
	stage.End(nil)
	second := snapshot(t, tl)
	if first.Stages[0].Status != timeline.Failed || !reflect.DeepEqual(first.Stages, second.Stages) {
		t.Fatal("concurrent End changed the first result")
	}
}
