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
	ctx := context.Background()
	tl, err := gospantimeline.New(ctx, t.Name(), "operation", timeline.Attribute{Key: "attempt", Value: 1})
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
	if err != nil || !(s.Collection.LocalFlushed && s.Collection.StoreRead) {
		t.Fatalf("snapshot: complete=%v err=%v", (s.Collection.LocalFlushed && s.Collection.StoreRead), err)
	}
	return s
}

func TestNestedAndOverlappingStages(t *testing.T) {
	ctx, tl := newTimeline(t)
	parentCtx, parent := timeline.BeginWithContext(ctx, tl, "prepare", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	_, child := timeline.BeginWithContext(parentCtx, tl, "workspace", timeline.WithParent(parent.ID()), timeline.WithStageActor(timeline.Actor{Name: "test"}))
	_, sibling := timeline.BeginWithContext(ctx, tl, "capacity", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	partial := snapshot(t, tl)
	if partial.Status != timeline.Running || len(partial.Stages) != 3 {
		t.Fatalf("running snapshot = %+v", partial)
	}
	byID := func(snapshot timeline.Snapshot, id timeline.StageID) timeline.Stage {
		t.Helper()
		for _, record := range snapshot.Stages {
			if record.ID == id {
				return record
			}
		}
		t.Fatalf("missing stage %s", id)
		return timeline.Stage{}
	}
	// Equal wall-clock timestamps are ordered by ID, not by creation order.
	p, c, s := byID(partial, parent.ID()), byID(partial, child.ID()), byID(partial, sibling.ID())
	if p.ParentID != "" || c.ParentID != p.ID || s.ParentID != "" {
		t.Fatalf("lost parent relationships: %+v", partial.Stages)
	}
	child.End(nil)
	sibling.End(nil)
	parent.End(nil)
	final, err := tl.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if byID(final, sibling.ID()).StartedAt.After(byID(final, child.ID()).FinishedAt) {
		t.Fatal("overlapping work was serialized in the projection")
	}
	if final.Stages[0].FinishedAt.IsZero() || final.Duration() <= 0 {
		t.Fatal("missing real interval")
	}
	if partial.Stages[0].Status != timeline.Running || !partial.Stages[0].FinishedAt.IsZero() {
		t.Fatal("a later finish mutated the earlier snapshot")
	}
}

func TestStageResultsAndDetachedAttributes(t *testing.T) {
	ctx, tl := newTimeline(t)
	attributes := []timeline.Attribute{{Key: "count", Value: 1}}
	_, failed := timeline.BeginWithContext(ctx, tl, "failed", timeline.WithStageActor(timeline.Actor{Name: "test"}), timeline.WithAttributes(attributes...))
	attributes[0].Value = 99
	failed.SetAttributes(timeline.Attribute{Key: "count", Value: 2})
	before := snapshot(t, tl)
	failure := errors.New("metadata commit failed")
	failed.End(failure)
	failed.End(nil)
	failed.SetAttributes(timeline.Attribute{Key: "count", Value: 3})
	_, canceled := timeline.BeginWithContext(ctx, tl, "canceled", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	canceled.End(fmt.Errorf("waiting: %w", context.DeadlineExceeded))
	final, err := tl.Finish(context.Background(), failure)
	if err != nil || final.Status != timeline.Failed || final.Error != failure.Error() {
		t.Fatalf("business failure must not become a collection error: %+v, %v", final, err)
	}
	if final.Stages[0].Status != timeline.Failed || final.Stages[0].Error != failure.Error() || final.Stages[1].Status != timeline.Canceled {
		t.Fatalf("lost stage result: %+v", final.Stages)
	}
	if string(before.Stages[0].Attributes["count"]) != "2" || string(final.Stages[0].Attributes["count"]) != "2" || string(final.Attributes["attempt"]) != "1" {
		t.Fatal("recorded attribute values changed")
	}
	final.Attributes["attempt"][0] = '4'
	final.Stages[0].Attributes["count"][0] = '5'
	final.Stages[0].Name = "changed"
	fresh := snapshot(t, tl)
	if string(fresh.Attributes["attempt"]) != "1" || string(fresh.Stages[0].Attributes["count"]) != "2" || fresh.Stages[0].Name != "failed" {
		t.Fatal("caller mutation reached retained records")
	}
}

func TestFinishRejectsActiveStagesThenFreezesFirstResult(t *testing.T) {
	ctx, tl := newTimeline(t)
	_, work := timeline.BeginWithContext(ctx, tl, "work", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	partial, err := tl.Finish(context.Background(), errors.New("premature result"))
	if !errors.Is(err, timeline.ErrActiveStages) || partial.Status != timeline.Running || !partial.FinishedAt.IsZero() {
		t.Fatalf("premature finish = %+v, %v", partial, err)
	}
	work.End(nil)
	first, err := tl.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	returnedCtx, ignored := timeline.BeginWithContext(ctx, tl, "too late", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	ignored.SetAttributes(timeline.Attribute{Key: "ignored", Value: true})
	ignored.End(errors.New("ignored"))
	second, err := tl.Finish(context.Background(), errors.New("too late"))
	if err != nil || returnedCtx != ctx || !reflect.DeepEqual(first, second) || !reflect.DeepEqual(first, snapshot(t, tl)) {
		t.Fatalf("finished snapshot changed: first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestCancellationDoesNotOwnOperationLifetime(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	tl, err := gospantimeline.New(requestCtx, t.Name(), "detached")
	if err != nil {
		t.Fatal(err)
	}
	stageCtx, stage := timeline.BeginWithContext(requestCtx, tl, "background", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	cancel()
	if stageCtx.Err() != context.Canceled {
		t.Fatal("stage context lost request cancellation")
	}
	if _, ok := timeline.FromContext(stageCtx); ok {
		t.Fatal("Begin implicitly bound a timeline to context")
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
	foreignCtx, parent := timeline.BeginWithContext(timeline.NewContext(ctx, first), first, "foreign", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	second, err := gospantimeline.New(ctx, t.Name()+"-second", "operation")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = second.Finish(context.Background(), nil) })
	childCtx, child := timeline.BeginWithContext(foreignCtx, second, "own", timeline.WithStageActor(timeline.Actor{Name: "test"}))
	child.End(nil)
	parent.End(nil)
	got := snapshot(t, second)
	if len(got.Stages) != 1 || got.Stages[0].ParentID != "" {
		t.Fatalf("foreign IDs contaminated timeline: %+v", got)
	}
	if owner, _ := timeline.FromContext(childCtx); owner != first {
		t.Fatal("Begin replaced the caller's optional context binding")
	}
}

func TestConcurrentStagesAndSnapshots(t *testing.T) {
	ctx, tl := newTimeline(t)
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			parentCtx, parent := timeline.BeginWithContext(ctx, tl, fmt.Sprintf("parent-%d", i), timeline.WithStageActor(timeline.Actor{Name: "test"}))
			_, child := timeline.BeginWithContext(parentCtx, tl, "child", timeline.WithParent(parent.ID()), timeline.WithStageActor(timeline.Actor{Name: "test"}))
			child.SetAttributes(timeline.Attribute{Key: "worker", Value: i})
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
	parents[""] = true
	for _, stage := range final.Stages {
		parents[stage.ID] = true
	}
	for _, stage := range final.Stages {
		if stage.Status != timeline.Succeeded || !parents[stage.ParentID] {
			t.Fatalf("unfinished or orphaned stage: %+v", stage)
		}
		parents[stage.ID] = true
	}
}

func TestConcurrentEndKeepsOneResult(t *testing.T) {
	ctx, tl := newTimeline(t)
	_, stage := timeline.BeginWithContext(ctx, tl, "contended", timeline.WithStageActor(timeline.Actor{Name: "test"}))
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
