package manager_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestSixEntryPointsAndOptionalBoundaries(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	defaultManager(t, m)
	if _, err := managed.Begin("task", "prepare"); err != nil {
		t.Fatal(err)
	}
	if err := managed.End("task", "prepare", nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := managed.Record("task", timeline.Stage{ID: "observed", Name: "pull", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := managed.Read(ctx, "task")
	if err != nil || len(snapshot.Stages) != 2 || snapshot.Status != timeline.Unknown || !snapshot.StartedAt.IsZero() || !snapshot.FinishedAt.IsZero() {
		t.Fatalf("implicit operation state: %+v %v", snapshot, err)
	}
	if err := managed.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = managed.Read(ctx, "task")
	if err != nil || snapshot.Status != timeline.Succeeded || !snapshot.StartedAt.IsZero() {
		t.Fatalf("finish without start: %+v %v", snapshot, err)
	}
	end := snapshot.FinishedAt
	if err := managed.Start("task", "startup", timeline.Attribute{Key: "tenant", Value: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := managed.Start("task", "startup", timeline.Attribute{Key: "tenant", Value: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := managed.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := managed.Begin("task", "late"); err != nil {
		t.Fatal(err)
	}
	if err := managed.End("task", "late", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = managed.Read(ctx, "task")
	if err != nil || snapshot.StartedAt.IsZero() || !snapshot.FinishedAt.Equal(end) || len(snapshot.Stages) != 3 || snapshot.Operation != "startup" {
		t.Fatalf("late facts: %+v %v", snapshot, err)
	}
	if got := m.Stats(); got.CachedTimelines != 1 || got.ActiveStages != 0 {
		t.Fatalf("Finish changed cache lifetime: %+v", got)
	}
	if err := managed.Start("task", "different"); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatal(err)
	}
	if err := managed.Finish("task", errors.New("different")); !errors.Is(err, timelinestore.ErrConflict) {
		t.Fatal(err)
	}
}

func TestNamedStageAmbiguityParentAndReuse(t *testing.T) {
	ctx := context.Background()
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	first, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.End("task", "work", nil); !errors.Is(err, managed.ErrAmbiguousStage) {
		t.Fatal(err)
	}
	if err := first.End(nil); err != nil {
		t.Fatal(err)
	}
	if err := m.End("task", "work", nil); err != nil {
		t.Fatal(err)
	}
	if err := m.End("task", "work", nil); !errors.Is(err, managed.ErrStageNotFound) {
		t.Fatal(err)
	}
	parentCtx, third, err := m.BeginContext(ctx, "task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if third.ID() == first.ID() || third.ID() == second.ID() {
		t.Fatal("reused old identity")
	}
	_, child, err := m.BeginContext(parentCtx, "task", "child", timeline.WithStageActor(timeline.Actor{ID: "worker"}))
	if err != nil {
		t.Fatal(err)
	}
	if err := child.SetAttributes(timeline.Attribute{Key: "bytes", Value: 123}); err != nil {
		t.Fatal(err)
	}
	if err := child.End(nil, timeline.WithCode("Cached")); err != nil {
		t.Fatal(err)
	}
	third.End(nil)
	snapshot, err := m.Read(ctx, "task")
	if err != nil || len(snapshot.Stages) != 4 || len(snapshot.RunningStages()) != 0 {
		t.Fatalf("snapshot: %+v %v", snapshot, err)
	}
	for _, stage := range snapshot.Stages {
		if stage.Name == "child" && (stage.ParentID != third.ID() || stage.Actor.ID != "worker" || stage.Code != "Cached" || string(stage.Attributes["bytes"]) != "123") {
			t.Fatalf("child: %+v", stage)
		}
	}
}

func TestBoundariesDoNotWaitForStore(t *testing.T) {
	store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	unavailable := errors.New("store unavailable")
	var available atomic.Bool
	store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		if !available.Load() {
			return unavailable
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, store, managed.Config{})
	if err := m.Start("task", "startup"); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("work failed")
	if err := m.Finish("task", failure); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(context.Background(), "task"); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	available.Store(true)
	snapshot, err := m.Read(context.Background(), "task")
	if err != nil || snapshot.Status != timeline.Failed || snapshot.Error != failure.Error() {
		t.Fatalf("retry: %+v %v", snapshot, err)
	}
	if err := m.Start("start-only", "running"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = m.Read(context.Background(), "start-only")
	if err != nil || snapshot.Status != timeline.Unknown || !snapshot.FinishedAt.IsZero() {
		t.Fatalf("invented outcome: %+v %v", snapshot, err)
	}
}

func TestManagersShareFactsNotStageHandles(t *testing.T) {
	ctx := context.Background()
	store := timelinestore.NewMemoryStore()
	first, second := newManager(t, store, managed.Config{}), newManager(t, store, managed.Config{})
	if err := first.Start("task", "startup"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Begin("task", "first"); err != nil {
		t.Fatal(err)
	}
	if err := second.End("task", "first", nil); !errors.Is(err, managed.ErrStageNotFound) {
		t.Fatal(err)
	}
	if err := second.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.End("task", "first", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.Read(ctx, "task")
	if err != nil || snapshot.Status != timeline.Succeeded || snapshot.StartedAt.IsZero() || len(snapshot.Stages) != 1 {
		t.Fatalf("cross-process boundaries: %+v %v", snapshot, err)
	}
}

func TestConcurrentNamedStagesAndIdempotentStart(t *testing.T) {
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.Start("task", "startup"); err != nil {
				t.Error(err)
			}
			name := fmt.Sprintf("stage-%d", i)
			if _, err := m.Begin("task", name, timeline.WithAttributes(timeline.Attribute{Key: "worker", Value: i})); err != nil {
				t.Error(err)
				return
			}
			if err := m.End("task", name, nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := m.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.Read(context.Background(), "task")
	if err != nil || len(snapshot.Stages) != 24 || len(snapshot.RunningStages()) != 0 {
		t.Fatalf("parallel: %+v %v", snapshot, err)
	}
}

func TestReadDoesNotVisitOtherIDs(t *testing.T) {
	store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
		if id == "blocked" {
			if calls.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := newManager(t, store, managed.Config{ExportTimeout: time.Second})
	if _, err := m.Begin("blocked", "wait"); err != nil {
		t.Fatal(err)
	}
	defer close(release)
	waitSignal(t, entered)
	if _, err := m.Begin("selected", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.End("selected", "work", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	snapshot, err := m.Read(ctx, "selected")
	if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Status != timeline.Succeeded || calls.Load() != 1 {
		t.Fatalf("ID isolation: %+v %v", snapshot, err)
	}
}
