package timeline_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func TestGlobalOperationWithoutPassingHandles(t *testing.T) {
	ctx := context.Background()
	m := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	defaultManager(t, m)
	if _, err := timeline.Start(ctx, "task", "startup"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.SetAttributes("task", timeline.Attribute{Key: "tenant", Value: "a"}); err != nil {
		t.Fatal(err)
	}
	parentCtx, parent, err := timeline.BeginContext(ctx, "task", "prepare")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := timeline.BeginContext(parentCtx, "task", "files", timeline.WithStageActor(timeline.Actor{ID: "worker"})); err != nil {
		t.Fatal(err)
	}
	if err := timeline.SetStageAttributes("task", "files", timeline.Attribute{Key: "bytes", Value: 123}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "files", nil, timeline.WithCode("Cached")); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "prepare", nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := timeline.Record("task", timeline.Stage{ID: "observed", Name: "image_pull", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := timeline.Finish(ctx, "task", nil)
	if err != nil || snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != 3 || !snapshot.Collection.LocalFlushed || !snapshot.Collection.StoreRead {
		t.Fatalf("finish: %+v %v", snapshot, err)
	}
	if string(snapshot.Attributes["tenant"]) != `"a"` {
		t.Fatal("root attributes missing")
	}
	for _, stage := range snapshot.Stages {
		if stage.Name == "files" && (stage.ParentID != parent.ID() || stage.Actor.ID != "worker" || stage.Code != "Cached" || string(stage.Attributes["bytes"]) != "123") {
			t.Fatalf("child metadata lost: %+v", stage)
		}
	}
	if got := m.Stats(); got.ActiveOperations != 0 || got.ActiveStages != 0 {
		t.Fatalf("live index retained history: %+v", got)
	}
	if _, err := timeline.Begin("task", "late_observation"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "late_observation", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = timeline.Capture(ctx, "task")
	if err != nil || snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != 4 {
		t.Fatalf("late stage: %+v %v", snapshot, err)
	}
	if _, err := timeline.Finish(ctx, "task", nil); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("released coordinator reused: %v", err)
	}
}

func TestNamedStageAmbiguityAndReuse(t *testing.T) {
	ctx := context.Background()
	m := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	defaultManager(t, m)
	first, err := timeline.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	second, err := timeline.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "work", nil); !errors.Is(err, timeline.ErrAmbiguousStage) {
		t.Fatalf("ambiguous End: %v", err)
	}
	if err := timeline.SetStageAttributes("task", "work", timeline.Attribute{Key: "x", Value: 1}); !errors.Is(err, timeline.ErrAmbiguousStage) {
		t.Fatalf("ambiguous attributes: %v", err)
	}
	first.End(nil)
	if err := timeline.End("task", "work", nil); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "work", nil); !errors.Is(err, timeline.ErrStageNotFound) {
		t.Fatalf("ended stage stayed addressable: %v", err)
	}
	third, err := timeline.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if third.ID() == first.ID() || third.ID() == second.ID() {
		t.Fatal("new work reused an old stage identity")
	}
	if err := timeline.End("task", "work", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := timeline.Capture(ctx, "task")
	if err != nil || len(snapshot.Stages) != 3 || len(snapshot.RunningStages()) != 0 {
		t.Fatalf("stage history: %+v %v", snapshot, err)
	}
	if got := m.Stats(); got.ActiveOperations != 0 || got.ActiveStages != 0 {
		t.Fatalf("participant-only index leaked: %+v", got)
	}
}

func TestIDFinishRetainsCoordinatorAndFirstResultOnFailure(t *testing.T) {
	ctx := context.Background()
	var available atomic.Bool
	unavailable := errors.New("store unavailable")
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if !available.Load() {
			return unavailable
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{})
	defaultManager(t, m)
	if _, err := timeline.Start(ctx, "task", "startup"); !errors.Is(err, unavailable) {
		t.Fatalf("start: %v", err)
	}
	if r, err := timeline.Start(ctx, "task", "startup"); r != nil || !errors.Is(err, timeline.ErrAlreadyStarted) {
		t.Fatalf("duplicate start: %v %v", r, err)
	}
	businessErr := errors.New("work failed")
	if _, err := timeline.Finish(ctx, "task", businessErr); !errors.Is(err, unavailable) {
		t.Fatalf("finish: %v", err)
	}
	if m.Stats().ActiveOperations != 1 {
		t.Fatal("failed finish lost retry ownership")
	}
	available.Store(true)
	snapshot, err := timeline.Finish(ctx, "task", nil)
	if err != nil || snapshot.Status != timeline.Failed || snapshot.Error != businessErr.Error() {
		t.Fatalf("retry changed result: %+v %v", snapshot, err)
	}
	if m.Stats().ActiveOperations != 0 {
		t.Fatal("successful retry leaked coordinator")
	}
}

func TestLocalManagersShareDataNotLiveOwnership(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	owner := manager(t, store, timeline.ManagerConfig{})
	participant := manager(t, store, timeline.ManagerConfig{})
	defaultManager(t, owner)
	if _, err := timeline.Start(ctx, "task", "startup"); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Begin("task", "owner_stage"); err != nil {
		t.Fatal(err)
	}
	timeline.SetDefaultManager(participant)
	if _, err := timeline.Finish(ctx, "task", nil); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("remote completion acquired: %v", err)
	}
	if err := timeline.SetAttributes("task", timeline.Attribute{Key: "x", Value: 1}); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("remote root attributes acquired: %v", err)
	}
	if err := timeline.End("task", "owner_stage", nil); !errors.Is(err, timeline.ErrStageNotFound) {
		t.Fatalf("remote stage acquired: %v", err)
	}
	if _, err := timeline.Begin("task", "participant_stage"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("task", "participant_stage", nil); err != nil {
		t.Fatal(err)
	}
	if err := timeline.Flush(ctx, "task"); err != nil {
		t.Fatal(err)
	}
	timeline.SetDefaultManager(owner)
	if err := timeline.End("task", "owner_stage", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := timeline.Finish(ctx, "task", nil)
	if err != nil || len(snapshot.Stages) != 2 {
		t.Fatalf("shared result: %+v %v", snapshot, err)
	}
}

func TestActiveLimitsReleaseAndLateStageOwnership(t *testing.T) {
	ctx := context.Background()
	m := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{MaxActiveOperations: 1, MaxActiveStages: 1})
	defaultManager(t, m)
	if _, err := timeline.Start(ctx, "task", "startup"); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Start(ctx, "other", "startup"); !errors.Is(err, timeline.ErrActiveOperationLimit) {
		t.Fatalf("active operations unbounded: %v", err)
	}
	stage, err := timeline.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Begin("task", "another"); !errors.Is(err, timeline.ErrActiveStageLimit) {
		t.Fatalf("active stages unbounded: %v", err)
	}
	if _, err := timeline.Finish(ctx, "task", nil); err != nil {
		t.Fatal(err)
	}
	if m.Stats().ActiveStages != 1 {
		t.Fatal("root finish revoked live stage")
	}
	if err := timeline.End("task", "work", nil); err != nil {
		t.Fatal(err)
	}
	if got := m.Stats(); got.ActiveOperations != 0 || got.ActiveStages != 0 {
		t.Fatalf("ended stage leaked: %+v", got)
	}
	if _, err := timeline.Start(ctx, "other", "startup"); err != nil {
		t.Fatal(err)
	}
	stage, err = timeline.Begin("other", "abandoned")
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.Release("other"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("other", "abandoned", nil); !errors.Is(err, timeline.ErrStageNotFound) {
		t.Fatalf("released stage still indexed: %v", err)
	}
	stage.End(nil) // Retained handles can still report real completion.
	if err := timeline.Flush(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	doc, err := timeline.Read(ctx, "other")
	if err != nil || doc.Status != timeline.Running || len(doc.Stages) != 1 || doc.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("Release changed business data: %+v %v", doc, err)
	}
}

func TestFlushIDDoesNotVisitOtherIDs(t *testing.T) {
	ctx := context.Background()
	blockWorker := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int32
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if id == "blocked" {
			if calls.Add(1) == 1 {
				close(entered)
			}
			select {
			case <-blockWorker:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{ExportTimeout: time.Second})
	defaultManager(t, m)
	if _, err := timeline.Begin("blocked", "wait"); err != nil {
		t.Fatal(err)
	}
	defer close(blockWorker)
	waitSignal(t, entered)
	if _, err := timeline.Begin("selected", "work"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("selected", "work", nil); err != nil {
		t.Fatal(err)
	}
	flushCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := timeline.Flush(flushCtx, "selected"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("ID flush visited another operation")
	}
	doc, err := timeline.Read(ctx, "selected")
	if err != nil || len(doc.Stages) != 1 || doc.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("selected writer not flushed: %+v %v", doc, err)
	}
}

func TestConcurrentNamedStagesAndStartAdmission(t *testing.T) {
	ctx := context.Background()
	m := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	defaultManager(t, m)
	var starts atomic.Int32
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := timeline.Start(ctx, "task", "startup"); err == nil {
				starts.Add(1)
			} else if !errors.Is(err, timeline.ErrAlreadyStarted) {
				t.Error(err)
			}
			name := fmt.Sprintf("stage-%d", i)
			if _, err := timeline.Begin("task", name); err != nil {
				t.Error(err)
				return
			}
			if err := timeline.SetStageAttributes("task", name, timeline.Attribute{Key: "worker", Value: i}); err != nil {
				t.Error(err)
			}
			if err := timeline.End("task", name, nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatalf("admitted %d coordinators", starts.Load())
	}
	snapshot, err := timeline.Finish(ctx, "task", nil)
	if err != nil || len(snapshot.Stages) != 24 || len(snapshot.RunningStages()) != 0 {
		t.Fatalf("parallel result: %+v %v", snapshot, err)
	}
}

func TestGlobalIDErrors(t *testing.T) {
	ctx := context.Background()
	defaultManager(t, nil)
	if _, err := timeline.Begin("task", "work"); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatal(err)
	}
	if got, _, err := timeline.BeginContext(ctx, "task", "work"); got != ctx || !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatal(err)
	}
	for _, err := range []error{
		timeline.Record("task", timeline.Stage{}), timeline.SetAttributes("task"), timeline.SetStageAttributes("task", "work"),
		timeline.End("task", "work", nil), timeline.Flush(ctx, "task"), timeline.Release("task"),
	} {
		if !errors.Is(err, timeline.ErrNoDefaultManager) {
			t.Fatal(err)
		}
	}
	if _, err := timeline.Capture(ctx, "task"); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatal(err)
	}
	if _, err := timeline.Finish(ctx, "task", nil); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatal(err)
	}
	m := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	timeline.SetDefaultManager(m)
	if _, err := timeline.Begin("task", "work", timeline.WithAttributes(timeline.Attribute{Key: "bad", Value: make(chan int)})); !errors.Is(err, timeline.ErrInvalidAttribute) {
		t.Fatalf("invalid stage: %v", err)
	}
	if m.Stats().ActiveOperations != 0 {
		t.Fatal("invalid begin leaked live state")
	}
	if _, err := timeline.Capture(ctx, "missing"); !errors.Is(err, timeline.ErrNotFound) {
		t.Fatal(err)
	}
	if err := timeline.Flush(ctx, ""); !errors.Is(err, timeline.ErrEmptyID) {
		t.Fatal(err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Begin("task", "work"); !errors.Is(err, timeline.ErrManagerClosed) {
		t.Fatal(err)
	}
}
