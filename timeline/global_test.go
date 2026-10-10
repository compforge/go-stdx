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

func defaultManager(t *testing.T, m *timeline.Manager) {
	t.Helper()
	previous := timeline.SetDefaultManager(m)
	t.Cleanup(func() { timeline.SetDefaultManager(previous) })
}

func TestGlobalRequiresExplicitSetup(t *testing.T) {
	defaultManager(t, nil)
	if _, err := timeline.For("task"); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatalf("For: %v", err)
	}
	if _, err := timeline.Read(context.Background(), "task"); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatalf("Read: %v", err)
	}
	if recorder, err := timeline.Start(context.Background(), "task", "startup"); recorder != nil || !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatalf("Start: %v, %v", recorder, err)
	}
	// Standalone construction neither depends on nor installs a global Manager.
	if _, err := timeline.New("standalone"); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.For("standalone"); !errors.Is(err, timeline.ErrNoDefaultManager) {
		t.Fatalf("New installed a default: %v", err)
	}
}

func TestGlobalWritersShareDocumentNotAuthority(t *testing.T) {
	ctx := context.Background()
	store := timeline.NewMemoryStore()
	m := manager(t, store, timeline.ManagerConfig{})
	defaultManager(t, m)
	owner, err := timeline.For("task", timeline.WithActor(timeline.Actor{ID: "owner"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Read(ctx, "task"); !errors.Is(err, timeline.ErrNotFound) {
		t.Fatalf("For created a durable operation: %v", err)
	}
	if err := owner.Start(ctx, "startup"); err != nil {
		t.Fatal(err)
	}
	worker, err := timeline.For("task", timeline.WithActor(timeline.Actor{ID: "worker"}))
	if err != nil {
		t.Fatal(err)
	}
	if worker == owner {
		t.Fatal("global entry cached coordinator handle")
	}
	worker.Begin("initialize").End(nil)
	// Read does not Flush: this verifies that the global handle uses the worker.
	eventually(t, func() bool {
		doc, err := timeline.Read(ctx, "task")
		return err == nil && len(doc.Stages) == 1 && doc.Stages[0].Status == timeline.Succeeded && doc.Stages[0].Actor.ID == "worker"
	})
	if _, err := worker.Finish(ctx, nil); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("worker inherited coordinator authority: %v", err)
	}
	if _, err := owner.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	worker.Begin("late_observation").End(nil)
	shutdownCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := m.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.For("task"); !errors.Is(err, timeline.ErrManagerClosed) {
		t.Fatalf("For bypassed closed manager: %v", err)
	}
	doc, err := timeline.Read(ctx, "task")
	if err != nil || doc.Status != timeline.Succeeded || len(doc.Stages) != 2 {
		t.Fatalf("drain/read lost late stage: %+v %v", doc, err)
	}
}

func TestDefaultReplacementKeepsExistingWritersBound(t *testing.T) {
	ctx := context.Background()
	firstStore, secondStore := timeline.NewMemoryStore(), timeline.NewMemoryStore()
	first, second := manager(t, firstStore, timeline.ManagerConfig{}), manager(t, secondStore, timeline.ManagerConfig{})
	defaultManager(t, first)
	before, err := timeline.For("same-id")
	if err != nil {
		t.Fatal(err)
	}
	if previous := timeline.SetDefaultManager(second); previous != first {
		t.Fatal("replacement lost previous default")
	}
	after, err := timeline.For("same-id")
	if err != nil {
		t.Fatal(err)
	}
	before.Begin("first").End(nil)
	after.Begin("second").End(nil)
	// Swapping defaults is not a lifecycle operation. Both Managers still work.
	for _, m := range []*timeline.Manager{first, second} {
		if err := m.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	oldDoc, err := first.Read(ctx, "same-id")
	if err != nil || len(oldDoc.Stages) != 1 || oldDoc.Stages[0].Name != "first" {
		t.Fatalf("old writer rebound: %+v %v", oldDoc, err)
	}
	current, err := timeline.Read(ctx, "same-id")
	if err != nil || len(current.Stages) != 1 || current.Stages[0].Name != "second" {
		t.Fatalf("global reader used old store: %+v %v", current, err)
	}
	// Explicit New keeps its configured store even when a default exists.
	standaloneStore := timeline.NewMemoryStore()
	independent, err := timeline.New("same-id", timeline.WithStore(standaloneStore))
	if err != nil {
		t.Fatal(err)
	}
	independent.Begin("standalone").End(nil)
	if err := independent.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.For("bad-store", timeline.WithStore(standaloneStore)); err == nil {
		t.Fatal("global handle bypassed Manager store")
	}
	if _, err := timeline.For(""); !errors.Is(err, timeline.ErrEmptyID) {
		t.Fatalf("empty ID: %v", err)
	}
	current, _ = timeline.Read(ctx, "same-id")
	if len(current.Stages) != 1 {
		t.Fatal("standalone constructor used the global manager")
	}
}

func TestConcurrentDefaultSelection(t *testing.T) {
	first := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	second := manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{})
	defaultManager(t, first)
	const count = 40
	var workers sync.WaitGroup
	for i := range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			selected := first
			if i%2 != 0 {
				selected = second
			}
			timeline.SetDefaultManager(selected)
			recorder, err := timeline.For(fmt.Sprint(i))
			if err != nil {
				t.Error(err)
				return
			}
			recorder.Begin("work").End(nil)
			// Read may address either default, and is race-safe during installation.
			_, err = timeline.Read(context.Background(), recorder.ID())
			if err != nil && !errors.Is(err, timeline.ErrNotFound) {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	for _, m := range []*timeline.Manager{first, second} {
		if err := m.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for i := range count {
		found := 0
		for _, m := range []*timeline.Manager{first, second} {
			doc, err := m.Read(context.Background(), fmt.Sprint(i))
			if err == nil && len(doc.Stages) == 1 && doc.Stages[0].Status == timeline.Succeeded {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("writer %d persisted to %d managers", i, found)
		}
	}
}

func TestGlobalStartOwnsOperation(t *testing.T) {
	ctx := context.Background()
	defaultManager(t, manager(t, timeline.NewMemoryStore(), timeline.ManagerConfig{}))
	owner, err := timeline.Start(ctx, "task", "startup", timeline.WithActor(timeline.Actor{ID: "owner"}))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := timeline.Read(ctx, "task")
	if err != nil || doc.Operation != "startup" || doc.Status != timeline.Running || doc.StartedAt.IsZero() {
		t.Fatalf("start boundary not persisted: %+v, %v", doc, err)
	}
	owner.Begin("prepare").End(nil)
	worker, err := timeline.For("task")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Finish(ctx, nil); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("worker inherited completion authority: %v", err)
	}
	snapshot, err := owner.Finish(ctx, nil)
	if err != nil || snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != 1 || snapshot.Stages[0].Actor.ID != "owner" {
		t.Fatalf("coordinator result: %+v, %v", snapshot, err)
	}
	if recorder, err := timeline.Start(ctx, "", "startup"); recorder != nil || !errors.Is(err, timeline.ErrEmptyID) {
		t.Fatalf("invalid construction: %v, %v", recorder, err)
	}
}

func TestGlobalStartRetainsHandleAfterPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	unavailable := errors.New("store unavailable")
	var available atomic.Bool
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	store.merge = func(ctx context.Context, id string, update timeline.Update) error {
		if !available.Load() {
			return unavailable
		}
		return store.MemoryStore.Merge(ctx, id, update)
	}
	defaultManager(t, manager(t, store, timeline.ManagerConfig{}))
	owner, err := timeline.Start(ctx, "task", "startup")
	if owner == nil || !errors.Is(err, unavailable) {
		t.Fatalf("lost handle or persistence error: %v, %v", owner, err)
	}
	if err := owner.Start(ctx, "startup"); !errors.Is(err, timeline.ErrAlreadyStarted) {
		t.Fatalf("failed persistence lost original start: %v", err)
	}
	available.Store(true)
	if err := owner.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := owner.Finish(ctx, nil); err != nil || snapshot.Status != timeline.Succeeded {
		t.Fatalf("retry lost completion authority: %+v, %v", snapshot, err)
	}
}
