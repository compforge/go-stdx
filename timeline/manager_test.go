package timeline_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

type managedStore struct {
	*timeline.MemoryStore
	merge func(context.Context, string, timeline.Update) error
}

func (s *managedStore) Merge(ctx context.Context, id string, u timeline.Update) error {
	return s.merge(ctx, id, u)
}

func manager(t *testing.T, store timeline.Store, config timeline.ManagerConfig) *timeline.Manager {
	t.Helper()
	if config.FlushInterval == 0 {
		config.FlushInterval = 5 * time.Millisecond
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 100 * time.Millisecond
	}
	if config.OnError == nil {
		config.OnError = func(string, error) {}
	}
	m, err := timeline.NewManager(store, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	return m
}

func managedHandle(t *testing.T, m *timeline.Manager, id, actor string) *timeline.Recorder {
	t.Helper()
	r, err := m.New(id, timeline.WithActor(timeline.Actor{ID: actor}))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background persistence did not converge")
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not reach expected IO")
	}
}

func TestManagerPersistsRunningAndFinalStagesWithoutFlush(t *testing.T) {
	store := timeline.NewMemoryStore()
	m := manager(t, store, timeline.ManagerConfig{})
	r := managedHandle(t, m, "operation", "worker")
	s := r.Begin("initialize")
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), r.ID())
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Running
	})
	s.SetAttributes(timeline.Attribute{Key: "step", Value: "files"})
	s.End(errors.New("init failed"))
	// Read the Store directly: Snapshot would flush and hide the original bug.
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), r.ID())
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Failed &&
			d.Stages[0].Error == "init failed" && string(d.Stages[0].Attributes["step"]) == `"files"`
	})
	eventually(t, func() bool { return m.Stats().PendingHandles == 0 })
	// An idle handle can become dirty again without a Registry entry or release API.
	r.Begin("late").End(nil)
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), r.ID())
		return len(d.Stages) == 2 && d.Stages[1].Status == timeline.Succeeded
	})
}

func TestManagerRetriesWithoutAnotherNotification(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	var calls, reported atomic.Int32
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if calls.Add(1) <= 2 {
			return errors.New("temporary database failure")
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{OnError: func(string, error) { reported.Add(1) }})
	// No subsequent mutation or explicit flush is needed to retry.
	managedHandle(t, m, "retry", "worker").Begin("work").End(nil)
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), "retry")
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Succeeded
	})
	if reported.Load() != 1 {
		t.Fatalf("failure streak reported %d times", reported.Load())
	}
}

func TestManagerRetriesIdenticalBatchThenPersistsConcurrentEnd(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	var first timeline.Update
	var calls atomic.Int32
	var identical atomic.Bool
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		switch calls.Add(1) {
		case 1:
			first = u
			if err := store.MemoryStore.Merge(ctx, id, u); err != nil {
				return err
			}
			close(entered)
			select {
			case <-release:
				return errors.New("commit acknowledgement lost")
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			identical.Store(reflect.DeepEqual(first, u))
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{ExportTimeout: time.Second})
	r := managedHandle(t, m, "concurrent", "worker")
	s := r.Begin("work")
	waitSignal(t, entered)
	mutated := make(chan struct{})
	go func() {
		s.SetAttributes(timeline.Attribute{Key: "version", Value: 8})
		s.End(nil)
		close(mutated)
	}()
	// Mutation must complete while the Store is still blocked.
	waitSignal(t, mutated)
	close(release)
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), "concurrent")
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Succeeded &&
			string(d.Stages[0].Attributes["version"]) == "8"
	})
	if !identical.Load() {
		t.Fatal("ambiguous write retried with different contents")
	}
}

func TestManagerSharedIDKeepsWriterAuthorityAndActor(t *testing.T) {
	store := timeline.NewMemoryStore()
	m := manager(t, store, timeline.ManagerConfig{})
	owner := managedHandle(t, m, "shared", "owner")
	if err := owner.Start(context.Background(), "task"); err != nil {
		t.Fatal(err)
	}
	worker := managedHandle(t, m, "shared", "worker")
	if _, err := worker.Finish(context.Background(), nil); !errors.Is(err, timeline.ErrNotStarted) {
		t.Fatalf("worker acquired coordinator authority: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		r := managedHandle(t, m, "shared", fmt.Sprint(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Begin("work").End(nil)
		}()
	}
	wg.Wait()
	if err := m.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := owner.Finish(context.Background(), nil)
	if err != nil || got.Status != timeline.Succeeded || len(got.Stages) != 20 {
		t.Fatalf("snapshot: %+v, %v", got, err)
	}
	actors := make(map[string]bool)
	for _, s := range got.Stages {
		actors[s.Actor.ID] = true
	}
	if len(actors) != 20 {
		t.Fatal("independent actor identities were merged")
	}
}

func TestManagerTimeoutDoesNotStarveHealthyHandle(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	entered := make(chan struct{})
	var once sync.Once
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if id == "stalled" {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return ctx.Err()
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{ExportTimeout: 20 * time.Millisecond})
	managedHandle(t, m, "stalled", "").Begin("work")
	waitSignal(t, entered)
	managedHandle(t, m, "healthy", "").Begin("work").End(nil)
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), "healthy")
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Succeeded
	})
}

func TestManagerShutdownCancelsWorkerAndDrains(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	entered := make(chan struct{})
	var calls atomic.Int32
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{ExportTimeout: time.Minute})
	r := managedHandle(t, m, "shutdown", "worker")
	s := r.Begin("work")
	waitSignal(t, entered)
	s.End(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	d, _ := store.Read(ctx, "shutdown")
	if len(d.Stages) != 1 || d.Stages[0].Status != timeline.Succeeded || m.Stats().PendingHandles != 0 {
		t.Fatalf("shutdown did not drain: %+v", d)
	}
	if _, err := m.New("new"); !errors.Is(err, timeline.ErrManagerClosed) {
		t.Fatalf("new after shutdown: %v", err)
	}
	r.Begin("too late")
	if err := r.Flush(ctx); !errors.Is(err, timeline.ErrManagerClosed) {
		t.Fatalf("late mutation not reported: %v", err)
	}
}

func TestManagerFailedShutdownCanBeRetried(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	var available atomic.Bool
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		if !available.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		return store.MemoryStore.Merge(ctx, id, u)
	}
	m := manager(t, store, timeline.ManagerConfig{})
	managedHandle(t, m, "shutdown-retry", "").Begin("work").End(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := m.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected drain deadline: %v", err)
	}
	available.Store(true)
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, _ := store.Read(context.Background(), "shutdown-retry")
	if len(d.Stages) != 1 || d.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("retry lost records: %+v", d)
	}
}

func TestManagerBoundsBuffersAndCoalescesUnsentUpdates(t *testing.T) {
	store := &managedStore{MemoryStore: timeline.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.merge = func(ctx context.Context, id string, u timeline.Update) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return store.MemoryStore.Merge(ctx, id, u)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m := manager(t, store, timeline.ManagerConfig{
		MaxPendingHandles: 1, MaxPendingUpdates: 2, ExportTimeout: time.Second,
	})
	r := managedHandle(t, m, "bounded", "")
	s := r.Begin("work")
	waitSignal(t, entered)
	for i := 0; i < 1000; i++ {
		s.SetAttributes(timeline.Attribute{Key: "value", Value: i})
	}
	s.End(nil)
	// One immutable in-flight update + one coalesced terminal update fill the limit.
	r.Begin("overflow")
	other := managedHandle(t, m, "overflow-handle", "")
	other.Begin("overflow")
	if m.Stats().DroppedUpdates != 2 {
		t.Fatalf("missing overflow accounting: %+v", m.Stats())
	}
	close(release)
	if err := r.Flush(context.Background()); !errors.Is(err, timeline.ErrBufferFull) {
		t.Fatalf("record loss must be visible to caller: %v", err)
	}
	if err := other.Flush(context.Background()); !errors.Is(err, timeline.ErrBufferFull) {
		t.Fatalf("handle admission loss must be visible: %v", err)
	}
	d, _ := store.Read(context.Background(), "bounded")
	if len(d.Stages) != 1 || d.Stages[0].Status != timeline.Succeeded || string(d.Stages[0].Attributes["value"]) != "999" {
		t.Fatalf("buffer coalescing lost final state: %+v", d)
	}
}

func TestManagerRecordImportAndConflict(t *testing.T) {
	store := timeline.NewMemoryStore()
	m := manager(t, store, timeline.ManagerConfig{})
	r := managedHandle(t, m, "import", "")
	now := time.Now()
	s := timeline.Stage{ID: "external", Name: "external", StartedAt: now, FinishedAt: now, Status: timeline.Succeeded}
	if err := r.Record(s); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), "import")
		return len(d.Stages) == 1 && d.Stages[0].Status == timeline.Succeeded
	})
	s.Name = "conflicting-name"
	_ = r.Record(s)
	if err := r.Flush(context.Background()); !errors.Is(err, timeline.ErrConflict) {
		t.Fatalf("conflict lost: %v", err)
	}
}

func TestManagerConcurrentFlushAndHandleReuse(t *testing.T) {
	store := timeline.NewMemoryStore()
	m := manager(t, store, timeline.ManagerConfig{})
	r := managedHandle(t, m, "reused", "")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if err := m.Flush(context.Background()); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		r.Begin("work").End(nil)
	}
	close(stop)
	wg.Wait()
	// Explicit flushes have stopped. Background persistence must still own the
	// final dirty generation even if an earlier completion retired the handle.
	r.Begin("last").End(nil)
	eventually(t, func() bool {
		d, _ := store.Read(context.Background(), "reused")
		if len(d.Stages) != 101 {
			return false
		}
		for _, s := range d.Stages {
			if s.Status != timeline.Succeeded {
				return false
			}
		}
		return true
	})
}
