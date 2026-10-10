package manager_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestFlushWithoutWaitWakesWorkerAndOutlivesCaller(t *testing.T) {
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	backend.merge = func(ctx context.Context, id string, update timelinestore.Update) error {
		close(entered)
		select {
		case <-release:
			return backend.MemoryStore.Merge(ctx, id, update)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, FlushInterval: time.Hour, ExportTimeout: 3 * time.Second})
	releaseSave := sync.OnceFunc(func() { close(release) })
	defer releaseSave()
	if _, err := m.Begin("task", "work"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- m.Flush(ctx, "task", false) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("nonblocking Flush waited for Store IO")
	}
	cancel()
	waitSignal(t, entered)
	// The caller has already canceled, but the worker must still own this save.
	if _, err := backend.Read(context.Background(), "task"); !errors.Is(err, timelinestore.ErrNotFound) {
		t.Fatalf("save unexpectedly completed before release: %v", err)
	}
	// The deferred release also unblocks IO after a failed assertion.
	releaseSave()
	eventually(t, func() bool {
		doc, err := backend.Read(context.Background(), "task")
		return err == nil && len(doc.Stages) == 1
	})
}

func TestFlushWaitsForRequestedID(t *testing.T) {
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	backend.merge = func(ctx context.Context, id string, update timelinestore.Update) error {
		if id != "task" {
			t.Errorf("blocking Flush saved unrelated ID %q", id)
		}
		close(entered)
		select {
		case <-release:
			return backend.MemoryStore.Merge(ctx, id, update)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, FlushInterval: time.Hour, ExportTimeout: 3 * time.Second})
	releaseSave := sync.OnceFunc(func() { close(release) })
	defer releaseSave()
	for _, id := range []string{"task", "other"} {
		if _, err := m.Begin(id, "work"); err != nil {
			t.Fatal(err)
		}
	}
	returned := make(chan error, 1)
	go func() { returned <- m.Flush(context.Background(), "task", true) }()
	waitSignal(t, entered)
	select {
	case err := <-returned:
		t.Fatalf("blocking Flush returned before Store IO finished: %v", err)
	default:
	}
	releaseSave()
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	// Clean up the unrelated entry without invoking the blocking mock at Shutdown.
	backend.merge = backend.MemoryStore.Merge
	if _, err := backend.Read(context.Background(), "other"); !errors.Is(err, timelinestore.ErrNotFound) {
		t.Fatalf("unrelated ID was submitted: %v", err)
	}
}

func TestFlushWithoutWaitReportsSaveFailure(t *testing.T) {
	failure := errors.New("offline")
	backend := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
	backend.merge = func(context.Context, string, timelinestore.Update) error { return failure }
	reported := make(chan error, 1)
	m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, FlushInterval: time.Hour, OnError: func(id string, err error) {
		if id != "task" {
			t.Errorf("reported ID %q", id)
		}
		reported <- err
	}})
	if _, err := m.Begin("task", "work"); err != nil {
		t.Fatal(err)
	}
	if err := m.Flush(context.Background(), "task", false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, failure) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker was not woken or save failure was not reported")
	}
}

func TestNoopStoreKeepsFactsInManagerCache(t *testing.T) {
	for _, backend := range []timelinestore.Store{nil, timelinestore.NewNoopStore()} {
		m := newManager(t, backend, managed.Config{Actor: managed.Actor{Name: "test"}, FlushInterval: time.Hour})
		writer := managedHandle(t, m, "task", "worker")
		stage := writer.Begin("work")
		if err := stage.End(nil); err != nil {
			t.Fatal(err)
		}
		snapshot, err := writer.Finish(context.Background(), nil)
		if err != nil || len(snapshot.Stages) != 1 || snapshot.Status != timeline.Succeeded || !snapshot.Collection.LocalFlushed || snapshot.Collection.StoreRead {
			t.Fatalf("writer bypassed recording cache: %+v %v", snapshot, err)
		}
		m.Evict("task")
		if _, err := m.Read(context.Background(), "task", false); !errors.Is(err, timelinestore.ErrNotFound) {
			t.Fatalf("NoopStore recovered evicted facts: %v", err)
		}
	}
}
