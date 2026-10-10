package manager_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func TestRejectedTerminalAdmissionCanBeRetried(t *testing.T) {
	for _, kind := range []string{"operation", "stage"} {
		t.Run(kind, func(t *testing.T) {
			store := &managedStore{MemoryStore: timelinestore.NewMemoryStore()}
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			store.merge = func(ctx context.Context, id string, u timelinestore.Update) error {
				if calls.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return store.MemoryStore.Merge(ctx, id, u)
			}
			m := newManager(t, store, managed.Config{MaxPendingUpdates: 1, ExportTimeout: time.Second})
			var end func(error) error
			if kind == "operation" {
				if err := m.Start("task", "work"); err != nil {
					t.Fatal(err)
				}
				end = func(err error) error { return m.Finish("task", err) }
			} else {
				if _, err := m.Begin("task", "work"); err != nil {
					t.Fatal(err)
				}
				end = func(err error) error { return m.End("task", "work", err) }
			}
			waitSignal(t, entered)
			businessErr := errors.New("first result")
			if err := end(businessErr); !errors.Is(err, managed.ErrBufferFull) {
				t.Fatalf("expected backpressure: %v", err)
			}
			if kind == "stage" {
				snapshot, err := m.Read(context.Background(), "task")
				if err != nil || len(snapshot.RunningStages()) != 1 {
					t.Fatalf("rejected End changed stage: %+v %v", snapshot, err)
				}
			}
			close(release)
			eventually(t, func() bool { return m.Stats().PendingTimelines == 0 })
			// Rejected operations do not advance cached state; retry accepts its result.
			if err := end(nil); err != nil {
				t.Fatal(err)
			}
			snapshot, err := m.Read(context.Background(), "task")
			if err != nil {
				t.Fatal(err)
			}
			status, message := snapshot.Status, snapshot.Error
			if kind == "stage" {
				status, message = snapshot.Stages[0].Status, snapshot.Stages[0].Error
			}
			if status != timeline.Succeeded || message != "" {
				t.Fatalf("terminal lost on retry: %+v", snapshot)
			}
		})
	}
}

func TestRejectedAttributesDoNotChangeCachedFacts(t *testing.T) {
	m := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	stage, err := m.Begin("task", "work")
	if err != nil {
		t.Fatal(err)
	}
	if err = stage.SetAttributes(timeline.Attribute{Key: "invalid", Value: make(chan int)}); !errors.Is(err, timeline.ErrInvalidAttribute) {
		t.Fatal(err)
	}
	if err = stage.End(nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.Read(context.Background(), "task")
	if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Status != timeline.Succeeded || len(snapshot.Stages[0].Attributes) != 0 {
		t.Fatalf("rejected write changed facts: %+v %v", snapshot, err)
	}
}
