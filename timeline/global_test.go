package timeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/manager"
)

func TestRootEntryPointsShareInstalledManager(t *testing.T) {
	m, err := timeline.NewManager(nil, timeline.Config{Actor: timeline.Actor{Name: "test"}, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	previous := timeline.SetDefault(m)
	defer timeline.SetDefault(previous)
	defer m.Shutdown(context.Background())
	if err := timeline.Start("task", "review"); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.Begin("task", "prepare"); err != nil {
		t.Fatal(err)
	}
	// Both import paths must operate on the same default and cached document.
	if err := manager.End("task", "prepare", nil); err != nil {
		t.Fatal(err)
	}
	if err := timeline.Record("task", completedStage()); err != nil {
		t.Fatal(err)
	}
	if err := timeline.Finish("task", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := timeline.Read(context.Background(), "task", false)
	if err != nil || snapshot.Operation != "review" || snapshot.Status != timeline.Succeeded || len(snapshot.Stages) != 2 || snapshot.Collection.LocalFlushed || snapshot.Collection.StoreRead {
		t.Fatalf("root entry points bypassed installed cache: %+v %v", snapshot, err)
	}
	if err := m.Flush(context.Background(), "task", true); err != nil {
		t.Fatal(err)
	}
	// Start and Finish are optional; NoopStore must not erase the cache on Flush.
	if _, err := timeline.Begin("stages-only", "work"); err != nil {
		t.Fatal(err)
	}
	if err := timeline.End("stages-only", "work", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err = manager.Read(context.Background(), "stages-only", false)
	if err != nil || snapshot.Status != timeline.Unknown || len(snapshot.Stages) != 1 || snapshot.Stages[0].Status != timeline.Succeeded {
		t.Fatalf("optional operation boundaries required: %+v %v", snapshot, err)
	}
	timeline.SetDefault(nil)
	if _, err := timeline.Read(context.Background(), "task", false); !errors.Is(err, timeline.ErrNoDefault) {
		t.Fatalf("root entry point ignored default replacement: %v", err)
	}
}
