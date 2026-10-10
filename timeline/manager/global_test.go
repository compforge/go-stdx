package manager_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/compforge/go-stdx/timeline"
	managed "github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func defaultManager(t *testing.T, m *managed.Manager) {
	t.Helper()
	previous := managed.SetDefault(m)
	t.Cleanup(func() { managed.SetDefault(previous) })
}

func TestGlobalRequiresExplicitSetup(t *testing.T) {
	defaultManager(t, nil)
	_, beginErr := managed.Begin("task", "work")
	_, readErr := managed.Read(context.Background(), "task")
	for _, err := range []error{beginErr, readErr, managed.Start("task", "startup"), managed.Finish("task", nil), managed.Record("task", timeline.Stage{}), managed.End("task", "work", nil)} {
		if !errors.Is(err, managed.ErrNoDefault) {
			t.Fatal(err)
		}
	}
	if _, err := timeline.New("standalone"); err != nil {
		t.Fatal(err)
	}
	if _, err := managed.Begin("task", "work"); !errors.Is(err, managed.ErrNoDefault) {
		t.Fatal(err)
	}
}

func TestDefaultReplacementKeepsExistingWritersBound(t *testing.T) {
	ctx := context.Background()
	first := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	second := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	defaultManager(t, first)
	before, err := managed.Begin("same-id", "first")
	if err != nil {
		t.Fatal(err)
	}
	if previous := managed.SetDefault(second); previous != first {
		t.Fatal("lost previous default")
	}
	after, err := managed.Begin("same-id", "second")
	if err != nil {
		t.Fatal(err)
	}
	if err := before.End(nil); err != nil {
		t.Fatal(err)
	}
	if err := after.End(nil); err != nil {
		t.Fatal(err)
	}
	for m, name := range map[*managed.Manager]string{first: "first", second: "second"} {
		snapshot, err := m.Read(ctx, "same-id")
		if err != nil || len(snapshot.Stages) != 1 || snapshot.Stages[0].Name != name {
			t.Fatalf("writer rebound: %+v %v", snapshot, err)
		}
	}
	current, err := managed.Read(ctx, "same-id")
	if err != nil || current.Stages[0].Name != "second" {
		t.Fatalf("wrong default: %+v %v", current, err)
	}
	standalone, _ := timeline.New("same-id")
	standalone.Begin("standalone").End(nil)
	current, _ = managed.Read(ctx, "same-id")
	if len(current.Stages) != 1 {
		t.Fatal("standalone used default")
	}
}

func TestConcurrentDefaultSelection(t *testing.T) {
	first := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	second := newManager(t, timelinestore.NewMemoryStore(), managed.Config{})
	defaultManager(t, first)
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			selected := first
			if i%2 != 0 {
				selected = second
			}
			managed.SetDefault(selected)
			stage, err := managed.Begin(fmt.Sprint(i), "work")
			if err != nil {
				t.Error(err)
				return
			}
			if err := stage.End(nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for i := range 40 {
		found := 0
		for _, m := range []*managed.Manager{first, second} {
			snapshot, err := m.Read(context.Background(), fmt.Sprint(i))
			if err == nil && len(snapshot.Stages) == 1 && snapshot.Stages[0].Status == timeline.Succeeded {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("ID %d persisted to %d managers", i, found)
		}
	}
}
