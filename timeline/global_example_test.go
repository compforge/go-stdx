package timeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func ExampleFor() {
	m, err := timeline.NewManager(timeline.NewMemoryStore(), timeline.ManagerConfig{})
	if err != nil {
		panic(err)
	}
	previous := timeline.SetDefaultManager(m)
	defer timeline.SetDefaultManager(previous)

	// Business code knows only its operation ID. The creator owns Start/Finish.
	owner, err := timeline.For("task-42")
	if err != nil {
		panic(err)
	}
	if err := owner.Start(context.Background(), "prepare"); err != nil {
		panic(err)
	}
	worker, err := timeline.For("task-42")
	if err != nil {
		panic(err)
	}
	worker.Begin("files").End(nil)

	// The application joins producers before draining its process Manager.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := owner.Finish(ctx, nil); err != nil {
		panic(err)
	}
	if err := m.Shutdown(ctx); err != nil {
		panic(err)
	}
	doc, err := timeline.Read(ctx, "task-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(doc.ID, doc.Status, len(doc.Stages))
	// Output: task-42 succeeded 1
}
