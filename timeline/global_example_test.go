package timeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func ExampleStart() {
	m, err := timeline.NewManager(timeline.NewMemoryStore(), timeline.ManagerConfig{})
	if err != nil {
		panic(err)
	}
	previous := timeline.SetDefaultManager(m)
	defer timeline.SetDefaultManager(previous)

	// Business code knows only its operation ID. The creator owns Start/Finish.
	_, err = timeline.Start(context.Background(), "task-42", "prepare")
	if err != nil {
		panic(err)
	}
	_, err = timeline.Begin("task-42", "files")
	if err != nil {
		panic(err)
	}
	if err := timeline.End("task-42", "files", nil); err != nil {
		panic(err)
	}

	// The application joins producers before draining its process Manager.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := timeline.Finish(ctx, "task-42", nil); err != nil {
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
