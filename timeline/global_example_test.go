package timeline_test

import (
	"context"
	"fmt"

	"github.com/compforge/go-stdx/timeline"
)

func Example() {
	m, err := timeline.NewManager(nil, timeline.Config{})
	if err != nil {
		panic(err)
	}
	previous := timeline.SetDefault(m)
	defer timeline.SetDefault(previous)
	defer m.Shutdown(context.Background())

	// Stage recording needs neither Start nor Finish nor persistent storage.
	if _, err := timeline.Begin("task-42", "prepare_files"); err != nil {
		panic(err)
	}
	if err := timeline.End("task-42", "prepare_files", nil); err != nil {
		panic(err)
	}
	snapshot, err := timeline.Read(context.Background(), "task-42", false)
	if err != nil {
		panic(err)
	}
	fmt.Println(snapshot.Status, snapshot.Stages[0].Status)
	// Output: unknown succeeded
}
