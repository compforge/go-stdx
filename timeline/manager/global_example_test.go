package manager_test

import (
	"context"
	"fmt"

	"github.com/compforge/go-stdx/timeline/manager"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func Example() {
	m, err := manager.New(timelinestore.NewMemoryStore(), manager.Config{MaxTimelines: 1024})
	if err != nil {
		panic(err)
	}
	previous := manager.SetDefault(m)
	defer manager.SetDefault(previous)
	defer m.Shutdown(context.Background())

	// Stage recording needs neither Start nor Finish.
	if _, err := manager.Begin("task-42", "prepare_files"); err != nil {
		panic(err)
	}
	if err := manager.End("task-42", "prepare_files", nil); err != nil {
		panic(err)
	}
	snapshot, err := manager.Read(context.Background(), "task-42")
	if err != nil {
		panic(err)
	}
	fmt.Println(snapshot.Status, snapshot.Stages[0].Status)
	// Output: unknown succeeded
}
