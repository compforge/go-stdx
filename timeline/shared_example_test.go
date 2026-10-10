package timeline_test

import (
	"context"
	"fmt"

	"github.com/compforge/go-stdx/timeline"
	timelinestore "github.com/compforge/go-stdx/timeline/store"
)

func ExampleNew() {
	ctx := context.Background()
	// A persistent Store can connect these handles across process boundaries.
	store := timelinestore.NewMemoryStore()
	owner, _ := timeline.New("sandbox-42", timeline.WithActor(timeline.Actor{Name: "test"}), timeline.WithStore(store))
	if err := owner.Start(ctx, "sandbox_start"); err != nil {
		panic(err)
	}
	worker, _ := timeline.New("sandbox-42", timeline.WithActor(timeline.Actor{Name: "test"}), timeline.WithStore(store),
		timeline.WithActor(timeline.Actor{Name: "scheduler-pod"}))
	_, stage := timeline.BeginWithContext(ctx, worker, "acquire_carrier")
	stage.End(nil)
	if err := worker.Flush(ctx); err != nil {
		panic(err)
	}
	snapshot, err := owner.Finish(ctx, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(snapshot.ID, snapshot.Operation, snapshot.Status)
	fmt.Println(snapshot.Stages[0].Name, snapshot.Stages[0].Actor.Name)
	// Output:
	// sandbox-42 sandbox_start succeeded
	// acquire_carrier scheduler-pod
}
