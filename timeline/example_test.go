package timeline_test

import (
	"context"
	"fmt"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func ExampleRegistry() {
	registry := timeline.NewRegistry(gospantimeline.New)
	ctx := context.Background()
	tl, err := registry.Create(ctx, "start-42", "sandbox_start")
	if err != nil {
		panic(err)
	}

	// Pass Timeline directly along a call chain.
	prepare := func(ctx context.Context, tl timeline.Timeline) {
		_, stage := tl.Begin(ctx, "prepare")
		stage.End(nil)
	}
	prepare(ctx, tl)

	// An independent component needs only the shared registry and operation ID.
	joined, ok := registry.Lookup("start-42")
	if !ok {
		panic("operation missing")
	}
	_, stage := joined.Begin(ctx, "provision")
	stage.End(nil)

	// Context propagation is an optional caller choice.
	bound, _ := timeline.FromContext(timeline.NewContext(ctx, tl))
	fmt.Println(joined == tl, bound == tl)
	result, err := tl.Finish(ctx, nil)
	if err != nil {
		panic(err)
	}
	_, active := registry.Lookup(tl.ID())
	fmt.Println(result.ID, len(result.Stages), active)
	// Output:
	// true true
	// start-42 2 false
}
