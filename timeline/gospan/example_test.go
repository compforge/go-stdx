package gospantimeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/compforge/go-stdx/timeline"
	gospantimeline "github.com/compforge/go-stdx/timeline/gospan"
)

func ExampleNew() {
	ctx, tl, err := gospantimeline.New(context.Background(), "sandbox.start",
		timeline.Field{Key: "sandbox_id", Value: "sandbox-1"})
	if err != nil {
		panic(err)
	}
	prepareCtx, prepare := tl.Begin(ctx, "prepare")
	_, workspace := tl.Begin(prepareCtx, "resolve_workspace")
	workspace.End(nil)
	prepare.End(nil)

	// The completion owner uses its own collection budget. A timed-out HTTP
	// request may instead call Snapshot while background work continues.
	collectCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := tl.Finish(collectCtx, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Operation, result.Status, result.Complete)
	fmt.Println(result.Stages[1].ParentID == result.Stages[0].ID)
	// Output:
	// sandbox.start succeeded true
	// true
}
