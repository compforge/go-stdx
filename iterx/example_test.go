package iterx_test

import (
	"context"
	"fmt"

	"github.com/compforge/go-stdx/iterx"
)

func ExampleNewLoadingIterator() {
	// A real loader could query a database or a paginated API. Its cursor and
	// batch size are private to this traversal.
	values := []string{"alice", "bob", "carol"}
	cursor := 0
	page := func(ctx context.Context) ([]string, bool, error) {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		end := min(cursor+2, len(values))
		items := values[cursor:end]
		cursor = end
		return items, cursor < len(values), nil
	}

	users := iterx.NewLoadingIterator(context.Background(), page)
	for user, err := range users {
		if err != nil {
			fmt.Println("load:", err)
			return
		}
		fmt.Println(user)
	}
	// Output:
	// alice
	// bob
	// carol
}
