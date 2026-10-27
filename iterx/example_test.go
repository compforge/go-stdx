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

// ExampleUniqueKeys joins identities from different value types and resolves
// them in caller-sized batches. Missing map entries remain distinct from errors.
func ExampleUniqueKeys() {
	ctx := context.Background()
	type record struct{ ID string }
	records := map[string]record{"a": {ID: "a"}, "b": {ID: "b"}}
	live := map[string]string{"b": "b", "c": "c"}

	db := iterx.NewLoadingSource(ctx,
		func(context.Context) ([]record, bool, error) {
			return []record{records["a"], records["b"]}, false, nil
		},
		func(_ context.Context, keys []string) (map[string]record, error) {
			result := make(map[string]record)
			for _, key := range keys {
				if value, found := records[key]; found {
					result[key] = value
				}
			}
			return result, nil
		},
		func(value record) string { return value.ID },
	)
	actual := iterx.NewLoadingSource(ctx,
		func(context.Context) ([]string, bool, error) { return []string{"b", "c"}, false, nil },
		func(_ context.Context, keys []string) (map[string]string, error) {
			result := make(map[string]string)
			for _, key := range keys {
				if value, found := live[key]; found {
					result[key] = value
				}
			}
			return result, nil
		},
		func(value string) string { return value },
	)

	reconcileBatch := func(keys []string) error {
		dbValues, err := db.MGet(keys)
		if err != nil {
			return err
		}
		actualValues, err := actual.MGet(keys)
		if err != nil {
			return err
		}
		for _, key := range keys {
			_, inDB := dbValues[key]
			_, inActual := actualValues[key]
			fmt.Printf("%s: db=%t actual=%t\n", key, inDB, inActual)
		}
		return nil
	}

	batch := make([]string, 0, 2)
	for key, err := range iterx.UniqueKeys[string](db, actual) {
		if err != nil {
			fmt.Println(err)
			return
		}
		batch = append(batch, key)
		if len(batch) == cap(batch) {
			if err := reconcileBatch(batch); err != nil {
				fmt.Println(err)
				return
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := reconcileBatch(batch); err != nil {
			fmt.Println(err)
		}
	}
	// Output:
	// a: db=true actual=false
	// b: db=true actual=true
	// c: db=false actual=true
}
