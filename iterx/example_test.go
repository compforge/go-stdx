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
	actual := iterx.NewMapSource(ctx, live, func(a, b string) bool { return a < b })

	for keys, err := range iterx.Batch(iterx.UniqueKeys[string](db, actual), 2) {
		if err != nil {
			fmt.Println(err)
			return
		}
		dbValues, err := db.MGet(keys)
		if err != nil {
			fmt.Println(err)
			return
		}
		actualValues, err := actual.MGet(keys)
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, key := range keys {
			_, inDB := dbValues[key]
			_, inActual := actualValues[key]
			fmt.Printf("%s: db=%t actual=%t\n", key, inDB, inActual)
		}
	}
	// Output:
	// a: db=true actual=false
	// b: db=true actual=true
	// c: db=false actual=true
}

func ExampleNewLoadingSourceFromKeys() {
	type record struct{ Name string }
	ctx := context.Background()
	// Enumeration can select only IDs; the independent lookup loads full records.
	source := iterx.NewLoadingSourceFromKeys(ctx,
		func(context.Context) ([]int, bool, error) { return []int{1, 2}, false, nil },
		func(_ context.Context, keys []int) (map[int]record, error) {
			values := make(map[int]record, len(keys))
			for _, key := range keys {
				values[key] = record{Name: fmt.Sprintf("user-%d", key)}
			}
			return values, nil
		})
	for keys, err := range iterx.Batch(source.Keys(), 2) {
		if err != nil {
			fmt.Println(err)
			return
		}
		values, err := source.MGet(keys)
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, key := range keys {
			fmt.Println(key, values[key].Name)
		}
	}
	// Output:
	// 1 user-1
	// 2 user-2
}

func ExampleNewLookupSource() {
	// The caller already has candidate identities, so no page loader is needed.
	source := iterx.NewLookupSource(context.Background(),
		func(_ context.Context, keys []string) (map[string]int, error) {
			values := make(map[string]int)
			for _, key := range keys {
				if key == "known" {
					values[key] = 0
				}
			}
			return values, nil
		})
	value, found, err := source.Get("known")
	fmt.Println(value, found, err)
	value, found, err = source.Get("missing")
	fmt.Println(value, found, err)
	// Output:
	// 0 true <nil>
	// 0 false <nil>
}

func ExampleNewMapSource() {
	values := map[string]int{"b": 2, "a": 1}
	source := iterx.NewMapSource(context.Background(), values,
		func(a, b string) bool { return a < b })
	values["a"] = 99
	for key, err := range source.Keys() {
		if err != nil {
			fmt.Println(err)
			return
		}
		value, found, err := source.Get(key)
		fmt.Println(key, value, found, err)
	}
	// Output:
	// a 1 true <nil>
	// b 2 true <nil>
}
