package iterx_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"testing"

	"github.com/compforge/go-stdx/iterx"
)

// +case:id=source_lookup_independent,desc=`Look up an identity outside paged candidates before and during traversal`,expect=`Lookup does not consume pages or inherit candidate filters`
func TestLoadingSourceLookupIndependentOfTraversal(t *testing.T) {
	pageCalls, lookupCalls := 0, 0
	pages := [][]int{{1, 1}, {}, {2}}
	source := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]int, bool, error) {
			page := pages[pageCalls]
			pageCalls++
			return page, pageCalls < len(pages), nil
		},
		func(_ context.Context, keys []int) (map[int]int, error) {
			lookupCalls++
			result := make(map[int]int)
			for _, key := range keys {
				result[key] = key
			}
			return result, nil
		},
		func(v int) int { return v },
	)
	keys := source.Keys()
	if pageCalls != 0 || lookupCalls != 0 {
		t.Fatal("constructing the source or iterator performed I/O")
	}
	checkLookup := func() {
		t.Helper()
		before := pageCalls
		value, found, err := source.Get(99)
		if err != nil || !found || value != 99 || pageCalls != before {
			t.Fatalf("lookup=(%d,%v,%v) pages=%d, before=%d", value, found, err, pageCalls, before)
		}
	}
	checkLookup()
	var got []int
	for key, err := range keys {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, key)
		checkLookup()
	}
	checkLookup()
	for range source.Keys() {
		t.Fatal("a second Keys call restarted the source")
	}
	if !reflect.DeepEqual(got, []int{1, 1, 2}) || pageCalls != 3 || lookupCalls != 5 {
		t.Fatalf("keys=%v pages=%d lookups=%d", got, pageCalls, lookupCalls)
	}
}

func TestLoadingSourceMGet(t *testing.T) {
	calls := 0
	source := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]int, bool, error) { return nil, false, nil },
		func(_ context.Context, keys []int) (map[int]int, error) {
			calls++
			if !reflect.DeepEqual(keys, []int{2, 0, 9}) {
				t.Fatalf("lookup keys=%v", keys)
			}
			return map[int]int{0: 0, 2: 2}, nil
		},
		func(v int) int { return v },
	)
	for _, empty := range [][]int{nil, {}} {
		got, err := source.MGet(empty)
		if err != nil || got != nil || calls != 0 {
			t.Fatalf("empty lookup=%v err=%v calls=%d", got, err, calls)
		}
	}
	input := []int{2, 0, 2, 9, 0}
	got, err := source.MGet(input)
	if err != nil || !reflect.DeepEqual(got, map[int]int{0: 0, 2: 2}) || calls != 1 {
		t.Fatalf("lookup=%v err=%v calls=%d", got, err, calls)
	}
	if !reflect.DeepEqual(input, []int{2, 0, 2, 9, 0}) {
		t.Fatalf("input mutated: %v", input)
	}
}

func TestLoadingSourceGetPresenceAndFreshLookup(t *testing.T) {
	calls := 0
	values := map[int]int{0: 0}
	source := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]int, bool, error) { return nil, false, nil },
		func(_ context.Context, keys []int) (map[int]int, error) {
			calls++
			result := make(map[int]int)
			for _, key := range keys {
				if value, found := values[key]; found {
					result[key] = value
				}
			}
			return result, nil
		}, func(v int) int { return v },
	)
	if v, found, err := source.Get(0); err != nil || !found || v != 0 {
		t.Fatalf("present zero=(%d,%v,%v)", v, found, err)
	}
	delete(values, 0)
	if v, found, err := source.Get(0); err != nil || found || v != 0 {
		t.Fatalf("deleted zero=(%d,%v,%v)", v, found, err)
	}
	if calls != 2 {
		t.Fatalf("Get reused a cached value: calls=%d", calls)
	}
}

// +case:id=source_lookup_failure,desc=`A lookup returns objects and an error`,expect=`Neither MGet nor Get exposes partial objects as usable facts`
func TestLoadingSourceMGetFailureAndCancellation(t *testing.T) {
	failure := errors.New("lookup unavailable")
	for _, mode := range []string{"loader error", "cancel before", "cancel during"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			source := iterx.NewLoadingSource(ctx,
				func(context.Context) ([]int, bool, error) { return nil, false, nil },
				func(gotCtx context.Context, keys []int) (map[int]int, error) {
					calls++
					if gotCtx != ctx {
						t.Fatal("loader did not receive bound context")
					}
					if mode == "cancel during" {
						cancel()
						return map[int]int{1: 1}, nil
					}
					return map[int]int{1: 1}, failure
				}, func(v int) int { return v },
			)
			want := failure
			if mode != "loader error" {
				want = context.Canceled
			}
			if mode == "cancel before" {
				cancel()
			}
			got, err := source.MGet([]int{1})
			if got != nil || !errors.Is(err, want) {
				t.Fatalf("lookup=%v err=%v, want %v", got, err, want)
			}
			value, found, err := source.Get(1)
			if value != 0 || found || !errors.Is(err, want) {
				t.Fatalf("Get=(%d,%v,%v)", value, found, err)
			}
			wantCalls := map[string]int{"loader error": 2, "cancel before": 0, "cancel during": 1}[mode]
			if calls != wantCalls {
				t.Fatalf("loader calls=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestUniqueKeysHeterogeneousSources(t *testing.T) {
	type record struct{ id int }
	type runtime struct{ carrier int }
	lookups := 0
	db := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]record, bool, error) { return []record{{3}, {1}, {3}}, false, nil },
		func(context.Context, []int) (map[int]record, error) { lookups++; return nil, nil },
		func(v record) int { return v.id },
	)
	actual := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]runtime, bool, error) { return []runtime{{1}, {0}, {2}, {2}}, false, nil },
		func(context.Context, []int) (map[int]runtime, error) { lookups++; return nil, nil },
		func(v runtime) int { return v.carrier },
	)
	var typedNil *iterx.LoadingSource[int, record]
	got := collectKeys(t, iterx.UniqueKeys[int](nil, db, typedNil, actual))
	if !reflect.DeepEqual(got, []int{3, 1, 0, 2}) || lookups != 0 {
		t.Fatalf("keys=%v lookups=%d", got, lookups)
	}
}

// +case:id=unique_keys_early_exit,desc=`Stop after one key in the first source`,expect=`No next page or later source is started`
func TestUniqueKeysEarlyExit(t *testing.T) {
	pageCalls, laterCalls := 0, 0
	source := iterx.NewLoadingSource(t.Context(),
		func(context.Context) ([]int, bool, error) { pageCalls++; return []int{1, 2}, true, nil },
		func(context.Context, []int) (map[int]int, error) { t.Fatal("unexpected lookup"); return nil, nil },
		func(v int) int { return v },
	)
	later := testKeySource(func() iter.Seq2[int, error] {
		laterCalls++
		return func(yield func(int, error) bool) { yield(3, nil) }
	})
	keys := iterx.UniqueKeys[int](source, later)
	if pageCalls != 0 || laterCalls != 0 {
		t.Fatal("UniqueKeys eagerly started enumeration")
	}
	for key, err := range keys {
		if err != nil || key != 1 {
			t.Fatalf("key=%d err=%v", key, err)
		}
		break
	}
	for range keys {
		t.Fatal("UniqueKeys resumed after early break")
	}
	for range source.Keys() {
		t.Fatal("source resumed after early break")
	}
	if pageCalls != 1 || laterCalls != 0 {
		t.Fatalf("pages=%d later=%d", pageCalls, laterCalls)
	}
}

func TestUniqueKeysPageFailureAndCancellation(t *testing.T) {
	failure := errors.New("page unavailable")
	for _, mode := range []string{"page error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			pageCalls, keyCalls, laterCalls := 0, 0, 0
			source := iterx.NewLoadingSource(ctx,
				func(context.Context) ([]int, bool, error) {
					pageCalls++
					if pageCalls == 1 {
						return []int{1}, true, nil
					}
					return []int{99}, false, failure
				},
				func(context.Context, []int) (map[int]int, error) { return nil, nil },
				func(v int) int { keyCalls++; return v },
			)
			later := testKeySource(func() iter.Seq2[int, error] { laterCalls++; return nil })
			keys := iterx.UniqueKeys[int](source, later)
			want := failure
			if mode == "cancel" {
				want = context.Canceled
			}
			var got []int
			failures := 0
			for key, err := range keys {
				if err != nil {
					if key != 0 || !errors.Is(err, want) {
						t.Fatalf("key=%d err=%v", key, err)
					}
					failures++
					continue
				}
				got = append(got, key)
				if mode == "cancel" {
					cancel()
				}
			}
			for range keys {
				t.Fatal("failed iterator restarted")
			}
			if !reflect.DeepEqual(got, []int{1}) || failures != 1 || keyCalls != 1 || laterCalls != 0 {
				t.Fatalf("keys=%v failures=%d key calls=%d later=%d", got, failures, keyCalls, laterCalls)
			}
			if mode == "cancel" && pageCalls != 1 {
				t.Fatalf("loaded after cancellation: %d", pageCalls)
			}
		})
	}
}

func TestUniqueKeysSingleSourceAndSingleUse(t *testing.T) {
	calls := 0
	// A reusable source must not make the combined iterator reusable.
	source := testKeySource(func() iter.Seq2[int, error] {
		calls++
		return func(yield func(int, error) bool) {
			for _, key := range []int{2, 2, 1} {
				if !yield(key, nil) {
					return
				}
			}
		}
	})
	keys := iterx.UniqueKeys[int](source)
	if got := collectKeys(t, keys); !reflect.DeepEqual(got, []int{2, 1}) {
		t.Fatalf("keys=%v", got)
	}
	if got := collectKeys(t, keys); len(got) != 0 || calls != 1 {
		t.Fatalf("repeat=%v calls=%d", got, calls)
	}
}

func TestLoadingSourceNilCallbacks(t *testing.T) {
	page := func(context.Context) ([]int, bool, error) { return nil, false, nil }
	mget := func(context.Context, []int) (map[int]int, error) { return nil, nil }
	key := func(v int) int { return v }
	if iterx.NewLoadingSource[int, int](t.Context(), nil, mget, key) != nil ||
		iterx.NewLoadingSource[int, int](t.Context(), page, nil, key) != nil ||
		iterx.NewLoadingSource[int, int](t.Context(), page, mget, nil) != nil {
		t.Fatal("nil callback did not return nil source")
	}
	if iterx.UniqueKeys[int]() != nil {
		t.Fatal("zero sources did not return nil iterator")
	}
	if got := collectKeys(t, iterx.UniqueKeys[int](nil)); len(got) != 0 {
		t.Fatalf("nil source keys=%v", got)
	}
}

type testKeySource func() iter.Seq2[int, error]

func (s testKeySource) Keys() iter.Seq2[int, error] { return s() }

func collectKeys(t *testing.T, keys iter.Seq2[int, error]) []int {
	t.Helper()
	var result []int
	for key, err := range keys {
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, key)
	}
	return result
}

// +case:id=key_source_pages,desc=`Enumerate lightweight keys and look up unrelated full objects`,expect=`No value fetch during paging and no page advance during lookup`
func TestLoadingSourceFromKeys(t *testing.T) {
	pages := [][]int{{2, 2}, {}, {1}}
	pageCalls, lookups := 0, 0
	source := iterx.NewLoadingSourceFromKeys(t.Context(),
		func(context.Context) ([]int, bool, error) {
			page := pages[pageCalls]
			pageCalls++
			return page, pageCalls < len(pages), nil
		},
		func(_ context.Context, keys []int) (map[int]string, error) {
			lookups++
			return map[int]string{99: "outside candidates"}, nil
		})
	if pageCalls != 0 || lookups != 0 {
		t.Fatal("construction performed I/O")
	}
	value, found, err := source.Get(99)
	if err != nil || !found || value != "outside candidates" || pageCalls != 0 {
		t.Fatalf("lookup=(%q,%t,%v) pages=%d", value, found, err, pageCalls)
	}
	if got := collectKeys(t, source.Keys()); !reflect.DeepEqual(got, []int{2, 2, 1}) {
		t.Fatalf("keys=%v", got)
	}
	if got := collectKeys(t, source.Keys()); len(got) != 0 || pageCalls != 3 || lookups != 1 {
		t.Fatalf("repeat=%v pages=%d lookups=%d", got, pageCalls, lookups)
	}
}

func TestKeySourceEarlyBreakAndFailure(t *testing.T) {
	failure := errors.New("page failed")
	for _, mode := range []string{"break", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			source := iterx.NewLoadingSourceFromKeys(ctx,
				func(context.Context) ([]int, bool, error) {
					calls++
					if calls == 2 {
						return []int{99}, true, failure
					}
					return []int{1, 2}, true, nil
				}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("enumeration looked up values")
					return nil, nil
				})
			var keys []int
			failures := 0
			for key, err := range source.Keys() {
				if err != nil {
					want := failure
					if mode == "cancel" {
						want = context.Canceled
					}
					if !errors.Is(err, want) || key != 0 {
						t.Fatalf("key=%d err=%v", key, err)
					}
					failures++
					continue
				}
				keys = append(keys, key)
				if mode == "break" {
					break
				}
				if mode == "cancel" {
					cancel()
				}
			}
			for range source.Keys() {
				t.Fatal("source restarted")
			}
			wantCalls, wantKeys, wantFailures := 1, []int{1}, 1
			if mode == "error" {
				wantCalls, wantKeys = 2, []int{1, 2}
			}
			if mode == "break" {
				wantFailures = 0
			}
			if calls != wantCalls || !reflect.DeepEqual(keys, wantKeys) || failures != wantFailures {
				t.Fatalf("calls=%d keys=%v errors=%d", calls, keys, failures)
			}
		})
	}
}

func TestLookupSourceHasNoCandidates(t *testing.T) {
	calls := 0
	source := iterx.NewLookupSource(t.Context(), func(_ context.Context, keys []int) (map[int]string, error) {
		calls++
		if !reflect.DeepEqual(keys, []int{1, 0}) {
			t.Fatalf("lookup keys=%v", keys)
		}
		return map[int]string{0: ""}, nil
	})
	if source.Keys() == nil {
		t.Fatal("lookup-only source needs a rangeable empty iterator")
	}
	if keys := collectKeys(t, source.Keys()); len(keys) != 0 || calls != 0 {
		t.Fatalf("keys=%v calls=%d", keys, calls)
	}
	got, err := source.MGet([]int{1, 0, 1})
	if err != nil || !reflect.DeepEqual(got, map[int]string{0: ""}) || calls != 1 {
		t.Fatalf("values=%v err=%v calls=%d", got, err, calls)
	}
}

// +case:id=map_source_snapshot,desc=`Mutate the original and returned maps after construction`,expect=`Source bindings remain stable while reference-valued contents stay shared`
func TestMapSourceSnapshotAndOrder(t *testing.T) {
	original := map[int]string{2: "two", 0: ""}
	source := iterx.NewMapSource(t.Context(), original, func(a, b int) bool { return a > b })
	delete(original, 2)
	original[0] = "changed"
	original[1] = "new"
	if keys := collectKeys(t, source.Keys()); !reflect.DeepEqual(keys, []int{2, 0}) {
		t.Fatalf("keys=%v", keys)
	}
	got, err := source.MGet([]int{0, 1, 2, 2})
	if err != nil || !reflect.DeepEqual(got, map[int]string{0: "", 2: "two"}) {
		t.Fatalf("values=%v err=%v", got, err)
	}
	got[2] = "mutated"
	delete(got, 0)
	if value, found, err := source.Get(2); value != "two" || !found || err != nil {
		t.Fatalf("lookup=(%q,%t,%v)", value, found, err)
	}
	if value, found, err := source.Get(0); value != "" || !found || err != nil {
		t.Fatalf("zero=(%q,%t,%v)", value, found, err)
	}
	if _, found, err := source.Get(1); found || err != nil {
		t.Fatalf("missing=(%t,%v)", found, err)
	}
	if keys := collectKeys(t, source.Keys()); len(keys) != 0 {
		t.Fatalf("restarted keys=%v", keys)
	}

	value := 7
	refs := iterx.NewMapSource(t.Context(), map[int]*int{1: &value}, nil)
	value = 8
	gotRef, found, err := refs.Get(1)
	if err != nil || !found || gotRef != &value || *gotRef != 8 {
		t.Fatalf("reference=(%v,%t,%v)", gotRef, found, err)
	}
}

func TestMapSourceCompositeKeysAndNoOrdering(t *testing.T) {
	type key struct{ Namespace, Name string }
	values := map[key]string{{"a", "same"}: "a", {"b", "same"}: "b"}
	source := iterx.NewMapSource(t.Context(), values, func(a, b key) bool { return a.Namespace < b.Namespace })
	var keys []key
	for k, err := range source.Keys() {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if !reflect.DeepEqual(keys, []key{{"a", "same"}, {"b", "same"}}) {
		t.Fatalf("keys=%v", keys)
	}
	unordered := iterx.NewMapSource(t.Context(), values, nil)
	seen := make(map[key]bool)
	for k, err := range unordered.Keys() {
		if err != nil {
			t.Fatal(err)
		}
		seen[k] = true
	}
	if len(seen) != 2 || !seen[key{"a", "same"}] || !seen[key{"b", "same"}] {
		t.Fatalf("keys=%v", seen)
	}
}

func TestSourceAdaptersCancellationAndNil(t *testing.T) {
	page := func(context.Context) ([]int, bool, error) { return nil, false, nil }
	mget := func(context.Context, []int) (map[int]int, error) {
		t.Fatal("cancelled source performed lookup")
		return nil, nil
	}
	if iterx.NewLoadingSourceFromKeys[int, int](t.Context(), nil, mget) != nil ||
		iterx.NewLoadingSourceFromKeys[int, int](t.Context(), page, nil) != nil ||
		iterx.NewLookupSource[int, int](t.Context(), nil) != nil ||
		iterx.NewMapSource[int, int](t.Context(), nil, nil) != nil {
		t.Fatal("nil input did not return nil source")
	}
	empty := iterx.NewMapSource(t.Context(), map[int]int{}, nil)
	if empty == nil {
		t.Fatal("empty non-nil map returned nil source")
	}
	if keys := collectKeys(t, empty.Keys()); len(keys) != 0 {
		t.Fatalf("empty map keys=%v", keys)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, source := range []*iterx.LoadingSource[int, int]{
		iterx.NewLoadingSourceFromKeys(ctx, page, mget),
		iterx.NewLookupSource(ctx, mget),
		iterx.NewMapSource(ctx, map[int]int{1: 1}, nil),
	} {
		failures := 0
		for key, err := range source.Keys() {
			if key != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("key=%d err=%v", key, err)
			}
			failures++
		}
		if failures != 1 {
			t.Fatalf("errors=%d", failures)
		}
		if values, err := source.MGet([]int{1}); values != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("values=%v err=%v", values, err)
		}
	}
}
