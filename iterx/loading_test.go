package iterx_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"strconv"
	"testing"

	"github.com/compforge/go-stdx/iterx"
)

// +case:id=lazy_pages,desc=`Load only when the preceding batch is consumed`,expect=`No eager load or prefetch`
func TestLoadingIteratorLoadsOnDemand(t *testing.T) {
	calls := 0
	pages := [][]int{{1, 2}, {3, 4}, {5}}
	var sequence iter.Seq2[int, error] = iterx.NewLoadingIterator(t.Context(), func(context.Context) ([]int, bool, error) {
		page := pages[calls]
		calls++
		return page, calls < len(pages), nil
	})
	if calls != 0 {
		t.Fatal("constructor loaded a page")
	}
	var got []int
	for value, err := range sequence {
		if err != nil {
			t.Fatal(err)
		}
		if want := (value + 1) / 2; calls != want {
			t.Fatalf("at item %d: calls=%d want=%d", value, calls, want)
		}
		got = append(got, value)
	}
	if !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5}) || calls != 3 {
		t.Fatalf("items=%v calls=%d", got, calls)
	}
	for range sequence {
		t.Fatal("completed iterator yielded again")
	}
	if calls != 3 {
		t.Fatal("completed iterator loaded again")
	}
}

func TestLoadingIteratorEarlyBreak(t *testing.T) {
	for _, stopAt := range []int{1, 3} {
		t.Run(strconv.Itoa(stopAt), func(t *testing.T) {
			calls := 0
			sequence := iterx.NewLoadingIterator(t.Context(), func(context.Context) ([]int, bool, error) {
				calls++
				return []int{1, 2, 3}, true, nil
			})
			for value, err := range sequence {
				if err != nil {
					t.Fatal(err)
				}
				if value == stopAt {
					break
				}
			}
			for range sequence {
				t.Fatal("early-stopped iterator resumed")
			}
			if calls != 1 {
				t.Fatalf("early break caused %d loads", calls)
			}
		})
	}
}

func TestLoadingIteratorEmptyPages(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages [][]int
		want  []int
	}{
		{"empty", [][]int{nil}, nil},
		{"empty intermediate pages", [][]int{nil, {}, {1}, nil}, []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			sequence := iterx.NewLoadingIterator(t.Context(), func(context.Context) ([]int, bool, error) {
				page := tc.pages[calls]
				calls++
				return page, calls < len(tc.pages), nil
			})
			var got []int
			for value, err := range sequence {
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
			if !reflect.DeepEqual(got, tc.want) || calls != len(tc.pages) {
				t.Fatalf("items=%v calls=%d", got, calls)
			}
		})
	}
}

func TestLoadingIteratorPageError(t *testing.T) {
	failure := errors.New("page unavailable")
	calls, failures := 0, 0
	sequence := iterx.NewLoadingIterator(t.Context(), func(context.Context) ([]int, bool, error) {
		calls++
		if calls == 1 {
			return []int{1}, true, nil
		}
		return []int{99}, true, failure
	})
	var got []int
	for value, err := range sequence {
		if err != nil {
			if !errors.Is(err, failure) || value != 0 {
				t.Fatalf("error item=(%d, %v)", value, err)
			}
			failures++
			continue // The iterator must stop even if the consumer continues.
		}
		got = append(got, value)
	}
	for range sequence {
		t.Fatal("failed iterator yielded again")
	}
	if !reflect.DeepEqual(got, []int{1}) || failures != 1 || calls != 2 {
		t.Fatalf("items=%v errors=%d calls=%d", got, failures, calls)
	}
}

func TestLoadingIteratorCancellation(t *testing.T) {
	for _, when := range []string{"before load", "during load", "between items", "empty page"} {
		t.Run(when, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if when == "before load" {
				cancel()
			}
			calls, items, failures := 0, 0, 0
			sequence := iterx.NewLoadingIterator(ctx, func(gotCtx context.Context) ([]int, bool, error) {
				if gotCtx != ctx {
					t.Fatal("loader received a different context")
				}
				calls++
				if when == "during load" || when == "empty page" {
					cancel()
				}
				if when == "empty page" {
					return nil, false, nil
				}
				return []int{1, 2}, true, nil
			})
			for value, err := range sequence {
				if err != nil {
					if !errors.Is(err, context.Canceled) || value != 0 {
						t.Fatalf("error item=(%d, %v)", value, err)
					}
					failures++
					continue
				}
				items++
				cancel()
			}
			wantCalls, wantItems := 1, 0
			if when == "before load" {
				wantCalls = 0
			}
			if when == "between items" {
				wantItems = 1
			}
			if calls != wantCalls || items != wantItems || failures != 1 {
				t.Fatalf("calls=%d items=%d errors=%d", calls, items, failures)
			}
		})
	}
}

func TestLoadingIteratorNilPage(t *testing.T) {
	if got := iterx.NewLoadingIterator[int](t.Context(), nil); got != nil {
		t.Fatal("nil page must return nil iterator")
	}
}

// +case:id=batch_buffers,desc=`Retain batches across page boundaries`,expect=`Order and duplicates survive; yielded slices do not share buffers`
func TestBatchPagesAndOwnedBuffers(t *testing.T) {
	for _, size := range []int{1, 2, 3, 5} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			pages := [][]int{{1, 1, 2}, {}, {3, 4}}
			calls := 0
			items := iterx.NewLoadingIterator(t.Context(), func(context.Context) ([]int, bool, error) {
				page := pages[calls]
				calls++
				return page, calls < len(pages), nil
			})
			batches := iterx.Batch(items, size)
			if calls != 0 {
				t.Fatal("construction read input")
			}
			var saved [][]int
			for batch, err := range batches {
				if err != nil {
					t.Fatal(err)
				}
				if len(batch) == 0 || len(batch) > size || cap(batch) != len(batch) {
					t.Fatalf("invalid batch len=%d cap=%d", len(batch), cap(batch))
				}
				saved = append(saved, batch)
			}
			var got []int
			for _, batch := range saved {
				got = append(got, batch...)
			}
			if !reflect.DeepEqual(got, []int{1, 1, 2, 3, 4}) {
				t.Fatalf("retained batches=%v", saved)
			}
			if len(saved) > 1 {
				saved[1][0] = 99
				if saved[0][0] != 1 {
					t.Fatal("batches share backing storage")
				}
			}
			for range batches {
				t.Fatal("batch iterator restarted")
			}
			if calls != 3 {
				t.Fatalf("page calls=%d", calls)
			}
		})
	}
}

func TestBatchEarlyBreak(t *testing.T) {
	reads := 0
	items := func(yield func(int, error) bool) {
		for i := 0; i < 10; i++ {
			reads++
			if !yield(i, nil) {
				return
			}
		}
	}
	batches := iterx.Batch[int](items, 2)
	for batch, err := range batches {
		if err != nil || !reflect.DeepEqual(batch, []int{0, 1}) {
			t.Fatalf("batch=%v err=%v", batch, err)
		}
		break
	}
	for range batches {
		t.Fatal("stopped batch iterator restarted reusable input")
	}
	if reads != 2 {
		t.Fatalf("read ahead: %d", reads)
	}
}

// +case:id=batch_failure,desc=`A source fails partway through a batch`,expect=`Discard incomplete batch, emit one error and stop reading`
func TestBatchDiscardsIncompleteBatchOnError(t *testing.T) {
	failure := errors.New("unavailable")
	for _, beforeError := range []int{0, 1, 2, 3} {
		t.Run(strconv.Itoa(beforeError), func(t *testing.T) {
			items := func(yield func(int, error) bool) {
				for i := 0; i < beforeError; i++ {
					if !yield(i, nil) {
						return
					}
				}
				if yield(99, failure) {
					t.Fatal("continued after error")
				}
			}
			count, failures := 0, 0
			batches := iterx.Batch[int](items, 2)
			for batch, err := range batches {
				if err != nil {
					if !errors.Is(err, failure) || batch != nil {
						t.Fatalf("batch=%v err=%v", batch, err)
					}
					failures++
					continue
				}
				count += len(batch)
			}
			if count != beforeError/2*2 || failures != 1 {
				t.Fatalf("count=%d errors=%d", count, failures)
			}
			for range batches {
				t.Fatal("failed iterator restarted")
			}
		})
	}
}

func TestBatchCancellationAndEmptyInput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	items := iterx.NewLoadingIterator(ctx, func(context.Context) ([]int, bool, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return []int{calls}, true, nil
	})
	failures := 0
	for batch, err := range iterx.Batch(items, 2) {
		if !errors.Is(err, context.Canceled) || batch != nil {
			t.Fatalf("batch=%v err=%v", batch, err)
		}
		failures++
	}
	if failures != 1 || calls != 2 {
		t.Fatalf("errors=%d calls=%d", failures, calls)
	}
	if iterx.Batch[int](nil, 2) != nil {
		t.Fatal("nil input must return nil")
	}
	for range iterx.Batch[int](func(func(int, error) bool) {}, 2) {
		t.Fatal("empty input yielded a batch")
	}
	for _, size := range []int{0, -1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid size did not panic")
				}
			}()
			iterx.Batch[int](nil, size)
		})
	}
}
