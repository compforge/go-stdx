// Package iterx extends standard iterators with lazy page loading and keyed sources.
package iterx

import (
	"context"
	"iter"
)

// PageFunc loads the next batch. It owns query conditions, page size, and cursor
// state. Each iterator needs its own paging state. An empty batch with hasMore
// true is allowed, but the loader must still advance toward the end.
//
// The loader must honor ctx and release per-query resources before returning.
// If err is non-nil, the iterator discards any items returned with it.
type PageFunc[T any] func(ctx context.Context) (items []T, hasMore bool, err error)

// LoadingIterator is a standard iterator over lazily loaded items and errors.
// Iterators returned by NewLoadingIterator are single-use and not safe for
// concurrent traversal. After any traversal ends, including an early break,
// subsequent traversals yield nothing.
type LoadingIterator[T any] = iter.Seq2[T, error]

// NewLoadingIterator binds page without calling it. Iteration loads one batch
// at a time and yields its items in order, including items in the final batch.
// Loading is synchronous; there is no prefetch or automatic retry. A load or
// context error yields (zero T, err) once and ends the iterator.
//
// ctx must be non-nil. A nil page returns a nil iterator, which must not be ranged.
// Breaking stops this iterator; restarting requires a new loader and iterator.
// Loader cursor progress represents fetched pages, not processed items, and
// must not be used as a processing checkpoint after an early break.
//
// +spec=`Construction performs no I/O; early termination never loads another page`
// +spec=`Each iterator is single-use; a failed page yields only its error`
func NewLoadingIterator[T any](ctx context.Context, page PageFunc[T]) LoadingIterator[T] {
	if page == nil {
		return nil
	}
	started := false
	return func(yield func(T, error) bool) {
		if started {
			return
		}
		started = true
		var zero T
		for {
			if err := ctx.Err(); err != nil {
				yield(zero, err)
				return
			}
			items, more, err := page(ctx)
			if err == nil {
				err = ctx.Err()
			}
			if err != nil {
				yield(zero, err)
				return
			}
			for _, item := range items {
				if err := ctx.Err(); err != nil {
					yield(zero, err)
					return
				}
				if !yield(item, nil) {
					return
				}
			}
			if !more {
				return
			}
		}
	}
}

// Batch lazily groups items into slices of at most size, preserving order and
// duplicates. It yields a final nonempty partial batch on normal exhaustion.
// An upstream error discards the incomplete batch, yields (nil, err) once and
// stops. Previously yielded batches remain valid and are not rolled back.
//
// Each batch owns its backing array and can be retained or modified by callers.
// The iterator is single-use, synchronous and not safe for concurrent traversal;
// stopping never reads further items. Cancellation belongs to the input iterator.
// Batch panics if size < 1; with a valid size, nil input returns nil.
// +spec=`Batch does not prefetch, reuse yielded buffers, or expose partial batches on error`
func Batch[T any](items iter.Seq2[T, error], size int) iter.Seq2[[]T, error] {
	if size < 1 {
		panic("iterx.Batch: size must be positive")
	}
	if items == nil {
		return nil
	}
	started := false
	return func(yield func([]T, error) bool) {
		if started {
			return
		}
		started = true
		batch := make([]T, 0, size)
		for item, err := range items {
			if err != nil {
				yield(nil, err)
				return
			}
			batch = append(batch, item)
			if len(batch) == size {
				if !yield(batch, nil) {
					return
				}
				batch = make([]T, 0, size)
			}
		}
		if len(batch) > 0 {
			yield(batch[:len(batch):len(batch)], nil)
		}
	}
}
