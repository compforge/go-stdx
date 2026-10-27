package iterx

import (
	"context"
	"iter"

	"github.com/compforge/go-stdx/slicesx"
)

// KeyFunc extracts an object's stable identity. Sources combined by UniqueKeys
// must agree on the meaning of their keys, even when their value types differ.
type KeyFunc[K comparable, V any] func(V) K

// MGetFunc looks up identities independently of the page loader's candidate
// filters and cursor. An omitted key means confirmed absence only when err is
// nil. On error, LoadingSource discards the entire result.
//
// The returned map must contain only requested keys, and each value's KeyFunc
// must equal its map key. Implementations must honor ctx and release per-query
// resources before returning.
type MGetFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// LoadingSource combines a single-use candidate traversal with independent
// batch lookups. It does not cache objects, retry, or guarantee a snapshot
// across reads. It must not be used concurrently.
//
// Construct a new source with fresh page state for each traversal. Get and MGet
// remain usable before, during and after traversal while ctx is active.
type LoadingSource[K comparable, V any] struct {
	ctx   context.Context
	items LoadingIterator[V]
	mget  MGetFunc[K, V]
	key   KeyFunc[K, V]
}

// NewLoadingSource binds the loaders without performing I/O. ctx must be
// non-nil. All three callbacks are required; a nil callback returns nil.
// Pagination, batch size and candidate filters belong to page.
// +spec=`Source construction performs no I/O; lookup never advances enumeration`
func NewLoadingSource[K comparable, V any](ctx context.Context, page PageFunc[V], mget MGetFunc[K, V], key KeyFunc[K, V]) *LoadingSource[K, V] {
	if page == nil || mget == nil || key == nil {
		return nil
	}
	return &LoadingSource[K, V]{
		ctx:   ctx,
		items: NewLoadingIterator(ctx, page),
		mget:  mget,
		key:   key,
	}
}

// Keys lazily extracts candidate identities, preserving duplicates and order.
// All calls share the source's single-use traversal: after it completes or is
// stopped early, further traversals yield nothing. A nil source returns nil,
// which must not be ranged directly.
func (s *LoadingSource[K, V]) Keys() iter.Seq2[K, error] {
	if s == nil {
		return nil
	}
	return func(yield func(K, error) bool) {
		for value, err := range s.items {
			if err != nil {
				var zero K
				yield(zero, err)
				return
			}
			if !yield(s.key(value), nil) {
				return
			}
		}
	}
}

// MGet deduplicates keys in first-occurrence order before invoking the loader.
// Empty input returns nil without I/O. Missing result keys mean absence; a
// loader or context error returns no partial result. No results are cached.
// +spec=`Batch lookup preserves absence versus failure and does not return partial results on error`
func (s *LoadingSource[K, V]) MGet(keys []K) (map[K]V, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := s.mget(s.ctx, slicesx.Uniq(keys))
	if err == nil {
		err = s.ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	return values, nil
}

// Get is a single-key MGet. found distinguishes an absent key from a present
// zero value. Repeated calls perform repeated lookups; batch callers use MGet.
func (s *LoadingSource[K, V]) Get(key K) (V, bool, error) {
	values, err := s.MGet([]K{key})
	if err != nil {
		var zero V
		return zero, false, err
	}
	value, found := values[key]
	return value, found, nil
}

// KeySource enumerates candidate identities. Keys must defer loading until
// iteration, honor early termination, and report read failures as errors.
// Cancellation belongs to the source. UniqueKeys accepts a nil iterator as an
// empty source.
type KeySource[K comparable] interface {
	Keys() iter.Seq2[K, error]
}

// UniqueKeys lazily visits sources in argument order and yields each key only
// on its first occurrence. It never looks up values. Storage is O(unique keys);
// this is not a sorted merge and it does not interleave sources.
//
// The returned iterator is single-use and must not be traversed concurrently.
// Early termination leaves subsequent sources unstarted. A source error yields
// (zero K, err) once and ends traversal; already yielded keys are not rolled
// back. Nil sources and nil iterators are skipped. No sources returns nil,
// which must not be ranged directly.
// +spec=`UniqueKeys preserves first occurrence across sources and never starts a later source after early exit or error`
func UniqueKeys[K comparable](sources ...KeySource[K]) iter.Seq2[K, error] {
	if len(sources) == 0 {
		return nil
	}
	started := false
	return func(yield func(K, error) bool) {
		if started {
			return
		}
		started = true
		seen := make(map[K]struct{})
		for _, source := range sources {
			if source == nil {
				continue
			}
			keys := source.Keys()
			if keys == nil {
				continue
			}
			for key, err := range keys {
				if err != nil {
					var zero K
					yield(zero, err)
					return
				}
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				if !yield(key, nil) {
					return
				}
			}
		}
	}
}
