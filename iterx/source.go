package iterx

import (
	"context"
	"iter"
	"maps"
	"sort"

	"github.com/compforge/go-stdx/slicesx"
)

// KeyFunc extracts an object's stable identity. Sources combined by UniqueKeys
// must agree on the meaning of their keys, even when their value types differ.
type KeyFunc[K comparable, V any] func(V) K

// MGetFunc looks up identities independently of the page loader's candidate
// filters and cursor. An omitted key means confirmed absence only when err is
// nil. On error, LoadingSource discards the entire result.
//
// The returned map must contain only requested keys and use the same identity
// as candidate enumeration (and KeyFunc, when supplied). Implementations must
// honor ctx and release per-query resources before returning.
type MGetFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// LoadingSource combines a single-use candidate traversal with independent
// batch lookups. It adds no object cache, retry, or snapshot guarantee across
// reads; those properties belong to the loaders. It must not be used concurrently.
//
// Construct a new source with fresh page state for each traversal. Get and MGet
// remain usable before, during and after traversal while ctx is active.
type LoadingSource[K comparable, V any] struct {
	ctx  context.Context
	keys LoadingIterator[K]
	mget MGetFunc[K, V]
}

// NewLoadingSource binds the loaders without performing I/O. ctx must be
// non-nil. All three callbacks are required; a nil callback returns nil.
// Pagination, batch size and candidate filters belong to page.
// +spec=`Source construction performs no I/O; lookup never advances enumeration`
func NewLoadingSource[K comparable, V any](ctx context.Context, page PageFunc[V], mget MGetFunc[K, V], key KeyFunc[K, V]) *LoadingSource[K, V] {
	if page == nil || mget == nil || key == nil {
		return nil
	}
	items := NewLoadingIterator(ctx, page)
	keys := func(yield func(K, error) bool) {
		for value, err := range items {
			if err != nil {
				var zero K
				yield(zero, err)
				return
			}
			if !yield(key(value), nil) {
				return
			}
		}
	}
	return &LoadingSource[K, V]{ctx: ctx, keys: keys, mget: mget}
}

// NewLoadingSourceFromKeys binds a key page loader and an independent batch
// lookup without fetching or constructing values during enumeration. ctx must
// be non-nil. Either nil callback returns nil. Use NewLookupSource when no
// candidate traversal is needed.
// +spec=`Key-only enumeration does not load values or advance through lookups`
func NewLoadingSourceFromKeys[K comparable, V any](ctx context.Context, page PageFunc[K], mget MGetFunc[K, V]) *LoadingSource[K, V] {
	if page == nil || mget == nil {
		return nil
	}
	return &LoadingSource[K, V]{ctx: ctx, keys: NewLoadingIterator(ctx, page), mget: mget}
}

// NewLookupSource binds only Get/MGet. Keys returns an empty, non-nil iterator
// that still reports context cancellation. ctx must be non-nil; a nil mget
// returns nil. This source can resolve keys enumerated by another source.
func NewLookupSource[K comparable, V any](ctx context.Context, mget MGetFunc[K, V]) *LoadingSource[K, V] {
	return NewLoadingSourceFromKeys(ctx,
		func(context.Context) ([]K, bool, error) { return nil, false, nil }, mget)
}

// NewMapSource takes a shallow snapshot of values at construction. Changes to
// the input map or returned result maps do not alter its bindings; pointers,
// slices and other references inside keys/values remain shared. The input must
// not be mutated concurrently with construction.
//
// Map keys are authoritative identities. A non-nil less orders enumeration and
// must define a strict weak ordering; use a total order when deterministic
// traversal is required. With nil less, order is unspecified. A nil values map
// returns nil; an empty non-nil map returns an empty source. ctx must be non-nil.
// Construction performs O(n) copying and, with less, O(n log n) sorting. This
// adapts an already loaded map; it does not provide lazy external data loading.
// +spec=`Map sources preserve construction-time bindings and isolate returned maps`
func NewMapSource[K comparable, V any](ctx context.Context, values map[K]V, less func(K, K) bool) *LoadingSource[K, V] {
	if values == nil {
		return nil
	}
	snapshot := maps.Clone(values)
	keys := make([]K, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	if less != nil {
		sort.Slice(keys, func(i, j int) bool { return less(keys[i], keys[j]) })
	}
	return NewLoadingSourceFromKeys(ctx,
		func(context.Context) ([]K, bool, error) { return keys, false, nil },
		func(_ context.Context, keys []K) (map[K]V, error) {
			result := make(map[K]V, len(keys))
			for _, key := range keys {
				if value, found := snapshot[key]; found {
					result[key] = value
				}
			}
			return result, nil
		})
}

// Keys lazily enumerates candidate identities, preserving duplicates and order.
// All calls share the source's single-use traversal: after it completes or is
// stopped early, further traversals yield nothing. A nil source returns nil,
// which must not be ranged directly.
func (s *LoadingSource[K, V]) Keys() iter.Seq2[K, error] {
	if s == nil {
		return nil
	}
	return s.keys
}

// MGet deduplicates keys in first-occurrence order before invoking the loader.
// Empty input returns nil without I/O. Missing result keys mean absence; a
// loader or context error returns no partial result. LoadingSource itself does not cache results.
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
