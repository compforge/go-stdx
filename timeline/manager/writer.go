package manager

import (
	"context"
	"github.com/compforge/go-stdx/timeline/cache"
	"github.com/compforge/go-stdx/timeline/store"
)

// cachedWriter shares Manager's admission, merge and save protocol, with explicit
// checkpoints and cache residency bounded by this standalone recorder's lifetime.
type cachedWriter struct {
	id    string
	cache *cache.Cache
}

func (*cachedWriter) RecordError(error) {}
func (w *cachedWriter) Write(update store.Update) error {
	return w.cache.Write(context.Background(), w.id, update)
}
func (w *cachedWriter) Flush(ctx context.Context) error {
	return w.cache.Flush(ctx, w.id, true)
}
func (w *cachedWriter) ReadSnapshot(ctx context.Context) (Snapshot, error) {
	return w.cache.Read(ctx, w.id, true)
}
