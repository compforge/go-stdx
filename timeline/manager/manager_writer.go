package manager

import (
	"context"
	"github.com/compforge/go-stdx/timeline/store"
)

type writer struct {
	manager *Manager
	id      string
}

func (w *writer) Write(u store.Update) error {
	return w.manager.cache.Write(context.Background(), w.id, u)
}
func (w *writer) RecordError(error)               {}
func (w *writer) Flush(ctx context.Context) error { return w.manager.Flush(ctx, w.id, true) }

// ReadSnapshot collects through the same cache as ID-based recording.
func (w *writer) ReadSnapshot(ctx context.Context) (Snapshot, error) {
	return w.manager.Read(ctx, w.id)
}
