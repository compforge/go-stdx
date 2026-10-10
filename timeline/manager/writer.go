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
	return w.manager.saveUpdate(context.Background(), w.id, u)
}
func (w *writer) RecordError(error)               {}
func (w *writer) Flush(ctx context.Context) error { return w.manager.FlushID(ctx, w.id) }
