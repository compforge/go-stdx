package timeline

import (
	"context"
	"sync"

	"github.com/compforge/go-stdx/timeline/store"
)

// localWriter is the explicit-checkpoint implementation. It has no goroutine,
// capacity policy or cache; process-level submission belongs to manager.
type localWriter struct {
	id       string
	store    store.Store
	mu       sync.Mutex
	gate     chan struct{}
	pending  []store.Update
	flushing int
}

func (w *localWriter) RecordError(error) {}

func (w *localWriter) Write(update store.Update) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = append(w.pending, update)
	return nil
}

func (w *localWriter) Flush(ctx context.Context) error {
	select {
	case w.gate <- struct{}{}:
		defer func() { <-w.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	remaining := len(w.pending)
	w.mu.Unlock()
	for remaining > 0 {
		w.mu.Lock()
		if w.flushing == 0 {
			w.flushing = remaining
		}
		updates := append([]store.Update(nil), w.pending[:w.flushing]...)
		w.mu.Unlock()
		update, err := store.CoalesceUpdates(updates)
		if err != nil {
			return err
		}
		if err := w.store.Merge(ctx, w.id, update); err != nil {
			return err
		}
		w.mu.Lock()
		w.pending = append([]store.Update(nil), w.pending[len(updates):]...)
		w.flushing = 0
		w.mu.Unlock()
		remaining -= len(updates)
	}
	return nil
}
