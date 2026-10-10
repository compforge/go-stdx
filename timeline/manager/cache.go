package manager

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

// loaderFunc supplies a document on a cache miss. It may read Store and create a
// new document on ErrNotFound. Errors are returned to callers and are not cached.
// Concurrent callers for one ID share one loader, so use compatible loaders.
type loaderFunc func(context.Context, string) (store.Document, error)

type loadedEntry struct {
	entry     *entry
	fromStore bool
}

// ttlcache's Loader has no error/context return path. singleflight adapts the
// supplied loader while ttlcache still owns residency, expiry and LRU.
func (c *Manager) load(ctx context.Context, id string, loader loaderFunc) (*entry, bool, error) {
	if id == "" {
		return nil, false, model.ErrEmptyID
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, false, ErrClosed
	}
	if item := c.items.Get(id); item != nil {
		return item.Value(), false, nil
	}
	loaded := c.loads.DoChan(id, func() (any, error) {
		if item := c.items.Get(id); item != nil {
			return loadedEntry{entry: item.Value()}, nil
		}
		readCtx, cancel := context.WithTimeout(c.ctx, c.config.ExportTimeout)
		defer cancel()
		doc, err := loader(readCtx, id)
		if err != nil {
			return nil, err
		}
		if doc.ID != id {
			return nil, store.ErrConflict
		}
		e := &entry{id: id, doc: doc.Clone(), exists: len(doc.Stages) > 0 || !doc.StartedAt.IsZero() || !doc.FinishedAt.IsZero(), gate: make(chan struct{}, 1)}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return nil, ErrClosed
		}
		c.items.DeleteExpired()
		item, _ := c.items.GetOrSet(id, e)
		return loadedEntry{entry: item.Value(), fromStore: true}, nil
	})
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case result := <-loaded:
		if result.Err != nil {
			return nil, false, result.Err
		}
		value := result.Val.(loadedEntry)
		return value.entry, value.fromStore, nil
	}
}

// loadOrCreate is the write policy. Manager.load itself does not decide how a
// missing document is created. A simultaneous read-only loader may return
// ErrNotFound to this waiter; retry with the write policy in that case.
func (c *Manager) loadOrCreate(ctx context.Context, id string) (*entry, error) {
	loader := func(ctx context.Context, id string) (store.Document, error) {
		doc, err := c.store.Read(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			return store.Document{ID: id, RootStageID: model.RootID(id)}, nil
		}
		return doc, err
	}
	for {
		e, _, err := c.load(ctx, id, loader)
		if !errors.Is(err, store.ErrNotFound) {
			return e, err
		}
	}
}

// Read returns the latest cached view, loading from Store on a miss. It does not
// wait for a save, and a warm cache may lag behind another process's writes.
// The returned snapshot owns its data. A closed cache reads Store directly.
func (c *Manager) Read(ctx context.Context, id string) (model.Snapshot, error) {
	if id == "" {
		return model.Snapshot{}, model.ErrEmptyID
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		readCtx, cancel := context.WithTimeout(ctx, c.config.ExportTimeout)
		defer cancel()
		doc, err := c.store.Read(readCtx, id)
		if doc.ID == "" {
			doc.ID = id
		}
		s := doc.Snapshot(time.Now().UTC())
		s.Collection.StoreRead = err == nil
		return s, err
	}
	e, fromStore, err := c.load(ctx, id, c.store.Read)
	if err != nil {
		return model.Snapshot{ID: id}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.exists {
		return model.Snapshot{ID: id}, store.ErrNotFound
	}
	s := e.doc.Snapshot(time.Now().UTC())
	s.Collection = model.Collection{LocalFlushed: len(e.updates) == 0 && e.batch == nil && !e.lost, StoreRead: fromStore}
	return s, nil
}

// saveUpdate accepts facts into memory and schedules persistence. It may read Store on
// a cache miss; success does not mean the facts have reached persistent storage.
func (c *Manager) saveUpdate(ctx context.Context, id string, update store.Update) error {
	return c.update(ctx, id, func(store.Document) (store.Update, error) { return update, nil })
}

// update builds one update against the current local document, atomically with
// other updates to this cache entry. The callback gets a detached document and
// must not reenter this Manager or perform IO. This is not a distributed transaction.
func (c *Manager) update(ctx context.Context, id string, fn func(store.Document) (store.Update, error)) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	e, err := c.loadOrCreate(ctx, id)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	update, err := fn(e.doc.Clone())
	if err != nil {
		return err
	}
	doc, changed, err := store.MergeDocument(id, e.doc, update)
	if err != nil || !changed {
		return err
	}
	update = detachUpdate(update)
	if update.Operation != nil {
		operation := doc.OperationRecord
		update.Operation = &operation
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	index := -1
	for i, old := range e.updates {
		if old.Operation != nil && update.Operation != nil && len(old.Stages) == 0 && len(old.Completed) == 0 && len(update.Stages) == 0 && len(update.Completed) == 0 {
			index = i
			break
		}
		if old.Operation == nil && update.Operation == nil && len(old.Completed) == 0 && len(update.Completed) == 0 && len(old.Stages) == 1 && len(update.Stages) == 1 && old.Stages[0].ID == update.Stages[0].ID {
			index = i
			break
		}
	}
	if index < 0 && len(e.updates)+e.batchCount >= c.config.MaxPendingUpdates {
		c.dropped.Add(1)
		return ErrBufferFull
	}
	if _, ok := c.pending[e]; !ok && len(c.pending) >= c.config.MaxPendingTimelines {
		c.dropped.Add(1)
		return ErrBufferFull
	}
	e.doc, e.exists = doc, true
	if index >= 0 {
		e.updates[index] = update
	} else {
		e.updates = append(e.updates, update)
	}
	c.pending[e] = struct{}{}
	return nil
}

func detachUpdate(u store.Update) store.Update {
	if u.Operation != nil {
		op := *u.Operation
		op.Attributes = model.CloneJSONAttributes(op.Attributes)
		u.Operation = &op
	}
	u.Stages = append([]store.StageUpdate(nil), u.Stages...)
	for i := range u.Stages {
		u.Stages[i].Attributes = model.CloneJSONAttributes(u.Stages[i].Attributes)
	}
	u.Completed = append([]model.Stage(nil), u.Completed...)
	for i := range u.Completed {
		u.Completed[i].Attributes = model.CloneJSONAttributes(u.Completed[i].Attributes)
	}
	return u
}

// Evict releases a cached copy and schedules its final best-effort save. Later
// access loads persistent state; an unsaved last update may be absent from it.
func (c *Manager) Evict(id string) { c.items.Delete(id) }
