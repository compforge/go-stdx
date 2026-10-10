package cache

import (
	"context"
	"errors"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

// Read optionally refreshes the Store view. Local unsubmitted updates are always
// overlaid; a fresh read never implicitly flushes and cannot see unsubmitted peers.
func (c *Cache) Read(ctx context.Context, id string, fresh bool) (model.Snapshot, error) {
	if id == "" {
		return model.Snapshot{}, model.ErrEmptyID
	}
	if err := ctx.Err(); err != nil {
		return model.Snapshot{ID: id}, err
	}
	if c.Closed() {
		return c.readCached(ctx, id)
	}
	if !fresh {
		s, err := c.readCached(ctx, id)
		if err == nil && !s.Collection.StoreRead {
			c.requestLoad(id)
		}
		return s, err
	}
	result := c.refreshes.DoChan(id, func() (any, error) {
		before := c.observed([]string{id})
		ioCtx, cancel := context.WithTimeout(c.ctx, c.config.ExportTimeout)
		defer cancel()
		doc, err := c.store.Read(ioCtx, id)
		if errors.Is(err, store.ErrNotFound) {
			return (*store.Document)(nil), nil
		}
		if err != nil {
			return (*store.Document)(nil), err
		}
		if doc.ID != id {
			return (*store.Document)(nil), store.ErrConflict
		}
		c.mergeLoaded(doc, before, false)
		return &doc, nil
	})
	var fromStore bool
	var remote *store.Document
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case v := <-result:
		err = v.Err
		if v.Val != nil {
			remote = v.Val.(*store.Document)
			fromStore = remote != nil
		}
	}
	// Do not start a second load when refresh failed; preserve the available view.
	if item := c.peek(id); item != nil {
		e := item.Value()
		e.mu.Lock()
		s := e.snapshot(fromStore && err == nil)
		exists := e.exists
		e.mu.Unlock()
		if exists {
			return s, err
		}
	}
	if err == nil && remote != nil {
		s := remote.Snapshot(time.Now().UTC())
		s.Collection.StoreRead = true
		return s, nil
	}
	if err == nil {
		err = store.ErrNotFound
	}
	return model.Snapshot{ID: id}, err
}
func (e *entry) snapshot(fromStore bool) model.Snapshot {
	s := e.doc.Snapshot(time.Now().UTC())
	s.Collection = model.Collection{LocalFlushed: len(e.updates) == 0 && e.batch == nil && !e.lost, StoreRead: fromStore}
	return s
}

// Snapshot the local save generation before remote IO. A refresh which raced a
// successful save must not overwrite that just-saved state with an older read.
func (c *Cache) observed(ids []string) map[*entry]uint64 {
	result := make(map[*entry]uint64)
	items := c.items.Items()
	for _, id := range ids {
		if item := items[id]; item != nil {
			e := item.Value()
			e.mu.Lock()
			result[e] = e.saved
			e.mu.Unlock()
		}
	}
	return result
}
func (c *Cache) mergeLoaded(doc store.Document, before map[*entry]uint64, prewarm bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	item := c.peek(doc.ID)
	if item == nil {
		if prewarm && c.items.Len() >= c.config.MaxTimelines {
			c.mu.Unlock()
			return
		}
		e := &entry{id: doc.ID, doc: doc.Clone(), exists: true, gate: make(chan struct{}, 1), changed: make(chan struct{})}
		item, _ = c.items.GetOrSet(doc.ID, e)
	}
	c.mu.Unlock()
	e := item.Value()
	e.mu.Lock()
	defer e.mu.Unlock()
	base := doc.Clone()
	saved, observed := before[e]
	if (!observed && e.accepted > 0) || e.saved > saved {
		base = store.Overlay(base, e.doc.Update())
	}
	if e.batch != nil {
		base = store.Overlay(base, *e.batch)
	}
	base = store.Overlay(base, e.updates...)
	e.doc, e.exists = base, true
}
func (c *Cache) refreshMany(ctx context.Context, ids []string) error {
	before := c.observed(ids)
	ioCtx, cancel := context.WithTimeout(ctx, c.config.ExportTimeout)
	defer cancel()
	docs, err := c.store.MGet(ioCtx, ids)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		c.mergeLoaded(doc, before, false)
	}
	return nil
}
func (c *Cache) refreshLatest(ctx context.Context, after time.Time) error {
	items := c.items.Items()
	ids := make([]string, 0, len(items))
	for id := range items {
		ids = append(ids, id)
	}
	before := c.observed(ids)
	ioCtx, cancel := context.WithTimeout(ctx, c.config.ExportTimeout)
	defer cancel()
	docs, err := c.store.Latest(ioCtx, after, c.config.BatchLimit)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		c.mergeLoaded(doc, before, true)
	}
	return nil
}
