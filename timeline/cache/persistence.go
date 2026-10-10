package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

func (c *Cache) entries(id string) []*entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*entry, 0, len(c.pending))
	for e := range c.pending {
		if id == "" || e.id == id {
			result = append(result, e)
		}
	}
	return result
}
func permanent(err error) bool {
	return errors.Is(err, store.ErrConflict) || errors.Is(err, model.ErrInvalidStage) || errors.Is(err, model.ErrInvalidAttribute)
}

// save freezes each submitted batch until its outcome is known. Later writes
// stay separate: an ambiguous failure may already have committed in Store.
func (c *Cache) save(ctx context.Context, e *entry) (result error) {
	report := false
	defer func() {
		if report {
			c.config.OnError(e.id, result)
		}
	}()
	select {
	case e.gate <- struct{}{}:
		defer func() { <-e.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	remaining := e.batchCount + len(e.updates)
	e.mu.Unlock()
	for remaining > 0 {
		e.mu.Lock()
		if e.batch == nil {
			count := min(remaining, len(e.updates))
			batch, err := store.CoalesceUpdates(e.updates[:count])
			if err != nil {
				e.mu.Unlock()
				return err
			}
			e.batch = &batch
			e.batchCount = count
			e.updates = append([]store.Update(nil), e.updates[count:]...)
		}
		batch, count := *e.batch, e.batchCount
		e.mu.Unlock()
		saveCtx, cancel := context.WithTimeout(ctx, c.config.ExportTimeout)
		err := c.store.Merge(saveCtx, e.id, batch)
		cancel()
		e.mu.Lock()
		// A shutdown cancellation is followed by a caller-scoped drain. It is not
		// the final eviction attempt and must not discard the pending batch.
		final := e.evicted && ctx.Err() == nil
		report = err != nil && (e.err == nil || final) && ctx.Err() == nil
		e.err = err
		if err == nil || permanent(err) || final {
			e.batch = nil
			e.batchCount = 0
			if err != nil {
				e.lost = true
				e.lossErr = err
				if permanent(err) {
					c.rejected.Add(uint64(count))
				} else {
					c.dropped.Add(uint64(count))
				}
				if final {
					c.dropped.Add(uint64(len(e.updates)))
					e.updates = nil
				}
			}
			if len(e.updates) == 0 {
				c.mu.Lock()
				delete(c.pending, e)
				c.mu.Unlock()
			}
		}
		e.mu.Unlock()
		if err != nil {
			return err
		}
		remaining -= count
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lossErr
}

func (c *Cache) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		case <-c.wake:
		}
		for _, e := range c.entries("") {
			if c.ctx.Err() != nil {
				return
			}
			_ = c.save(c.ctx, e)
		}
	}
}

// Flush checkpoints currently pending local facts for id when wait is true.
// With wait=false it wakes the shared save worker and returns without waiting
// for Store IO; failures are reported through OnError. The worker may also save
// other pending IDs. Neither mode discovers other processes' writes or promises
// delivery of facts previously dropped.
// +spec=`Nonblocking Flush coalesces worker wakeups; accepted saves use the Manager lifetime and IO timeout independently of the caller context.`
func (c *Cache) Flush(ctx context.Context, id string, wait bool) error {
	if id == "" {
		return model.ErrEmptyID
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !wait {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.closed {
			return ErrClosed
		}
		select {
		case c.wake <- struct{}{}:
		default:
		}
		return nil
	}
	err := c.flush(ctx, id)
	if item := c.items.Get(id); item != nil {
		e := item.Value()
		e.mu.Lock()
		err = errors.Join(err, e.lossErr)
		e.mu.Unlock()
	}
	return err
}
func (c *Cache) flush(ctx context.Context, id string) error {
	var result error
	for _, e := range c.entries(id) {
		if err := c.save(ctx, e); err != nil {
			result = errors.Join(result, fmt.Errorf("timeline %s: %w", e.id, err))
		}
	}
	return errors.Join(result, ctx.Err())
}

// Shutdown stops loading/writing, cancels background IO, then attempts to drain
// with the caller's context. Failed final saves are counted and released. Reads
// remain available directly from Store while the application keeps it open.
func (c *Cache) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	select {
	case <-c.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.shutdownMu.Lock()
	defer c.shutdownMu.Unlock()
	c.stopEviction()
	err := c.flush(ctx, "")
	for _, e := range c.entries("") {
		e.mu.Lock()
		c.dropped.Add(uint64(e.batchCount + len(e.updates)))
		e.batch = nil
		e.batchCount = 0
		e.updates = nil
		e.mu.Unlock()
	}
	c.mu.Lock()
	clear(c.pending)
	c.mu.Unlock()
	c.items.DeleteAll()
	return err
}

// Stats reports process-local cache and save-buffer health. Counts do not
// describe durable completeness across processes.
type Stats struct {
	CachedTimelines  int
	PendingTimelines int
	PendingUpdates   int
	DroppedUpdates   uint64
	RejectedUpdates  uint64
}

func (c *Cache) Stats() Stats {
	entries := c.entries("")
	s := Stats{CachedTimelines: c.items.Len(), PendingTimelines: len(entries), DroppedUpdates: c.dropped.Load(), RejectedUpdates: c.rejected.Load()}
	for _, e := range entries {
		e.mu.Lock()
		s.PendingUpdates += len(e.updates) + e.batchCount
		e.mu.Unlock()
	}
	return s
}
