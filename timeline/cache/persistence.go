package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

func (c *Cache) entries(id string) []*entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []*entry
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
func (e *entry) notify() { close(e.changed); e.changed = make(chan struct{}) }

// save submits one frozen batch. Local sequence numbers only acknowledge Flush
// checkpoints; they are never sent to Store as a cross-process ordering claim.
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
	if e.batch == nil && len(e.updates) > 0 {
		batch, err := store.CoalesceUpdates(e.updates)
		if err != nil {
			e.mu.Unlock()
			return err
		}
		e.batch = &batch
		e.batchCount = len(e.updates)
		e.batchSeq = e.accepted
		e.updates = nil
	}
	if e.batch == nil {
		err := e.lossErr
		e.mu.Unlock()
		return err
	}
	batch, count, seq := *e.batch, e.batchCount, e.batchSeq
	e.mu.Unlock()
	saveCtx, cancel := context.WithTimeout(ctx, c.config.ExportTimeout)
	err := c.store.Merge(saveCtx, e.id, batch)
	cancel()
	e.mu.Lock()
	final := e.evicted && ctx.Err() == nil
	report = err != nil && (e.err == nil || final) && ctx.Err() == nil
	e.err = err
	e.attempts++
	if err == nil || permanent(err) || final {
		e.batch = nil
		e.batchCount = 0
		if err == nil {
			e.saved = seq
		} else {
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
	pending := e.batch != nil || len(e.updates) > 0
	e.notify()
	e.mu.Unlock()
	// More accepted writes may have arrived during IO. Save them in a later
	// bounded batch; failures wait for the periodic retry instead of hot spinning.
	if pending && err == nil {
		c.requestSave(e.id)
	}
	return err
}

// Flush triggers the save worker. Waiting captures only records accepted before
// this call; caller cancellation never cancels the shared Store submission.
func (c *Cache) Flush(ctx context.Context, id string, wait bool) error {
	if id == "" {
		return model.ErrEmptyID
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.Closed() {
		return ErrClosed
	}
	if !c.background {
		return c.flush(ctx, id)
	}
	type checkpoint struct {
		e            *entry
		seq, attempt uint64
	}
	var points []checkpoint
	if wait {
		for _, e := range c.entries(id) {
			e.mu.Lock()
			points = append(points, checkpoint{e, e.accepted, e.attempts})
			e.mu.Unlock()
		}
	}
	c.requestSave(id)
	for _, p := range points {
		for {
			p.e.mu.Lock()
			err := p.e.lossErr
			if err == nil && p.e.attempts > p.attempt && p.e.err != nil {
				err = p.e.err
			}
			done := p.e.saved >= p.seq
			changed := p.e.changed
			p.e.mu.Unlock()
			if done {
				break
			}
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.ctx.Done():
				return ErrClosed
			case <-changed:
			}
		}
	}
	if wait {
		if item := c.peek(id); item != nil {
			e := item.Value()
			e.mu.Lock()
			err := e.lossErr
			e.mu.Unlock()
			return err
		}
	}
	return nil
}

// Explicit standalone checkpoints and final shutdown drain may exceed one batch,
// but retain the same serial IO and per-call timeout as the worker.
func (c *Cache) flush(ctx context.Context, id string) error {
	var result error
	for _, e := range c.entries(id) {
		for {
			if err := c.save(ctx, e); err != nil {
				result = errors.Join(result, fmt.Errorf("timeline %s: %w", e.id, err))
				break
			}
			e.mu.Lock()
			pending := e.batch != nil || len(e.updates) > 0
			e.mu.Unlock()
			if !pending {
				break
			}
		}
	}
	return errors.Join(result, ctx.Err())
}

// Shutdown stops both workers, then attempts a caller-scoped final drain. Store
// connections remain owned by the application. Failed buffers are released.
func (c *Cache) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	for _, done := range []chan struct{}{c.done, c.loadDone} {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
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
		e.notify()
		e.mu.Unlock()
	}
	c.mu.Lock()
	clear(c.pending)
	c.mu.Unlock()
	c.items.DeleteAll()
	return err
}

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
