package manager

import (
	"context"
	"errors"

	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
)

// writer is a bounded outbox; all queue fields are protected by manager.mu.
// The flush gate serializes IO without blocking memory-only recording calls.
type writer struct {
	manager  *Manager
	local    *recordingScope
	pending  []store.Update
	flushing int // immutable prefix, retained verbatim after ambiguous IO
	gate     chan struct{}
	err      error
}

func (w *writer) Write(update store.Update) error {
	m := w.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkLocked(w.local); err != nil {
		return err
	}
	index := -1
	for i := w.flushing; i < len(w.pending); i++ {
		old := w.pending[i]
		if old.Operation != nil && update.Operation != nil {
			index = i
			break
		}
		if len(old.Stages) == 1 && len(update.Stages) == 1 && old.Stages[0].ID == update.Stages[0].ID {
			// Validate boundaries before replacing an unsent stage state. In-flight
			// updates remain immutable because a failed write may already be committed.
			prior := store.Document{ID: w.local.id, Stages: old.Stages}
			_, _, err := store.MergeDocument(w.local.id, prior, update)
			if err != nil {
				return err
			}
			index = i
			break
		}
	}
	if index < 0 && len(w.pending) >= m.config.MaxPendingUpdates {
		m.dropped++
		return ErrBufferFull
	}
	p := m.dirty[w]
	if p == nil {
		if len(m.dirty) >= m.config.MaxPendingHandles {
			m.dropped++
			return ErrBufferFull
		}
		p = &pendingWork{}
		m.dirty[w] = p
	}
	if index >= 0 {
		w.pending[index] = update
	} else {
		w.pending = append(w.pending, update)
	}
	p.generation++
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

func (w *writer) RecordError(err error) {
	m := w.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if errors.Is(err, ErrBufferFull) || errors.Is(err, ErrExpired) || errors.Is(err, ErrClosed) {
		return
	}
	if w.local.err == nil {
		w.local.err = err
	}
}

func permanent(err error) bool {
	return errors.Is(err, store.ErrConflict) || errors.Is(err, timeline.ErrInvalidStage) || errors.Is(err, timeline.ErrInvalidAttribute)
}

func (w *writer) Flush(ctx context.Context) error {
	err := w.flush(ctx)
	w.manager.mu.Lock()
	defer w.manager.mu.Unlock()
	return errors.Join(err, w.err)
}

func (w *writer) flush(ctx context.Context) error {
	select {
	case w.gate <- struct{}{}:
		defer func() { <-w.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := w.manager
	m.mu.Lock()
	remaining := len(w.pending)
	m.mu.Unlock()
	for remaining > 0 {
		m.mu.Lock()
		if w.flushing == 0 {
			w.flushing = remaining
		}
		batch := append([]store.Update(nil), w.pending[:w.flushing]...)
		m.mu.Unlock()
		update, err := store.CoalesceUpdates(batch)
		if err == nil {
			err = m.store.Merge(ctx, w.local.id, update)
		}
		if err != nil && !permanent(err) {
			return err
		}
		m.mu.Lock()
		// A definitive rejection cannot become successful by retrying identical
		// facts. Retire that batch, retain health evidence, and let other work drain.
		w.pending = append([]store.Update(nil), w.pending[len(batch):]...)
		w.flushing = 0
		if err != nil {
			m.rejected += uint64(len(batch))
			if w.err == nil {
				w.err = err
			}
			if w.local.err == nil {
				w.local.err = err
			}
		}
		m.mu.Unlock()
		if err != nil {
			return err
		}
		remaining -= len(batch)
	}
	return nil
}

var _ timeline.Writer = (*writer)(nil)
