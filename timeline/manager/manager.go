// Package manager provides process-local timeline recording with TTL/LRU caching
// and background persistence. Business code uses Begin, End, Record and Read;
// Start and Finish optionally add operation-level facts.
package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
	"github.com/jellydator/ttlcache/v3"
)

var ErrClosed = errors.New("timeline manager: shut down")
var ErrExpired = errors.New("timeline manager: local timeline expired or was evicted")
var ErrBufferFull = errors.New("timeline manager: pending record limit exceeded")
var ErrStageNotFound = errors.New("timeline manager: no active local stage with this name")
var ErrAmbiguousStage = errors.New("timeline manager: multiple active local stages have this name")
var ErrStageLimit = errors.New("timeline manager: active stage limit exceeded")

// Config sets cache and submission bounds. Zero values use the documented
// defaults. Limits count objects/updates; callers bound encoded attribute sizes.
type Config struct {
	TTL               time.Duration // default 1h, fixed from cache entry creation
	MaxTimelines      int           // default 1024; overflow evicts the least recently used entry
	MaxActiveStages   int           // default 4096 name-indexed stages across cached timelines
	FlushInterval     time.Duration // default 1s; also the initial retry delay
	ExportTimeout     time.Duration // default 5s per writer
	MaxPendingHandles int           // default 1024, independent of cache capacity
	MaxPendingUpdates int           // default 1024 per writer, including in-flight updates
	// OnError reports background failure streaks and permanent rejections without
	// record data. It runs outside locks and must return promptly; do not call Shutdown.
	OnError func(id string, err error)
}

// Manager owns cached lookup and accepted submissions as separate lifetimes.
// TTL/LRU eviction invalidates local handles, but cannot discard accepted writes
// or delete stored history. Finish records a fact; it does not release the cache.
// link: ../../docs/timeline.md
// spec: Recorder only depends on timeline.Writer; the core never imports manager.
type Manager struct {
	store    store.Store
	config   Config
	mu       sync.Mutex
	cache    *ttlcache.Cache[string, *localTimeline]
	dirty    map[*writer]*pendingWork
	closed   bool
	dropped  uint64
	rejected uint64
	wake     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
}

type recordingScope struct {
	id  string
	err error // first collection loss, retained for this cache lifetime
}

// Pending writers retain only the small scope, not the evicted stage index.
type localTimeline struct {
	*recordingScope
	root   *timeline.Recorder
	stages map[string]map[*stageHandle]struct{}
}

type pendingWork struct {
	generation uint64
	failures   int
	next       time.Time
}
type work struct {
	writer     *writer
	pending    *pendingWork
	generation uint64
}

func New(backend store.Store, config Config) (*Manager, error) {
	if backend == nil {
		return nil, errors.New("timeline manager: nil Store")
	}
	if config.TTL < 0 || config.MaxTimelines < 0 || config.MaxActiveStages < 0 || config.FlushInterval < 0 || config.ExportTimeout < 0 || config.MaxPendingHandles < 0 || config.MaxPendingUpdates < 0 {
		return nil, errors.New("timeline manager: negative configuration")
	}
	if config.TTL == 0 {
		config.TTL = time.Hour
	}
	if config.MaxTimelines == 0 {
		config.MaxTimelines = 1024
	}
	if config.MaxActiveStages == 0 {
		config.MaxActiveStages = 4096
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = time.Second
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 5 * time.Second
	}
	if config.MaxPendingHandles == 0 {
		config.MaxPendingHandles = 1024
	}
	if config.MaxPendingUpdates == 0 {
		config.MaxPendingUpdates = 1024
	}
	if config.OnError == nil {
		config.OnError = func(id string, err error) {
			slog.Error("timeline background submission failed", "timeline_id", id, "error", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{store: backend, config: config, dirty: make(map[*writer]*pendingWork), wake: make(chan struct{}, 1), done: make(chan struct{}), cancel: cancel}
	m.cache = ttlcache.New(
		ttlcache.WithTTL[string, *localTimeline](config.TTL),
		ttlcache.WithCapacity[string, *localTimeline](uint64(config.MaxTimelines)),
		ttlcache.WithDisableTouchOnHit[string, *localTimeline](),
	)
	go m.run(ctx)
	return m, nil
}

// localLocked requires m.mu. Cache.Get updates LRU but never refreshes TTL.
func (m *Manager) localLocked(id string) (*localTimeline, error) {
	if id == "" {
		return nil, timeline.ErrEmptyID
	}
	if m.closed {
		return nil, ErrClosed
	}
	if item := m.cache.Get(id); item != nil {
		return item.Value(), nil
	}
	// Drop expired entries before applying capacity eviction to live entries.
	m.cache.DeleteExpired()
	local := &localTimeline{recordingScope: &recordingScope{id: id}, stages: make(map[string]map[*stageHandle]struct{})}
	m.cache.Set(id, local, ttlcache.DefaultTTL)
	return local, nil
}

func (m *Manager) checkLocked(local *recordingScope) error {
	if m.closed {
		return ErrClosed
	}
	item := m.cache.Get(local.id)
	if item == nil || item.Value().recordingScope != local {
		return ErrExpired
	}
	return nil
}

func (m *Manager) recorderLocked(local *localTimeline, actor timeline.Actor) *timeline.Recorder {
	w := &writer{manager: m, local: local.recordingScope, gate: make(chan struct{}, 1)}
	r, _ := timeline.New(local.id, timeline.WithStore(m.store), timeline.WithActor(actor), timeline.WithWriter(w))
	return r
}

// NewWriter creates an independent explicit handle. Most callers use the six
// ID-based methods. Handles reject new writes after TTL/LRU eviction; accepted
// writes can still be flushed. Actor belongs to this writer's stages.
func (m *Manager) NewWriter(id string, actor timeline.Actor) (*timeline.Recorder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	local, err := m.localLocked(id)
	if err != nil {
		return nil, err
	}
	return m.recorderLocked(local, actor), nil
}

// Stats reports local cache and queue counts, never distributed completeness.
type Stats struct {
	CachedTimelines int
	ActiveStages    int
	PendingHandles  int
	DroppedUpdates  uint64
	RejectedUpdates uint64
}

func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache.DeleteExpired()
	return Stats{CachedTimelines: m.cache.Len(), ActiveStages: m.stageCountLocked(), PendingHandles: len(m.dirty), DroppedUpdates: m.dropped, RejectedUpdates: m.rejected}
}
func (m *Manager) stageCountLocked() int {
	count := 0
	m.cache.Range(func(item *ttlcache.Item[string, *localTimeline]) bool {
		for _, stages := range item.Value().stages {
			count += len(stages)
		}
		return true
	})
	return count
}

func (m *Manager) work(readyOnly bool, id string) []work {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	result := make([]work, 0, len(m.dirty))
	for w, p := range m.dirty {
		if (id == "" || w.local.id == id) && (!readyOnly || !now.Before(p.next)) {
			result = append(result, work{writer: w, pending: p, generation: p.generation})
		}
	}
	return result
}

func (m *Manager) run(ctx context.Context) {
	// Cache cleanup has its own cancellation-bound loop so blocked store IO
	// cannot postpone TTL reclamation. ttlcache owns all expiration/LRU rules.
	cleaned := make(chan struct{})
	go func() {
		defer close(cleaned)
		ticker := time.NewTicker(min(m.config.TTL, time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.cache.DeleteExpired()
			}
		}
	}()
	defer func() { <-cleaned; close(m.done) }()
	ticker := time.NewTicker(m.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.wake:
		}
		for _, w := range m.work(true, "") {
			if ctx.Err() != nil {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, m.config.ExportTimeout)
			err := w.writer.flush(writeCtx)
			cancel()
			if m.complete(w, err) && ctx.Err() == nil {
				m.config.OnError(w.writer.local.id, err)
			}
		}
	}
}

func (m *Manager) complete(w work, err error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.dirty[w.writer]
	if p != w.pending {
		return false
	}
	report := err != nil && p.failures == 0
	if len(w.writer.pending) == 0 && p.generation == w.generation {
		delete(m.dirty, w.writer)
	} else if err != nil {
		p.failures++
		delay := min(m.config.FlushInterval, 30*time.Second)
		for i := 1; i < p.failures && delay < 30*time.Second; i++ {
			delay = min(delay*2, 30*time.Second)
		}
		p.next = time.Now().Add(delay)
	} else {
		p.failures, p.next = 0, time.Time{}
	}
	return report
}

// Flush checkpoints currently pending local writers. It does not wait for
// future recording or report already retired historical rejections; Read reports
// per-ID collection health while that cache entry exists, and Stats counts loss.
func (m *Manager) Flush(ctx context.Context) error { return m.flush(ctx, "") }
func (m *Manager) flush(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var errs []error
	for _, w := range m.work(false, id) {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		writeCtx, cancel := context.WithTimeout(ctx, m.config.ExportTimeout)
		err := w.writer.flush(writeCtx)
		cancel()
		m.complete(w, err)
		if err != nil {
			errs = append(errs, fmt.Errorf("timeline %s: %w", w.writer.local.id, err))
		}
	}
	return errors.Join(errs...)
}

// Shutdown rejects new writes, stops the worker, and drains accepted records.
// Stop producers first and supply an independent bounded context. Failed drains
// may be retried. Shutdown neither calls Finish nor closes the application Store.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.cancel()
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.done:
	}
	err := m.Flush(ctx)
	if err == nil {
		m.mu.Lock()
		m.cache.DeleteAll()
		m.mu.Unlock()
	}
	return err
}
