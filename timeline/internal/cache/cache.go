package cache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/compforge/go-stdx/timeline/store"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/singleflight"
)

var ErrClosed = errors.New("timeline manager: shut down")
var ErrBufferFull = errors.New("timeline manager: pending save limit exceeded")

// Config bounds cache capacity and backend calls. Zero fields select defaults.
type Config struct {
	MaxTimelines        int           // default 1024 cached documents
	FlushInterval       time.Duration // default 1s between background saves
	ExportTimeout       time.Duration // default 5s for each Store read/merge
	MaxPendingTimelines int           // default 1024, including evicted documents awaiting a final save
	MaxPendingUpdates   int           // default 1024 per document, before coalescing a save batch
	// OnError reports save failures outside locks. It must return promptly and
	// must not call Shutdown. Default: slog with the timeline ID, without data.
	OnError func(id string, err error)
}

// Cache owns the policy joining ttlcache and Store. Writes acknowledge
// memory acceptance; Flush is the optional persistence checkpoint. This is a
// best-effort cache, without durable delivery or cross-process consistency.
// spec: Eviction schedules one final bounded save. Failure is reported and the
// buffer is released, so an unavailable Store cannot pin memory indefinitely.
type Cache struct {
	store        store.Store
	config       Config
	items        *ttlcache.Cache[string, *entry]
	loads        singleflight.Group
	mu           sync.Mutex // lifecycle and pending membership; never held during Store IO
	pending      map[*entry]struct{}
	closed       bool
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	stopEviction func()
	shutdownMu   sync.Mutex
	dropped      atomic.Uint64
	rejected     atomic.Uint64
}

type entry struct {
	mu         sync.Mutex
	id         string
	doc        store.Document
	exists     bool
	updates    []store.Update
	batch      *store.Update // immutable until a save succeeds or is deliberately dropped
	batchCount int
	evicted    bool
	err        error
	lossErr    error
	lost       bool
	gate       chan struct{}
}

// Closed reports whether new writes and loads are rejected.
func (c *Cache) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// New starts a background saving cache.
func New(backend store.Store, config Config) (*Cache, error) {
	return newCache(backend, config, true)
}

// NewStandalone shares the same cache/Store protocol without a background worker.
// Its owner explicitly flushes one ID and retains it for the recorder lifetime.
func NewStandalone(backend store.Store) (*Cache, error) {
	return newCache(backend, Config{MaxTimelines: 1, MaxPendingTimelines: 1, MaxPendingUpdates: int(^uint(0) >> 1)}, false)
}

func newCache(backend store.Store, config Config, background bool) (*Cache, error) {
	if backend == nil {
		return nil, errors.New("timeline manager: nil Store")
	}
	if config.MaxTimelines < 0 || config.FlushInterval < 0 || config.ExportTimeout < 0 || config.MaxPendingTimelines < 0 || config.MaxPendingUpdates < 0 {
		return nil, errors.New("timeline manager: negative configuration")
	}
	if config.MaxTimelines == 0 {
		config.MaxTimelines = 1024
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = time.Second
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 5 * time.Second
	}
	if config.MaxPendingTimelines == 0 {
		config.MaxPendingTimelines = 1024
	}
	if config.MaxPendingUpdates == 0 {
		config.MaxPendingUpdates = 1024
	}
	if config.OnError == nil {
		config.OnError = func(id string, err error) { slog.Error("timeline save failed", "timeline_id", id, "error", err) }
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cache{store: backend, config: config, pending: make(map[*entry]struct{}), ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	c.items = ttlcache.New(ttlcache.WithCapacity[string, *entry](uint64(config.MaxTimelines)))
	c.stopEviction = c.items.OnEviction(func(_ context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, *entry]) {
		e := item.Value()
		e.mu.Lock()
		e.evicted = true
		e.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
	})
	if background {
		go c.run()
	} else {
		close(c.done)
	}
	return c, nil
}
