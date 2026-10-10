package timeline

import (
	"github.com/compforge/go-stdx/timeline/manager"
	"github.com/compforge/go-stdx/timeline/store"
)

// Manager coordinates recording and background persistence for multiple IDs.
type Manager = manager.Manager
type Config = manager.Config
type Stats = manager.Stats

var ErrClosed = manager.ErrClosed
var ErrBufferFull = manager.ErrBufferFull
var ErrStageNotFound = manager.ErrStageNotFound
var ErrAmbiguousStage = manager.ErrAmbiguousStage

// NewManager starts a Manager. A nil backend selects NoopStore for cache-only recording.
func NewManager(backend store.Store, config Config) (*Manager, error) {
	return manager.New(backend, config)
}
