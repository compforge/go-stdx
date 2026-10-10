package timeline

import (
	"github.com/compforge/go-stdx/timeline/manager"
	"github.com/compforge/go-stdx/timeline/store"
)

// Handle groups the optional object-style API for one recording identity.
// The primary business API is Begin/End/Record/Read with optional Start/Finish.
type Handle = manager.Handle

// Option configures a recording handle without performing remote IO.
type Option = manager.Option

func WithStore(backend store.Store) Option { return manager.WithStore(backend) }
func WithActor(actor Actor) Option         { return manager.WithActor(actor) }

// New creates a standalone handle. Without WithStore, its cache is the only
// document owner and NoopStore accepts checkpoints without retaining data.
func New(id string, options ...Option) (*Handle, error) {
	return manager.NewHandle(id, options...)
}

// Noop returns a disabled timeline retaining id. It records no intervals or
// attributes, and leaves contexts unchanged. An empty ID denotes untracked work.
func Noop(id string) Timeline { return manager.Noop(id) }
