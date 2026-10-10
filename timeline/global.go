package timeline

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrNoDefaultManager = errors.New("timeline: no default Manager installed")

var defaultManager atomic.Pointer[Manager]

// SetDefaultManager installs the process default used by For and Read, returning
// the previous default. Install it before starting business producers. Passing
// nil clears the default. Replacement is concurrency-safe but does not shut down
// either Manager or rebind existing handles; their owner controls their lifetime.
// link: ../docs/timeline.md#全局入口与默认-manager
func SetDefaultManager(manager *Manager) (previous *Manager) {
	return defaultManager.Swap(manager)
}

// For returns a fresh local writer for id through the default Manager. It does
// not query storage, start an operation, or cache the handle by ID. Only the
// business creator calls Start/Finish; other writers contribute stages.
// It returns ErrNoDefaultManager when no default is installed. Options follow
// Manager.New, including rejecting WithStore. Use New for standalone handles.
func For(id string, options ...Option) (*Recorder, error) {
	manager := defaultManager.Load()
	if manager == nil {
		return nil, ErrNoDefaultManager
	}
	return manager.New(id, options...)
}

// Read reads the default Manager's persisted document, without flushing local
// writers or claiming that every process has reported. Use Recorder.Snapshot
// to submit that handle's pending updates before reading.
func Read(ctx context.Context, id string) (Document, error) {
	manager := defaultManager.Load()
	if manager == nil {
		return Document{}, ErrNoDefaultManager
	}
	return manager.Read(ctx, id)
}
