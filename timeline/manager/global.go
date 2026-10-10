package manager

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/compforge/go-stdx/timeline"
)

var ErrNoDefault = errors.New("timeline manager: no default installed")
var defaultManager atomic.Pointer[Manager]

// SetDefault installs the process Manager and returns the previous one. Install
// before producers start; replacement neither moves state nor shuts down either
// instance. Tests changing the default must run serially and restore it.
func SetDefault(m *Manager) *Manager { return defaultManager.Swap(m) }
func current() (*Manager, error) {
	m := defaultManager.Load()
	if m == nil {
		return nil, ErrNoDefault
	}
	return m, nil
}

func Begin(id, name string, options ...timeline.StageOption) (timeline.StageHandle, error) {
	m, err := current()
	if err != nil {
		return nil, err
	}
	return m.Begin(id, name, options...)
}
func End(id, name string, stageErr error, options ...timeline.EndOption) error {
	m, err := current()
	if err != nil {
		return err
	}
	return m.End(id, name, stageErr, options...)
}
func Record(id string, stage timeline.Stage) error {
	m, err := current()
	if err != nil {
		return err
	}
	return m.Record(id, stage)
}
func Read(ctx context.Context, id string) (timeline.Snapshot, error) {
	m, err := current()
	if err != nil {
		return timeline.Snapshot{}, err
	}
	return m.Read(ctx, id)
}
func Start(id, operation string, attributes ...timeline.Attribute) error {
	m, err := current()
	if err != nil {
		return err
	}
	return m.Start(id, operation, attributes...)
}
func Finish(id string, operationErr error) error {
	m, err := current()
	if err != nil {
		return err
	}
	return m.Finish(id, operationErr)
}
