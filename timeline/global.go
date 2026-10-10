package timeline

import (
	"context"

	"github.com/compforge/go-stdx/timeline/manager"
)

var ErrNoDefault = manager.ErrNoDefault

// SetDefault installs the process Manager and returns the previous one. Install
// before producers start; replacement neither moves state nor shuts down either
// instance. Tests changing the default must run serially and restore it.
func SetDefault(m *Manager) *Manager { return manager.SetDefault(m) }
func Begin(id, name string, options ...StageOption) (StageHandle, error) {
	return manager.Begin(id, name, options...)
}
func End(id, name string, err error, options ...EndOption) error {
	return manager.End(id, name, err, options...)
}
func Record(id string, stage Stage) error                   { return manager.Record(id, stage) }
func Read(ctx context.Context, id string) (Snapshot, error) { return manager.Read(ctx, id) }
func Start(id, operation string, attributes ...Attribute) error {
	return manager.Start(id, operation, attributes...)
}
func Finish(id string, err error) error { return manager.Finish(id, err) }
