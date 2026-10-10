// Package manager forwards to timeline's Manager and global entry points.
// New integrations use the timeline root package.
package manager

import (
	"context"
	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
)

type Manager = timeline.Manager
type Config = timeline.Config
type Stats = timeline.Stats

var ErrClosed = timeline.ErrClosed
var ErrBufferFull = timeline.ErrBufferFull
var ErrStageNotFound = timeline.ErrStageNotFound
var ErrAmbiguousStage = timeline.ErrAmbiguousStage
var ErrNoDefault = timeline.ErrNoDefault

func New(backend store.Store, config Config) (*Manager, error) {
	return timeline.NewManager(backend, config)
}
func SetDefault(m *Manager) *Manager { return timeline.SetDefault(m) }
func Begin(id, name string, options ...timeline.StageOption) (timeline.StageHandle, error) {
	return timeline.Begin(id, name, options...)
}
func End(id, name string, err error, options ...timeline.EndOption) error {
	return timeline.End(id, name, err, options...)
}
func Record(id string, stage timeline.Stage) error                   { return timeline.Record(id, stage) }
func Read(ctx context.Context, id string) (timeline.Snapshot, error) { return timeline.Read(ctx, id) }
func Start(id, operation string, attributes ...timeline.Attribute) error {
	return timeline.Start(id, operation, attributes...)
}
func Finish(id string, err error) error { return timeline.Finish(id, err) }
