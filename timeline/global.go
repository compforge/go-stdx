package timeline

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrNoDefaultManager = errors.New("timeline: no default Manager installed")

var defaultManager atomic.Pointer[Manager]

// SetDefaultManager installs the process default and returns the previous one.
// Install it before producers start. Passing nil clears it. Replacement does
// not shut down Managers or move their handles/coordinators; the application
// controls their lifetime. ID-based calls always use the current default.
// link: ../docs/timeline.md#全局入口与默认-manager
func SetDefaultManager(manager *Manager) (previous *Manager) {
	return defaultManager.Swap(manager)
}

func currentManager() (*Manager, error) {
	manager := defaultManager.Load()
	if manager == nil {
		return nil, ErrNoDefaultManager
	}
	return manager, nil
}

// For returns an independent writer through the default Manager, without
// starting an operation, querying storage, or borrowing coordinator authority.
// Options follow Manager.New; WithStore is invalid. Most business code can use
// the ID-based Start/Begin/Record/Finish functions instead of passing a Recorder.
func For(id string, options ...Option) (*Recorder, error) {
	manager, err := currentManager()
	if err != nil {
		return nil, err
	}
	return manager.New(id, options...)
}

// Start starts and registers a coordinator in the default Manager. The returned
// handle is optional: subsequent business calls can use only id. Admission
// errors return nil; persistence errors preserve the coordinator for Flush retry.
func Start(ctx context.Context, id, operation string, options ...Option) (*Recorder, error) {
	manager, err := currentManager()
	if err != nil {
		return nil, err
	}
	return manager.Start(ctx, id, operation, options...)
}

// Begin records a stage for id, without starting or taking over its root.
// Use WithStageActor for executor identity; end the returned stage with End.
func Begin(id, name string, options ...StageOption) (StageHandle, error) {
	manager, err := currentManager()
	if err != nil {
		return nil, err
	}
	return manager.Begin(id, name, options...)
}

// BeginContext is Begin with parentage carried by StageRef in ctx. It preserves
// cancellation/deadlines and returns the original context if recording fails.
func BeginContext(ctx context.Context, id, name string, options ...StageOption) (context.Context, StageHandle, error) {
	manager, err := currentManager()
	if err != nil {
		return ctx, nil, err
	}
	return manager.BeginContext(ctx, id, name, options...)
}

// Record buffers a completed stage for id, preserving source times and Actor.
func Record(id string, stage Stage) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.Record(id, stage)
}

// SetAttributes updates the coordinator started in this process for id.
func SetAttributes(id string, attributes ...Attribute) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.SetAttributes(id, attributes...)
}

// Flush confirms currently pending local records for id reached the Store.
// It includes independent writers for id, but not other IDs or remote processes.
func Flush(ctx context.Context, id string) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.FlushID(ctx, id)
}

// Capture flushes this ID's local writers then reads its snapshot. Collection
// describes this process's checkpoint, never distributed completeness.
func Capture(ctx context.Context, id string) (Snapshot, error) {
	manager, err := currentManager()
	if err != nil {
		return Snapshot{}, err
	}
	return manager.Capture(ctx, id)
}

// Finish completes this process's coordinator for id and collects its snapshot.
// Success releases local coordination; errors preserve it for retry. Participants
// in other processes end their stages, leaving root completion to the coordinator.
func Finish(ctx context.Context, id string, operationErr error) (Snapshot, error) {
	manager, err := currentManager()
	if err != nil {
		return Snapshot{}, err
	}
	return manager.Finish(ctx, id, operationErr)
}

// Read reads the persisted document without flushing any writer.
func Read(ctx context.Context, id string) (Document, error) {
	manager, err := currentManager()
	if err != nil {
		return Document{}, err
	}
	return manager.Read(ctx, id)
}

// Release removes local coordinator and stage-name lookup without ending work
// or deleting its data. Pending records still drain; successful Finish releases
// automatically. This is for explicitly abandoned operations.
func Release(id string) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.Release(id)
}

// End ends the uniquely named active stage in this process. Same-name parallel
// stages return ErrAmbiguousStage; use their handles to identify the right one.
func End(id, name string, stageErr error, options ...EndOption) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.End(id, name, stageErr, options...)
}

// SetStageAttributes updates the uniquely named active stage in this process.
func SetStageAttributes(id, name string, attributes ...Attribute) error {
	manager, err := currentManager()
	if err != nil {
		return err
	}
	return manager.SetStageAttributes(id, name, attributes...)
}
