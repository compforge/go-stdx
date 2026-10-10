package timeline

import (
	"time"

	"github.com/compforge/go-stdx/timeline/manager"
)

// StageOption sets the identity, source start time or initial attributes of a stage.
// Implementations apply options synchronously before returning a handle.
type StageOption = manager.StageOption

// EndOption supplies a source end time, final attributes or a code.
type EndOption = manager.EndOption

func WithStageID(id StageID) StageOption { return manager.WithStageID(id) }

// WithParent records an association without looking up or requiring the parent.
// The ID is preserved even when the parent is absent or arrives later.
func WithParent(id StageID) StageOption      { return manager.WithParent(id) }
func WithStartTime(at time.Time) StageOption { return manager.WithStartTime(at) }
func WithStageActor(actor Actor) StageOption { return manager.WithStageActor(actor) }
func WithEndTime(at time.Time) EndOption     { return manager.WithEndTime(at) }
func WithAttributes(attributes ...Attribute) StageOption {
	return manager.WithAttributes(attributes...)
}

// WithCode records a caller-defined code independently of the stage result.
// An empty code is omitted from JSON.
func WithCode(code string) EndOption { return manager.WithCode(code) }
func WithEndAttributes(attributes ...Attribute) EndOption {
	return manager.WithEndAttributes(attributes...)
}
