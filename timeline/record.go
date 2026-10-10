package timeline

import (
	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

// Record buffers an already completed interval, without inventing timestamps or
// an executor identity. Use a stable ID to retry the same external fact.
func (t *recorder) Record(stage Stage) error {
	if err := model.ValidateCompleted(t.id, stage); err != nil {
		return err
	}
	if stage.ParentID == "" {
		stage.ParentID = model.RootID(t.id)
	}
	stage.Attributes = model.CloneJSONAttributes(stage.Attributes)
	t.mu.Lock()
	defer t.mu.Unlock()
	// A completed import has one immutable state, independent of process-local
	// observation order. Source resource versions never become stage revisions.
	return t.writer.Write(store.Update{Completed: []Stage{stage}})
}
