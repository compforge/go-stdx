package timeline

import (
	"encoding/json"
	"fmt"
)

// Record buffers an already completed interval, without inventing timestamps or
// an executor identity. Use a stable ID to retry the same external fact.
func (t *Recorder) Record(stage Stage) error {
	if err := validateCompleted(t.id, stage); err != nil {
		return err
	}
	if stage.ParentID == "" {
		stage.ParentID = rootID(t.id)
	}
	stage.Attributes = cloneJSONAttributes(stage.Attributes)
	t.mu.Lock()
	defer t.mu.Unlock()
	// A completed import has one immutable state, independent of process-local
	// observation order. Source resource versions never become stage revisions.
	t.pending = append(t.pending, Update{Completed: []Stage{stage}})
	return nil
}

func validateCompleted(id string, stage Stage) error {
	if stage.ID == "" || stage.Name == "" || stage.StartedAt.IsZero() || stage.FinishedAt.IsZero() || stage.FinishedAt.Before(stage.StartedAt) || stage.Elapsed < 0 || stage.ID == stage.ParentID || stage.ID == rootID(id) {
		return ErrInvalidStage
	}
	switch stage.Status {
	case Succeeded, Failed, Canceled:
	default:
		return ErrInvalidStage
	}
	for key, value := range stage.Attributes {
		if !json.Valid(value) {
			return fmt.Errorf("%w: %s", ErrInvalidAttribute, key)
		}
	}
	return nil
}
