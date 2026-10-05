package timeline

import "encoding/json"

// AttributeValue decodes the most recent value for key using encoding/json.
// It returns false for a missing key or a JSON value incompatible with T.
func AttributeValue[T any](attributes map[string]json.RawMessage, key string) (T, bool) {
	var value T
	raw, ok := attributes[key]
	if !ok || json.Unmarshal(raw, &value) != nil {
		var zero T
		return zero, false
	}
	return value, true
}

// RunningStages returns all running stages in snapshot order. Concurrent stages
// are separate facts: the last-started stage is not necessarily the blocker.
func (s Snapshot) RunningStages() []Stage {
	var result []Stage
	for _, stage := range s.Stages {
		if stage.Status == Running {
			result = append(result, stage)
		}
	}
	return result
}

// LatestFailedStage returns the most recently finished failed or canceled stage.
// Equal finish times are broken by ID, independently of snapshot slice order.
// It reports temporal order, not a root-cause or critical-path judgment.
func (s Snapshot) LatestFailedStage() (Stage, bool) {
	var result Stage
	found := false
	for _, stage := range s.Stages {
		if stage.Status != Failed && stage.Status != Canceled {
			continue
		}
		if !found || stage.FinishedAt.After(result.FinishedAt) ||
			(stage.FinishedAt.Equal(result.FinishedAt) && stage.ID > result.ID) {
			result, found = stage, true
		}
	}
	return result, found
}
