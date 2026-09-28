package timeline

// FieldValue returns the most recent value for key. Values retain Field's
// immutable, shallow-copy contract. The boolean is false for missing keys or values of another type.
func FieldValue[T any](fields []Field, key string) (T, bool) {
	for i := len(fields) - 1; i >= 0; i-- {
		if fields[i].Key == key {
			value, ok := fields[i].Value.(T)
			return value, ok
		}
	}
	var zero T
	return zero, false
}

// RunningStages returns all running stages in snapshot order. Concurrent stages
// are separate facts: the last-started stage is not necessarily the blocker.
func (s Snapshot) RunningStages() []StageRecord {
	var result []StageRecord
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
func (s Snapshot) LatestFailedStage() (StageRecord, bool) {
	var result StageRecord
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
