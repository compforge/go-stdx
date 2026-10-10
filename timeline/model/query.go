package model

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
			(stage.FinishedAt.Equal(result.FinishedAt) && (stage.ID > result.ID || (stage.ID == result.ID && stage.Actor.Key() > result.Actor.Key()))) {
			result, found = stage, true
		}
	}
	return result, found
}
