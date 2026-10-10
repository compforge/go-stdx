package manager

import (
	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/store"
)

// A handle identifies a stage independently of cache residency. If unsaved
// facts were lost on eviction, later operations can return ErrStageNotFound.
type stageHandle struct {
	manager    *Manager
	timelineID string
	id         timeline.StageID
}

func (s *stageHandle) ID() timeline.StageID { return s.id }

// Begin records a running stage. Same-name parallel stages are allowed; retain
// their handles to disambiguate completion.
func (m *Manager) Begin(id, name string, options ...timeline.StageOption) (timeline.StageHandle, error) {
	var stageID timeline.StageID
	err := m.apply(id, func(r *timeline.Recorder, _ store.Document) error {
		stageID = r.Begin(name, options...).ID()
		return r.Err()
	})
	if err != nil {
		return nil, err
	}
	return &stageHandle{manager: m, timelineID: id, id: stageID}, nil
}

// End completes the uniquely named running stage, restoring it on a cache miss.
// A rejected admission leaves it running and available for retry.
func (m *Manager) End(id, name string, stageErr error, options ...timeline.EndOption) error {
	return m.apply(id, func(r *timeline.Recorder, doc store.Document) error {
		var selected *store.StageUpdate
		for i := range doc.Stages {
			stage := &doc.Stages[i]
			if stage.Name != name || !stage.FinishedAt.IsZero() {
				continue
			}
			if selected != nil {
				return ErrAmbiguousStage
			}
			selected = stage
		}
		if selected == nil {
			return ErrStageNotFound
		}
		return r.RestoreStage(*selected).End(stageErr, options...)
	})
}
func (s *stageHandle) apply(fn func(timeline.StageHandle) error) error {
	return s.manager.apply(s.timelineID, func(r *timeline.Recorder, doc store.Document) error {
		for _, record := range doc.Stages {
			if record.ID == s.id {
				return fn(r.RestoreStage(record))
			}
		}
		return ErrStageNotFound
	})
}
func (s *stageHandle) End(err error, options ...timeline.EndOption) error {
	return s.apply(func(stage timeline.StageHandle) error { return stage.End(err, options...) })
}
func (s *stageHandle) SetAttributes(attributes ...timeline.Attribute) error {
	return s.apply(func(stage timeline.StageHandle) error { return stage.SetAttributes(attributes...) })
}
