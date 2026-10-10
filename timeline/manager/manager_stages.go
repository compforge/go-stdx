package manager

import (
	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

// A handle identifies a stage independently of cache residency. If unsaved
// facts were lost on eviction, later operations can return ErrStageNotFound.
type stageHandle struct {
	manager    *Manager
	timelineID string
	id         StageID
	actor      string
}

func (s *stageHandle) ID() StageID { return s.id }

// Begin records a running stage. Same-name parallel stages are allowed; retain
// their handles to disambiguate completion.
func (m *Manager) Begin(id, name string, options ...StageOption) (StageHandle, error) {
	var stageID StageID
	var actor string
	err := m.apply(id, func(r *recorder, _ store.Document) error {
		handle := r.Begin(name, options...)
		stageID = handle.ID()
		if stage, ok := handle.(*recordedStage); ok {
			actor = stage.record.Actor.Key()
		}
		return r.Err()
	})
	if err != nil {
		return nil, err
	}
	return &stageHandle{manager: m, timelineID: id, id: stageID, actor: actor}, nil
}

// End completes the uniquely named running stage, restoring it on a cache miss.
// A rejected admission leaves it running and available for retry.
func (m *Manager) End(id, name string, stageErr error, options ...EndOption) error {
	selector := Stage{Actor: m.actor}
	for _, option := range options {
		if err := option(&selector); err != nil {
			return err
		}
	}
	if selector.Actor.Key() == "" {
		return model.ErrInvalidStage
	}
	return m.apply(id, func(r *recorder, doc store.Document) error {
		var selected *store.StageUpdate
		for i := range doc.Stages {
			stage := &doc.Stages[i]
			if stage.Actor.Key() != selector.Actor.Key() || stage.Name != name || !stage.FinishedAt.IsZero() {
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
func (s *stageHandle) apply(fn func(StageHandle) error) error {
	return s.manager.apply(s.timelineID, func(r *recorder, doc store.Document) error {
		for _, record := range doc.Stages {
			if record.ID == s.id && record.Actor.Key() == s.actor {
				return fn(r.RestoreStage(record))
			}
		}
		return ErrStageNotFound
	})
}
func (s *stageHandle) End(err error, options ...EndOption) error {
	return s.apply(func(stage StageHandle) error { return stage.End(err, options...) })
}
func (s *stageHandle) SetAttributes(attributes ...Attribute) error {
	return s.apply(func(stage StageHandle) error { return stage.SetAttributes(attributes...) })
}
