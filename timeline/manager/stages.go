package manager

import (
	"sync"

	"github.com/compforge/go-stdx/timeline"
)

// stageHandle composes core timing with cache ownership. End only releases the
// stage's name after the terminal fact has been accepted by the submission queue.
type stageHandle struct {
	mu      sync.Mutex
	manager *Manager
	local   *recordingScope
	name    string
	stage   timeline.StageHandle
}

func (s *stageHandle) ID() timeline.StageID { return s.stage.ID() }

// Begin records a stage and indexes it by name for End. Same-name parallel
// stages are allowed; retain their handles to disambiguate completion.
func (m *Manager) Begin(id, name string, options ...timeline.StageOption) (timeline.StageHandle, error) {
	m.mu.Lock()
	local, err := m.localLocked(id)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if m.stageCountLocked() >= m.config.MaxActiveStages {
		m.mu.Unlock()
		return nil, ErrStageLimit
	}
	r := m.recorderLocked(local, timeline.Actor{})
	// Reserve name/capacity before encoding options without the Manager lock.
	s := &stageHandle{manager: m, local: local.recordingScope, name: name}
	if local.stages[name] == nil {
		local.stages[name] = make(map[*stageHandle]struct{})
	}
	local.stages[name][s] = struct{}{}
	s.mu.Lock()
	m.mu.Unlock()
	s.stage = r.Begin(name, options...)
	err = r.Err()
	if err != nil {
		s.remove()
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *stageHandle) remove() {
	m := s.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.cache.Get(s.local.id)
	if item == nil || item.Value().recordingScope != s.local {
		return
	}
	local := item.Value()
	stages := local.stages[s.name]
	delete(stages, s)
	if len(stages) == 0 {
		delete(local.stages, s.name)
	}
}

func (s *stageHandle) End(stageErr error, options ...timeline.EndOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stage.End(stageErr, options...); err != nil {
		return err
	}
	s.remove()
	return nil
}

func (s *stageHandle) SetAttributes(attributes ...timeline.Attribute) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stage.SetAttributes(attributes...)
}

func (m *Manager) stage(id, name string) (*stageHandle, error) {
	if id == "" {
		return nil, timeline.ErrEmptyID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if item := m.cache.Get(id); item != nil {
		stages := item.Value().stages[name]
		if len(stages) > 1 {
			return nil, ErrAmbiguousStage
		}
		for stage := range stages {
			return stage, nil
		}
	}
	return nil, ErrStageNotFound
}

// End ends the uniquely named running stage. A rejected submission leaves the
// name available for retry; an accepted End removes it. Explicit handles remain
// idempotent. Use the handles for concurrent stages sharing the same name.
func (m *Manager) End(id, name string, stageErr error, options ...timeline.EndOption) error {
	s, err := m.stage(id, name)
	if err != nil {
		return err
	}
	return s.End(stageErr, options...)
}
