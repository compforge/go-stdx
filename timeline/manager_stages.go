package timeline

// registerStage runs under the owning Recorder lock. All named lookups release
// the Manager lock before delegating, so a name index never serializes store IO.
func (m *Manager) registerStage(stage *recordedStage) error {
	if stage.record.Name == "" {
		return ErrInvalidStage
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	if m.activeStages >= m.config.MaxActiveStages {
		return ErrActiveStageLimit
	}
	local, err := m.localLocked(stage.owner.id)
	if err != nil {
		return err
	}
	stages := local.stages[stage.record.Name]
	if stages == nil {
		stages = make(map[*recordedStage]struct{})
		local.stages[stage.record.Name] = stages
	}
	stages[stage] = struct{}{}
	m.activeStages++
	return nil
}

func (m *Manager) removeStage(stage *recordedStage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	local := m.active[stage.owner.id]
	if local == nil {
		return
	}
	stages := local.stages[stage.record.Name]
	if _, ok := stages[stage]; !ok {
		return
	}
	delete(stages, stage)
	m.activeStages--
	if len(stages) == 0 {
		delete(local.stages, stage.record.Name)
	}
	m.removeIdleLocked(stage.owner.id, local)
}

func (m *Manager) stage(id, name string) (*recordedStage, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrManagerClosed
	}
	if local := m.active[id]; local != nil {
		stages := local.stages[name]
		if len(stages) > 1 {
			return nil, ErrAmbiguousStage
		}
		for stage := range stages {
			return stage, nil
		}
	}
	return nil, ErrStageNotFound
}

// End ends the single active local stage with this name. Repeated names are
// allowed, but concurrent same-name stages require their handles to disambiguate.
// Ended stages leave the name index; calling again returns ErrStageNotFound.
func (m *Manager) End(id, name string, stageErr error, options ...EndOption) error {
	stage, err := m.stage(id, name)
	if err != nil {
		return err
	}
	stage.End(stageErr, options...)
	return stage.owner.recordingError()
}

// SetStageAttributes updates the single active local stage with this name.
// It never changes operation attributes or takes over another process's stage.
func (m *Manager) SetStageAttributes(id, name string, attributes ...Attribute) error {
	stage, err := m.stage(id, name)
	if err != nil {
		return err
	}
	stage.SetAttributes(attributes...)
	return stage.owner.recordingError()
}
