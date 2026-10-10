package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// References exist only in a complete encoded document. Updates and public
// stages retain full Actor values so independent writers never merge local refs.
// Keep the wire stage explicit: embedding Stage here would also emit its actor.
type StageJSON struct {
	LegacyAttributes map[string]json.RawMessage `json:"fields,omitempty"`
	ID               StageID                    `json:"id"`
	ParentID         StageID                    `json:"parent_id"`
	ActorRef         uint64                     `json:"actor_ref,omitempty"`
	Elapsed          time.Duration              `json:"elapsed_ns,omitempty"`
	Name             string                     `json:"name"`
	StartedAt        time.Time                  `json:"started_at"`
	FinishedAt       time.Time                  `json:"finished_at,omitempty"`
	Status           Status                     `json:"status"`
	Error            string                     `json:"error,omitempty"`
	Code             string                     `json:"code,omitempty"`
	Attributes       map[string]json.RawMessage `json:"attributes,omitempty"`
}

type ActorTable struct {
	Actors []Actor
	refs   map[Actor]uint64
}

func (t *ActorTable) Encode(stage Stage) StageJSON {
	var ref uint64
	if stage.Actor != (Actor{}) {
		if t.refs == nil {
			t.refs = make(map[Actor]uint64)
		}
		ref = t.refs[stage.Actor]
		if ref == 0 {
			t.Actors = append(t.Actors, stage.Actor)
			ref = uint64(len(t.Actors)) // 1-based; zero means no actor.
			t.refs[stage.Actor] = ref
		}
	}
	return StageJSON{
		ID: stage.ID, ParentID: stage.ParentID, ActorRef: ref,
		Elapsed: stage.Elapsed, Name: stage.Name, StartedAt: stage.StartedAt,
		FinishedAt: stage.FinishedAt, Status: stage.Status, Error: stage.Error, Code: stage.Code, Attributes: stage.Attributes,
	}
}

func (s StageJSON) Decode(actors []Actor) (Stage, error) {
	if s.Attributes == nil {
		s.Attributes = s.LegacyAttributes
	}
	var actor Actor
	if s.ActorRef != 0 {
		if s.ActorRef > uint64(len(actors)) || actors[s.ActorRef-1] == (Actor{}) {
			return Stage{}, fmt.Errorf("%w: stage %q has invalid actor_ref %d", ErrInvalidStage, s.ID, s.ActorRef)
		}
		actor = actors[s.ActorRef-1]
	}
	return Stage{
		ID: s.ID, ParentID: s.ParentID, Actor: actor,
		Elapsed: s.Elapsed, Name: s.Name, StartedAt: s.StartedAt,
		FinishedAt: s.FinishedAt, Status: s.Status, Error: s.Error, Code: s.Code, Attributes: s.Attributes,
	}, nil
}

type snapshotData Snapshot
type snapshotJSON struct {
	LegacyAttributes map[string]json.RawMessage `json:"fields,omitempty"`
	snapshotData
	Actors []Actor     `json:"actors,omitempty"`
	Stages []StageJSON `json:"stages,omitempty"`
}

// MarshalJSON uses the same actor dictionary format as Document. Public stages
// remain self-contained in memory and standalone Stage JSON retains its Actor.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	wire := snapshotJSON{snapshotData: snapshotData(s)}
	var table ActorTable
	if s.Stages != nil {
		wire.Stages = make([]StageJSON, len(s.Stages))
		for i, stage := range s.Stages {
			wire.Stages[i] = table.Encode(stage)
		}
	}
	wire.Actors = table.Actors
	return json.Marshal(wire)
}

// UnmarshalJSON resolves actor references, preserving the collection metadata.
// Invalid references return an error without replacing the receiver.
func (s *Snapshot) UnmarshalJSON(raw []byte) error {
	var wire snapshotJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	next := Snapshot(wire.snapshotData)
	if next.Attributes == nil {
		next.Attributes = wire.LegacyAttributes
	}
	if wire.Stages != nil {
		next.Stages = make([]Stage, len(wire.Stages))
		for i, stage := range wire.Stages {
			data, err := stage.Decode(wire.Actors)
			if err != nil {
				return err
			}
			next.Stages[i] = data
		}
	}
	*s = next
	return nil
}
