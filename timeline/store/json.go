package store

import (
	"encoding/json"

	"github.com/compforge/go-stdx/timeline/model"
)

type stageUpdateJSON struct {
	model.StageJSON
	Revision uint64 `json:"revision"`
}

// Aliases prevent recursive MarshalJSON calls. The explicit Stages fields below
// replace the embedded full stages while retaining all document metadata.
type documentData Document
type documentJSON struct {
	LegacyAttributes map[string]json.RawMessage `json:"fields,omitempty"`
	documentData
	Actors []model.Actor     `json:"actors,omitempty"`
	Stages []stageUpdateJSON `json:"stages,omitempty"`
}

// MarshalJSON stores each distinct nonempty Actor once. actor_ref is 1-based and
// local to this payload; serialization neither mutates d nor changes stage order.
func (d Document) MarshalJSON() ([]byte, error) {
	wire := documentJSON{documentData: documentData(d)}
	var table model.ActorTable
	if d.Stages != nil {
		wire.Stages = make([]stageUpdateJSON, len(d.Stages))
		for i, stage := range d.Stages {
			wire.Stages[i] = stageUpdateJSON{StageJSON: table.Encode(stage.Stage), Revision: stage.Revision}
		}
	}
	wire.Actors = table.Actors
	return json.Marshal(wire)
}

// UnmarshalJSON resolves payload-local actor references back to full Actor values.
// Invalid references return an error without replacing the receiver.
func (d *Document) UnmarshalJSON(raw []byte) error {
	var wire documentJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	next := Document(wire.documentData)
	if next.Attributes == nil {
		next.Attributes = wire.LegacyAttributes
	}
	if wire.Stages != nil {
		next.Stages = make([]StageUpdate, len(wire.Stages))
		for i, stage := range wire.Stages {
			data, err := stage.StageJSON.Decode(wire.Actors)
			if err != nil {
				return err
			}
			next.Stages[i] = StageUpdate{Stage: data, Revision: stage.Revision}
		}
	}
	*d = next
	return nil
}
