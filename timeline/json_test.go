package timeline_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
)

func actorDocument() timeline.Document {
	at := time.Date(2026, 9, 28, 1, 0, 0, 0, time.UTC)
	actors := []timeline.Actor{{}, {ID: "pod-a", Name: "sandctl-a"}, {ID: "pod-a", Name: "sandctl-a"}, {ID: "pod-a"}, {Name: "sandctl-a"}, {ID: "pod-a", Name: "renamed"}}
	doc := timeline.Document{ID: "operation", RootStageID: "operation:operation", OperationRecord: timeline.OperationRecord{
		Revision: 3, Operation: "start", StartedAt: at, FinishedAt: at.Add(time.Minute), Status: timeline.Failed, Error: "timeout",
		Fields: map[string]json.RawMessage{"attempt": json.RawMessage(`9007199254740993`)},
	}}
	for i, actor := range actors {
		doc.Stages = append(doc.Stages, timeline.StageUpdate{Revision: uint64(i + 1), Stage: timeline.Stage{
			ID: timeline.StageID(fmt.Sprint(i)), ParentID: doc.RootStageID, Name: "step", Actor: actor,
			StartedAt: at.Add(time.Duration(i) * time.Second), FinishedAt: at.Add(time.Duration(i+1) * time.Second), Elapsed: time.Second,
			Status: timeline.Failed, Error: "source failure", Fields: map[string]json.RawMessage{"count": json.RawMessage(`18446744073709551615`)},
		}})
	}
	return doc
}

func assertActorWire(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Actors []timeline.Actor             `json:"actors"`
		Stages []map[string]json.RawMessage `json:"stages"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	wantActors := []timeline.Actor{{ID: "pod-a", Name: "sandctl-a"}, {ID: "pod-a"}, {Name: "sandctl-a"}, {ID: "pod-a", Name: "renamed"}}
	if !reflect.DeepEqual(wire.Actors, wantActors) {
		t.Fatalf("actors: %+v", wire.Actors)
	}
	wantRefs := []uint64{0, 1, 1, 2, 3, 4}
	if len(wire.Stages) != len(wantRefs) {
		t.Fatalf("stages: %s", raw)
	}
	for i, stage := range wire.Stages {
		if _, ok := stage["actor"]; ok {
			t.Fatalf("inline actor: %s", raw)
		}
		ref := uint64(0)
		if encoded, ok := stage["actor_ref"]; ok {
			if err := json.Unmarshal(encoded, &ref); err != nil {
				t.Fatal(err)
			}
			if ref == 0 {
				t.Fatal("empty reference must be omitted")
			}
		}
		if ref != wantRefs[i] {
			t.Fatalf("stage %d: ref %d", i, ref)
		}
	}
	return raw
}

func TestActorDictionaryDocumentAndSnapshotRoundTrip(t *testing.T) {
	original := actorDocument()
	raw := assertActorWire(t, original)
	var restored timeline.Document
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored) {
		t.Fatalf("document round trip: %#v", restored)
	}
	if !reflect.DeepEqual(original, actorDocument()) {
		t.Fatal("encoding mutated document")
	}
	// Public snapshots resolve actors, retain collection metadata and preserve integers.
	snapshot := original.Snapshot(original.FinishedAt)
	snapshot.Collection = timeline.Collection{LocalFlushed: true, StoreRead: true}
	encoded := assertActorWire(t, snapshot)
	var decoded timeline.Snapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, decoded) {
		t.Fatalf("snapshot round trip: %#v", decoded)
	}
	replay, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(encoded, replay) {
		t.Fatalf("unstable encoding: %s %v", replay, err)
	}
	// Dictionary scope ends at the payload: each returned Actor is an independent value.
	restored.Stages[1].Actor.Name = "changed"
	if restored.Stages[2].Actor.Name != "sandctl-a" || original.Stages[1].Actor.Name != "sandctl-a" {
		t.Fatal("actor alias")
	}
	// Standalone records remain self-contained for Record and transport between writers.
	standalone, err := json.Marshal(original.Stages[1].Stage)
	if err != nil || !bytes.Contains(standalone, []byte(`"actor":`)) || bytes.Contains(standalone, []byte(`"actor_ref":`)) {
		t.Fatalf("standalone stage: %s %v", standalone, err)
	}
}

func TestActorDictionaryRejectsInvalidReferencesAtomically(t *testing.T) {
	for _, input := range []string{
		`{"stages":[{"id":"s","actor_ref":1}]}`,
		`{"actors":[{"id":"a"}],"stages":[{"id":"s","actor_ref":2}]}`,
		`{"actors":[{}],"stages":[{"id":"s","actor_ref":1}]}`,
		`{"actors":[{"id":"a"}],"stages":[{"id":"s","actor_ref":-1}]}`,
		`{"actors":[{"id":"a"}],"stages":[{"id":"s","actor_ref":1.5}]}`,
		`{"actors":[{"id":"a"}],"stages":[{"id":"s","actor_ref":18446744073709551615}]}`,
	} {
		t.Run(input, func(t *testing.T) {
			original := actorDocument()
			doc := original
			if err := json.Unmarshal([]byte(input), &doc); err == nil {
				t.Fatal("accepted invalid reference")
			}
			if !reflect.DeepEqual(doc, original) {
				t.Fatal("partially replaced document")
			}
			snapshot := original.Snapshot(original.FinishedAt)
			before := snapshot
			if err := json.Unmarshal([]byte(input), &snapshot); err == nil {
				t.Fatal("accepted invalid snapshot reference")
			}
			if !reflect.DeepEqual(snapshot, before) {
				t.Fatal("partially replaced snapshot")
			}
		})
	}
}

func TestActorDictionaryOmittedForUnattributedStages(t *testing.T) {
	doc := actorDocument()
	doc.Stages = doc.Stages[:1]
	for _, value := range []any{doc, doc.Snapshot(doc.FinishedAt), timeline.Document{}, timeline.Snapshot{}} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(`"actors"`)) || bytes.Contains(raw, []byte(`"actor_ref"`)) {
			t.Fatalf("empty actors: %s", raw)
		}
		switch original := value.(type) {
		case timeline.Document:
			var decoded timeline.Document
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, decoded) {
				t.Fatalf("actorless document round trip: %+v", decoded)
			}
		case timeline.Snapshot:
			var decoded timeline.Snapshot
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, decoded) {
				t.Fatalf("actorless snapshot round trip: %+v", decoded)
			}
		}
	}
	var docZero timeline.Document
	if err := json.Unmarshal([]byte(`{"stages":[{"id":"s","actor_ref":0}]}`), &docZero); err != nil || docZero.Stages[0].Actor != (timeline.Actor{}) {
		t.Fatalf("zero reference: %+v %v", docZero, err)
	}
}

func TestActorDictionaryReducesRepeatedActorPayload(t *testing.T) {
	doc := actorDocument()
	stage := doc.Stages[1]
	stage.Actor = timeline.Actor{ID: strings.Repeat("u", 36), Name: strings.Repeat("pod", 20)}
	doc.Stages = nil
	for i := range 100 {
		stage.ID = timeline.StageID(fmt.Sprint(i))
		doc.Stages = append(doc.Stages, stage)
	}
	compact, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	// A distinct type bypasses Document's codec and represents the inline layout.
	type inlineDocument timeline.Document
	inline, err := json.Marshal(inlineDocument(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(compact) >= len(inline) {
		t.Fatalf("compact=%d inline=%d", len(compact), len(inline))
	}
	t.Logf("100 stages sharing one actor: inline=%d compact=%d saved=%d bytes", len(inline), len(compact), len(inline)-len(compact))
}
