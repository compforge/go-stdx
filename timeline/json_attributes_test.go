package timeline_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/compforge/go-stdx/timeline"
)

func TestLegacyAttributesRoundTrip(t *testing.T) {
	doc := actorDocument()
	for _, original := range []any{doc, doc.Snapshot(doc.FinishedAt), doc.Stages[1].Stage, doc.Stages[1]} {
		t.Run(reflect.TypeOf(original).Name(), func(t *testing.T) {
			raw, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			legacy := bytes.ReplaceAll(raw, []byte(`"attributes":`), []byte(`"fields":`))
			restored := reflect.New(reflect.TypeOf(original))
			if err := json.Unmarshal(legacy, restored.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, restored.Elem().Interface()) {
				t.Fatalf("lost persisted attributes, actor, or revision: %s", legacy)
			}
			encoded, err := json.Marshal(restored.Interface())
			if err != nil || !bytes.Equal(encoded, raw) {
				t.Fatalf("noncanonical encoding: %s, %v", encoded, err)
			}
		})
	}
}

func TestAttributesTakePrecedenceOverLegacyKey(t *testing.T) {
	for _, input := range []string{
		`{"fields":{"legacy":true},"attributes":{"current":9007199254740993}}`,
		`{"fields":{"legacy":true},"attributes":{}}`,
	} {
		for _, dst := range []any{&timeline.Stage{}, &timeline.StageUpdate{}, &timeline.Document{}, &timeline.Snapshot{}} {
			if err := json.Unmarshal([]byte(input), dst); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(dst)
			if err != nil || bytes.Contains(raw, []byte("legacy")) || bytes.Contains(raw, []byte(`"fields":`)) {
				t.Fatalf("legacy key won for %T: %s, %v", dst, raw, err)
			}
			if bytes.Contains([]byte(input), []byte("current")) && !bytes.Contains(raw, []byte(`"current":9007199254740993`)) {
				t.Fatalf("current attribute lost for %T: %s", dst, raw)
			}
		}
		// Document/Snapshot use a compact stage codec rather than Stage.UnmarshalJSON.
		for _, dst := range []any{&timeline.Document{}, &timeline.Snapshot{}} {
			if err := json.Unmarshal([]byte(`{"stages":[`+input+`]}`), dst); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(dst)
			if err != nil || bytes.Contains(raw, []byte("legacy")) {
				t.Fatalf("nested legacy key won for %T: %s, %v", dst, raw, err)
			}
		}
	}
}
