package store_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
)

func TestActorScopedIdentityAndLocalRevision(t *testing.T) {
	at := time.Now().UTC()
	base := model.Stage{ID: "same", Name: "work", StartedAt: at, Status: model.Running}
	var doc store.Document
	actors := []model.Actor{{ID: "a", Name: "original"}, {ID: "b"}, {Name: "a"}}
	for _, actor := range actors {
		s := base
		s.Actor = actor
		var err error
		doc, _, err = store.MergeDocument("op", doc, store.Update{Stages: []store.StageUpdate{{Stage: s, Revision: 99}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(doc.Stages) != 3 {
		t.Fatalf("actor namespaces collapsed: %+v", doc)
	}
	updated := base
	updated.Actor = model.Actor{ID: "a", Name: "renamed"}
	updated.FinishedAt = at.Add(time.Second)
	updated.Status = model.Failed
	next, changed, err := store.MergeDocument("op", doc, store.Update{Stages: []store.StageUpdate{{Stage: updated, Revision: 1}}})
	if err != nil || !changed || len(next.Stages) != 3 {
		t.Fatalf("private revision rejected peer: %+v %v", next, err)
	}
	for _, s := range next.Stages {
		if s.Actor.ID == "a" && (s.Status != model.Failed || s.Actor.Name != "renamed") {
			t.Fatal(s)
		}
	}
	repeated, changed, err := store.MergeDocument("op", next, store.Update{Stages: []store.StageUpdate{{Stage: updated, Revision: 200}}})
	if err != nil || changed || !reflect.DeepEqual(next, repeated) {
		t.Fatalf("revision-only retry changed state: %v %v", changed, err)
	}
	invalid := base
	if _, _, err := store.MergeDocument("op", next, store.Update{Stages: []store.StageUpdate{{Stage: invalid}}}); err == nil {
		t.Fatal("empty actor admitted")
	}
}

func TestMemoryBatchReadsAndUpdateWindow(t *testing.T) {
	ctx := context.Background()
	backend := store.NewMemoryStore()
	first := store.OperationRecord{Operation: "task", StartedAt: time.Now(), Status: model.Unknown}
	if err := backend.Merge(ctx, "a", store.Update{Operation: &first}); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	first.Operation = "changed"
	if err := backend.Merge(ctx, "b", store.Update{Operation: &first}); err != nil {
		t.Fatal(err)
	}
	docs, err := backend.Latest(ctx, after, 1)
	if err != nil || len(docs) != 1 || docs[0].ID != "b" {
		t.Fatalf("latest: %+v %v", docs, err)
	}
	after = time.Now()
	if err := backend.Merge(ctx, "b", store.Update{Operation: &first}); err != nil {
		t.Fatal(err)
	}
	if docs, err := backend.Latest(ctx, after, 10); err != nil || len(docs) != 0 {
		t.Fatalf("duplicate refreshed time: %+v %v", docs, err)
	}
	docs, err = backend.MGet(ctx, []string{"b", "missing", "a", "b"})
	if err != nil || len(docs) != 2 {
		t.Fatalf("mget: %+v %v", docs, err)
	}
	docs[0].Operation = "caller mutated"
	actual, _ := backend.Read(ctx, "b")
	if actual.Operation != "changed" {
		t.Fatal("returned document aliases Store")
	}
}
