//go:build cgo

package sqlstore_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline/model"
	"github.com/compforge/go-stdx/timeline/store"
	"github.com/compforge/go-stdx/timeline/store/sqlstore"
)

func TestSQLBatchReadWindowAndStableOrdering(t *testing.T) {
	ctx := context.Background()
	db := open(t, filepath.Join(t.TempDir(), "batch.db"))
	schema(t, db)
	backend := sqlstore.New(db)
	at := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", "c"} {
		op := store.OperationRecord{Operation: id, StartedAt: at, Status: model.Unknown}
		if err := backend.Merge(ctx, id, store.Update{Operation: &op}); err != nil {
			t.Fatal(err)
		}
		updated := at
		if id != "a" {
			updated = at.Add(time.Second)
		}
		if _, err := db.Exec("UPDATE timelines SET updated_at = ? WHERE id = ?", updated, id); err != nil {
			t.Fatal(err)
		}
	}
	docs, err := backend.MGet(ctx, []string{"c", "a", "missing", "c"})
	if err != nil || len(docs) != 2 {
		t.Fatalf("MGet: %+v %v", docs, err)
	}
	ids := map[string]bool{}
	for _, d := range docs {
		ids[d.ID] = true
	}
	if !ids["a"] || !ids["c"] {
		t.Fatal(ids)
	}
	docs, err = backend.Latest(ctx, at, 1)
	if err != nil || len(docs) != 1 || docs[0].ID != "c" {
		t.Fatalf("Latest tie/limit: %+v %v", docs, err)
	}
	docs, err = backend.Latest(ctx, at.Add(time.Second), 10)
	if err != nil || len(docs) != 0 {
		t.Fatalf("exclusive boundary: %+v %v", docs, err)
	}
	op := store.OperationRecord{Operation: "c", StartedAt: at, Status: model.Unknown}
	if err := backend.Merge(ctx, "c", store.Update{Operation: &op}); err != nil {
		t.Fatal(err)
	}
	if docs, err = backend.Latest(ctx, at.Add(time.Second), 10); err != nil || len(docs) != 0 {
		t.Fatalf("duplicate moved updated_at: %+v %v", docs, err)
	}
}
