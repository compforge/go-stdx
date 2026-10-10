//go:build cgo

package sqlstore_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/compforge/go-stdx/timeline"
	"github.com/compforge/go-stdx/timeline/sqlstore"
)

// The reader has no access to the writers' buffers. Only automatic SQL writes
// can satisfy the assertions; neither Snapshot nor Flush is called by a writer.
func TestManagersPersistAcrossProcesses(t *testing.T) {
	if path := os.Getenv("TIMELINE_MANAGER_TEST_DB"); path != "" {
		store := sqlstore.New(open(t, path))
		m, err := timeline.NewManager(store, timeline.ManagerConfig{FlushInterval: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := m.Shutdown(ctx); err != nil {
				t.Error(err)
			}
		})
		actor := os.Getenv("TIMELINE_MANAGER_TEST_ACTOR")
		r, err := m.New("shared", timeline.WithActor(timeline.Actor{ID: actor}))
		if err != nil {
			t.Fatal(err)
		}
		s := r.Begin("initialize")
		s.End(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for ctx.Err() == nil {
			d, _ := store.Read(ctx, "shared")
			for _, stage := range d.Stages {
				if stage.ID == s.ID() && stage.Status == timeline.Succeeded {
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("stage did not reach SQL before shutdown")
	}
	path := filepath.Join(t.TempDir(), "manager.db")
	db := open(t, path)
	schema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagersPersistAcrossProcesses$")
			cmd.Env = append(os.Environ(), "TIMELINE_MANAGER_TEST_DB="+path, fmt.Sprintf("TIMELINE_MANAGER_TEST_ACTOR=worker-%d", i))
			if out, err := cmd.CombinedOutput(); err != nil {
				failures <- fmt.Errorf("writer %d: %w: %s", i, err, out)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	d, err := sqlstore.New(open(t, path)).Read(ctx, "shared")
	if err != nil || len(d.Stages) != 2 {
		t.Fatalf("independent SQL reader: %+v, %v", d, err)
	}
	if d.Stages[0].Actor == d.Stages[1].Actor || d.Stages[0].Status != timeline.Succeeded || d.Stages[1].Status != timeline.Succeeded {
		t.Fatalf("writer state lost: %+v", d)
	}
}
