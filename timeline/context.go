package timeline

import (
	"context"

	"github.com/compforge/go-stdx/timeline/manager"
)

// StageRef carries timeline and stage identity across process boundaries.
type StageRef = manager.StageRef

// NewContext optionally carries t without changing ctx's lifetime.
func NewContext(ctx context.Context, t Timeline) context.Context { return manager.NewContext(ctx, t) }

// FromContext retrieves a Timeline explicitly attached with NewContext.
func FromContext(ctx context.Context) (Timeline, bool) { return manager.FromContext(ctx) }

// BeginWithContext preserves cancellation and binds the new stage identity.
// Only WithParent sets parentage; context binding never supplies a parent.
func BeginWithContext(ctx context.Context, t Timeline, name string, opts ...StageOption) (context.Context, StageHandle) {
	return manager.BeginWithContext(ctx, t, name, opts...)
}

func NewStageContext(ctx context.Context, ref StageRef) context.Context {
	return manager.NewStageContext(ctx, ref)
}
func StageFromContext(ctx context.Context) (StageRef, bool) { return manager.StageFromContext(ctx) }
