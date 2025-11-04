package devtest

import "context"

// Scope captures the common capabilities needed to wire orchestration and tracing.
// Both package-level (P) and test-level (T) handles can implement this.
// It intentionally exposes a context-rebinding helper so callers that only
// depend on shared behavior don't need to distinguish between P and T.
type Scope interface {
	CommonT
	TempDir() string
	Cleanup(fn func())

	// WithScope clones the scope with the provided context, preserving the
	// underlying test scope while annotating logging/tracing metadata.
	WithScope(ctx context.Context) Scope
}
