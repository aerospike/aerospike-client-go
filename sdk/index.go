package sdk

import (
	"context"
)

// IndexBuilder is entered via Session.Index(ctx, ds).
type IndexBuilder struct{}

func (b *IndexBuilder) OnBin(name string) *IndexBuilder {
	return b
}

func (b *IndexBuilder) Named(name string) *IndexBuilder {
	return b
}

func (b *IndexBuilder) Numeric() *IndexBuilder {
	return b
}

func (b *IndexBuilder) String() *IndexBuilder {
	return b
}

func (b *IndexBuilder) Geo2DSphere() *IndexBuilder {
	return b
}

func (b *IndexBuilder) Blob() *IndexBuilder {
	return b
}

func (b *IndexBuilder) Collection(t CollectionType) *IndexBuilder {
	return b
}

// Create returns a Task — the same handle type ExecuteBackgroundTask and
// friends return (10.7/10.13 both describe one wait model; this package
// keeps a single Task type rather than a second BackgroundTask name).
func (b *IndexBuilder) Create(ctx context.Context) (*Task, error) {
	return nil, nil
}

func (b *IndexBuilder) Drop(ctx context.Context) error {
	return nil
}

// GAP: CollectionType has no exported constants. §10.13's own table names
// Collection(t) as a real method ("Type | Numeric() / String() /
// Geo2DSphere() / Blob() / Collection(t)") — that part is grounded — but
// nowhere does the PRD specify what values t should take (a LIST/
// MAPKEYS/MAPVALUES-style enum, presumably, matching the classic client's
// IndexCollectionType — but that's evidence a fix is possible, not
// authorization to invent the values myself, same rule as every other
// finding in sdk/FUNCTIONAL_GAPS.md, see finding #27). Until the PRD
// names them, there is no value a caller can pass to Collection(t) at
// all.
type CollectionType int

type IndexInfo struct{}
