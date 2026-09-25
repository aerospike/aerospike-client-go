package commonexamples

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateIndexCreation runs the full index lifecycle: drop any
// leftover index from a prior run (tolerating "didn't exist"), create a
// secondary index on the age bin, wait for it to complete, list every
// index on the cluster, then drop it again as cleanup — the PRD's full
// index-management surface (10.13): Index, OnBin, Named, Numeric, Create,
// Drop, Task.Wait, ListIndexes. Untouched by both ecommerce and
// queryexamples.
//
// GAP: the PRD's error sentinels (10.17) don't say what Drop returns for
// an index that doesn't exist. ErrNotFound is the closest fit — it's the
// general "doesn't exist" sentinel used everywhere else (Get, BatchGet
// miss) — but nothing in 10.13 confirms Drop reuses it specifically for
// this case rather than some other code or a plain no-op. Treated as the
// reasonable default rather than left unhandled, same as the
// Affected/ErrGenerationMismatch dual-check elsewhere in this repo.
func (s *Service) DemonstrateIndexCreation(ctx context.Context) error {
	const indexName = "common_age_idx"

	if err := s.session.Index(ctx, s.ds).Named(indexName).Drop(ctx); err != nil && !errors.Is(err, sdk.ErrNotFound) {
		return fmt.Errorf("drop leftover index %s: %w", indexName, err)
	}

	task, err := s.session.Index(ctx, s.ds).
		OnBin(ageBin).
		Named(indexName).
		Numeric().
		Create(ctx)
	if err != nil {
		return fmt.Errorf("create index on %s: %w", ageBin, err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for index creation: %w", err)
	}

	indexes, err := s.session.ListIndexes(ctx)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}
	fmt.Printf("indexes on cluster: %d\n", len(indexes))

	if err := s.session.Index(ctx, s.ds).Named(indexName).Drop(ctx); err != nil {
		return fmt.Errorf("drop index %s: %w", indexName, err)
	}
	fmt.Printf("dropped index %s\n", indexName)
	return nil
}
