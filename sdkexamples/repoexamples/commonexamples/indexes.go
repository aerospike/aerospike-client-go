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
// Source: functionally, every step here is a real port, drawn from two
// separate Java sources (the fluent-builder shape is Go's own idiom —
// the real Java API for this is a flat, non-builder method call).
// `QueryIndexTest.createDrop()` (active test,
// client/src/test/java/com/aerospike/client/sdk/query/QueryIndexTest.java)
// does drop-leftover-tolerating-INDEX_NOTFOUND, create, wait, drop, wait
// — steps 1-3 and 5 here, functionally identical. Step 4 (list every
// index, print the count) isn't in that test, but is real usage in
// `examples/src/main/java/com/aerospike/examples/QueryExamples.java:292`
// (`session.info().secondaryIndexes()`, examples dir, not a test).
// `CommonExample.java:287` (same "common" example family as this Go
// package) separately confirms Create's exact shape — bin `age`, type
// INTEGER. Combined, both real sources cover 100% of this demo's
// functional steps.
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
