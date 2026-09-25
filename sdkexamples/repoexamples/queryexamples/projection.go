package queryexamples

import (
	"context"
	"fmt"
)

// DemonstrateBinProjection queries customers over 21 and projects just the
// name bin, aliased to "displayName" in the result — QueryBuilder.OnBin's
// only real terminal (10.7): OnBin(name) *QueryBinBuilder, then SelectAs
// to rename the projected bin.
//
// GAP: the PRD's own 10.7 "Look" text for this exact row shows a
// different call — .OnBin("doc").OnMapKey("title", nil).Get() — CDT
// navigation terminating in a read, not a rename. But sdk/query.go's
// actual QueryBinBuilder only has SelectAs; there's no OnMapKey or Get
// anywhere on it. So the PRD's own narrative example for OnBin doesn't
// match its own API stub for the same row — the same kind of PRD-text-
// vs-PRD-stub mismatch already flagged for Task/BackgroundTask naming.
// This demonstrates the one real method that exists (SelectAs) rather
// than the narrative example, which doesn't compile against the current
// stub.
//
// GAP: the source Java example also has an expected-failure test right
// next to its object-mapping demo — session.query(ds).where(...).
// bin("fred").get().execute() is expected to throw BinOpInvalidException,
// because bin-level query operations aren't supported server/client-side
// yet. That test doesn't translate here: QueryBinBuilder has no .Get() to
// call in the first place (only SelectAs, above), so there's no call to
// make that would demonstrate the same "this is deliberately rejected"
// behavior — nothing to invoke, and no defined sdk error for it either.
func (s *Service) DemonstrateBinProjection(ctx context.Context) error {
	stream, err := s.session.Query(ctx, s.customerDS.DataSet()).
		WhereAEL(fmt.Sprintf("$.%s > 21", customerAgeBin)).
		OnBin(customerNameBin).SelectAs("displayName").
		Execute()
	if err != nil {
		return fmt.Errorf("query customers projecting name as displayName: %w", err)
	}
	defer stream.Close()

	count := 0
	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		if _, err := row.Record(); err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}
		count++
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("query customers projecting name as displayName: %w", err)
	}
	fmt.Printf("found %d customers over 21, name projected as displayName\n", count)
	return nil
}
