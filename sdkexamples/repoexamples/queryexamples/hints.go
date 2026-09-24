package queryexamples

import (
	"context"
	"fmt"
)

// DemonstrateQueryFiltering runs the filtered query ($.age > 30) that the
// source Java example's five query-hint variants are all built around.
//
// GAP: the hint mechanism itself has no PRD equivalent at all. Java shows
// five variants of the same query — hint by secondary-index name
// (forIndex), hint by bin name (forBin), an explicit expected query
// duration (queryDuration), and two ways of combining them — but
// QueryBuilder's full method set (10.7) has no WithHint, no ForIndex, no
// ForBin, no QueryDuration anywhere. There's nothing to call, so this
// runs the one query all five variants share instead of five near-
// identical calls that would only differ by an argument that doesn't
// exist.
func (s *Service) DemonstrateQueryFiltering(ctx context.Context) error {
	stream, err := s.session.Query(ctx, s.customerDS.DataSet()).
		WhereAEL(fmt.Sprintf("$.%s > 30", customerAgeBin)).
		Execute()
	if err != nil {
		return fmt.Errorf("query customers with age > 30: %w", err)
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
		return fmt.Errorf("query customers with age > 30: %w", err)
	}
	fmt.Printf("found %d customers with age > 30\n", count)
	return nil
}
