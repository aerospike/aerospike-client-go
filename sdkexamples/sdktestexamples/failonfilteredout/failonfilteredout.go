package failonfilteredout

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateFailOnFilteredOut ports the write half of
// FilterExpTest.java's putExcept(): seed two records, one whose bin
// value matches a filter expression and one that doesn't, then run the
// same filtered write against both with FailOnFilteredOut set. The
// matching record's write applies normally; the non-matching one raises
// an error instead of silently no-op'ing.
//
// GAP: sdk/PRD.md's error sentinels (10.17) don't say which sentinel a
// filtered-out write maps to. ErrFilterExpression is the natural fit by
// name — and the source test's own result code is literally
// ResultCode.FILTERED_OUT — but nothing in the PRD text confirms
// FailOnFilteredOut's error reuses this specific sentinel. Treated as
// the reasonable default, same reasoning as the ErrNotFound GAP already
// documented in commonexamples/indexes.go and
// sdktestexamples/replaceifexists.
func (s *Service) DemonstrateFailOnFilteredOut(ctx context.Context, matchID, mismatchID string) error {
	matchKey := sdk.Key(s.ds, matchID)
	mismatchKey := sdk.Key(s.ds, mismatchID)

	if _, err := s.session.Upsert(ctx, matchKey).Set(binA, int64(1)).ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", matchID, err)
	}
	if _, err := s.session.Upsert(ctx, mismatchKey).Set(binA, int64(2)).ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", mismatchID, err)
	}

	if _, err := s.session.Upsert(ctx, matchKey).
		Set(binA, int64(3)).
		WhereAEL("$.A == 1").
		FailOnFilteredOut().
		ExecuteOne(); err != nil {
		return fmt.Errorf("filtered write on matching record %s: %w", matchID, err)
	}
	fmt.Printf("record %s: filter matched, write applied\n", matchID)

	_, err := s.session.Upsert(ctx, mismatchKey).
		Set(binA, int64(3)).
		WhereAEL("$.A == 1").
		FailOnFilteredOut().
		ExecuteOne()
	if !errors.Is(err, sdk.ErrFilterExpression) {
		return fmt.Errorf("filtered write on mismatched record %s: expected ErrFilterExpression, got %v", mismatchID, err)
	}
	fmt.Printf("record %s: filter didn't match, write correctly refused\n", mismatchID)
	return nil
}
