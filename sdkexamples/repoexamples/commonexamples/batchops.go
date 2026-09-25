package commonexamples

import (
	"context"
	"errors"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateBatchExistsTouchDelete mirrors the source Java example's
// batch section exactly: one coherent set of keys, checked for existence,
// touched, then deleted, in that order — not three unrelated operations
// on three unrelated key sets. The last id in ids is deliberately left
// unseeded, mirroring Java's set.ids(113,114,999), where 113 and 114 are
// seeded earlier in that file and 999 never is — the mix of existing and
// missing keys is the actual point of the source demo.
//
// GAP: Session.Exists/Touch/Delete (10.4) are point-only — there is no
// single Session verb that takes multiple keys the way Java's
// session.exists(ids...)/touch(ids...)/delete(ids...) each do. Multi-key
// touch/delete goes through BatchWrite with TouchOp/DeleteOp instead
// (10.5's own table: "multi-key → TouchOp/DeleteOp in BatchWrite"); there
// is no BatchExists or ExistsOp at all, so batch-exists goes through
// BatchGet + local exists instead (10.4's own explicit prescription).
// Three Java calls of one shape become two different Go mechanisms.
//
// GAP: Java makes the missing key visible in every result via
// includeMissingKeys() (Key: 999 -> false, printed like any other row).
// Checked sdk/ops.go: WriteOp (TouchOp/DeleteOp's return type) has no
// IncludeMissingKeys method at all, unlike WriteSegmentBuilder and
// QueryBuilder — so there's no way to ask BatchWrite to surface a missing
// key explicitly. What a missing key actually does in a TouchOp/DeleteOp
// batch (silently absent from the stream, a per-row error, Affected ==
// false, or something else) is undocumented — the same ambiguity already
// flagged for point Touch/Delete in pointops.go, one level up. Handled
// defensively below (a per-row error or Affected == false both read as
// "not found"), not asserted as the one true behavior.
func (s *Service) DemonstrateBatchExistsTouchDelete(ctx context.Context, ids []int64) error {
	if len(ids) < 2 {
		return fmt.Errorf("need at least 2 ids (last one left deliberately unseeded)")
	}
	keys := make([]*as.Key, len(ids))
	for i, id := range ids {
		keys[i] = sdk.Key(s.ds, id)
	}
	for i := 0; i < len(keys)-1; i++ {
		if err := s.session.Put(ctx, keys[i], as.BinMap{nameBin: "batch", ageBin: 1}); err != nil {
			return fmt.Errorf("seed record %d: %w", ids[i], err)
		}
	}

	fmt.Println("Batch exists")
	if err := s.batchExists(ctx, keys); err != nil {
		return fmt.Errorf("batch exists: %w", err)
	}

	fmt.Println("Batch touch")
	if err := s.batchTouch(ctx, keys); err != nil {
		return fmt.Errorf("batch touch: %w", err)
	}

	fmt.Println("Batch delete")
	return s.batchDelete(ctx, keys)
}

// GAP: this can't print "Key: X -> exists" the way Java does (rr.key()
// alongside rr.asBoolean()) — checked ReadResult and Record (sdk/
// stream.go, session.go): neither carries the key a row came from.
// WriteResult does (Key field), ReadResult/Record don't. Correlating rows
// back to the input keys slice positionally isn't safe either — nothing
// in the PRD says BatchGet preserves input order. So this only reports a
// count, not a per-key breakdown — printing a fabricated or
// positionally-guessed key would misrepresent what's actually knowable
// here.
func (s *Service) batchExists(ctx context.Context, keys []*as.Key) error {
	stream, err := s.session.BatchGet(ctx, keys, sdk.AllBins)
	if err != nil {
		return err
	}
	defer stream.Close()

	found := 0
	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil && !errors.Is(rowErr, sdk.ErrNotFound) {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		_, recErr := row.Record()
		if recErr == nil {
			found++
		} else if !errors.Is(recErr, sdk.ErrNotFound) {
			fmt.Printf("  Error: %v\n", recErr)
		}
	}
	fmt.Printf("  %d of %d keys exist\n", found, len(keys))
	return stream.Err()
}

func (s *Service) batchTouch(ctx context.Context, keys []*as.Key) error {
	stream, err := s.session.BatchWrite(ctx, []sdk.WriteOp{sdk.TouchOp(keys...)})
	if err != nil {
		return err
	}
	defer stream.Close()

	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil && !errors.Is(rowErr, sdk.ErrNotFound) {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		found := rowErr == nil && row.Affected
		fmt.Printf("  Key: %v -> %v\n", row.Key, found)
	}
	return stream.Err()
}

func (s *Service) batchDelete(ctx context.Context, keys []*as.Key) error {
	stream, err := s.session.BatchWrite(ctx, []sdk.WriteOp{sdk.DeleteOp(keys...)})
	if err != nil {
		return err
	}
	defer stream.Close()

	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil && !errors.Is(rowErr, sdk.ErrNotFound) {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		found := rowErr == nil && row.Affected
		fmt.Printf("  Key: %v -> %v\n", row.Key, found)
	}
	return stream.Err()
}
