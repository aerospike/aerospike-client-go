package replaceifexists

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateReplaceIfExists ports two of ReplaceTest.java's tests:
// replaceOnlyModifiesOpType() (replace-if-exists on a record that
// already exists — the whole record's content is replaced, not merged,
// so bin1/bin2 disappear once only bin3 is written) and replaceOnly()
// (replace-if-exists on a record that doesn't exist — refused, not
// created).
//
// GAP: sdk/PRD.md's error sentinels (10.17) don't say what
// ReplaceIfExists returns when the key doesn't exist. ErrNotFound is
// the closest fit — it's the general "doesn't exist" sentinel used
// everywhere else (Get, BatchGet miss, Drop of a missing index) — but
// nothing confirms ReplaceIfExists reuses it specifically for this
// case. Treated as the reasonable default, same as
// commonexamples/indexes.go's Drop GAP.
//
// GAP: sdk/'s QueryBuilder only filters a whole DataSet — no
// single-key-scoped query exists (same finding #23 addendum already hit
// in sdktestexamples/listexp, mapexp, partition, pointreads,
// streamdisposition, operate, and deletebin). The source test's
// read-back is session.query(key).execute() — scoped to the whole
// replaceifexists dataset here instead of the one key the source reads.
func (s *Service) DemonstrateReplaceIfExists(ctx context.Context, existingID, missingID string) error {
	existingKey := sdk.Key(s.ds, existingID)
	missingKey := sdk.Key(s.ds, missingID)

	if _, err := s.session.Upsert(ctx, existingKey).
		Set(binName1, "value1").
		Set(binName2, "value2").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", existingID, err)
	}

	if _, err := s.session.ReplaceIfExists(ctx, existingKey).
		Set(binName3, "value3").
		ExecuteOne(); err != nil {
		return fmt.Errorf("replace-if-exists on existing record %s: %w", existingID, err)
	}

	stream, err := s.session.Query(ctx, s.ds).Execute()
	if err != nil {
		return fmt.Errorf("query record %s after replace: %w", existingID, err)
	}
	rec, err := stream.One(ctx)
	stream.Close()
	if err != nil {
		return fmt.Errorf("one() for record %s after replace: %w", existingID, err)
	}
	fmt.Printf("record %s read back after replace (can't verify %s/%s are gone and %s=value3 — see GAP comment): %v\n", existingID, binName1, binName2, binName3, rec)

	if err := s.session.Delete(ctx, missingKey); err != nil {
		return fmt.Errorf("delete leftover record %s: %w", missingID, err)
	}

	_, err = s.session.ReplaceIfExists(ctx, missingKey).
		Set(binName1, "value").
		ExecuteOne()
	if !errors.Is(err, sdk.ErrNotFound) {
		return fmt.Errorf("replace-if-exists on missing record %s: expected ErrNotFound, got %v", missingID, err)
	}
	fmt.Printf("record %s: replace-if-exists correctly refused (record doesn't exist)\n", missingID)
	return nil
}
