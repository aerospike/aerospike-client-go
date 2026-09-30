package pointreads

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateGetHeader mirrors the source Java test's underlying intent
// (PutGetTest.java's getHeader(), though Java itself builds it via
// session.query(key).withNoBins() — see the package doc comment):
// write a bin, then read the record's header via Go's own dedicated
// GetHeader convenience.
//
// GAP: the source test then checks rec.generation != 0 directly on the
// returned record. Record (sdk/session.go) is completely opaque — no
// generation or any other accessor (finding #16) — so this can only
// confirm the call is accepted, not that a real generation came back.
func (s *Service) DemonstrateGetHeader(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	if _, err := s.session.Upsert(ctx, key).
		Set(binName, "myvalue").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", id, err)
	}

	if _, err := s.session.GetHeader(ctx, key); err != nil {
		return fmt.Errorf("get header for record %s: %w", id, err)
	}

	fmt.Printf("record %s: header read via GetHeader (can't verify generation/expiration — see GAP comment)\n", id)
	return nil
}

// DemonstrateQueryWithNoBins mirrors TouchTest.java's touch()/
// touchOperate() tests: write a bin, then query for the record's
// metadata only, via WithNoBins — the way Java's real SDK actually
// builds a header-only read (there is no dedicated Session-level method
// on the Java side at all).
//
// GAP: the source test scopes this query to one key
// (session.query(key)). sdk/'s QueryBuilder only filters a whole
// DataSet — no single-key-scoped query exists (same finding #23
// addendum already hit in sdktestexamples/listexp/partition). Scoped to
// the whole dataset here instead. Same Record-opacity limitation as
// DemonstrateGetHeader above — can't verify rec.expiration came back
// non-zero.
func (s *Service) DemonstrateQueryWithNoBins(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	if _, err := s.session.Upsert(ctx, key).
		Set(binName, "myvalue").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", id, err)
	}

	if _, err := s.session.Query(ctx, s.ds).
		WithNoBins().
		Execute(); err != nil {
		return fmt.Errorf("query record %s with WithNoBins: %w", id, err)
	}

	fmt.Printf("record %s: header-only query built via WithNoBins (can't verify expiration — see GAP comment)\n", id)
	return nil
}

// DemonstrateIncludeMissingKeys mirrors BatchTest.java's batchExists():
// write some keys, leave one missing, then run a query that reports
// missing keys rather than silently omitting them.
//
// GAP: the source test scopes this to a specific list of keys
// (session.exists(keys).includeMissingKeys()) — sdk/'s
// IncludeMissingKeys only exists on QueryBuilder (whole-DataSet scope);
// there's no way to attach it to a specific key list at all (BatchGet
// returns a *ReadStream directly, no builder). Scoped to the whole
// dataset here instead — see the package doc comment for the full
// writeup. Same Record-opacity limitation as the other two demos in
// this package: can confirm the query completes, not which keys came
// back as present vs missing.
func (s *Service) DemonstrateIncludeMissingKeys(ctx context.Context, presentID, missingID string) error {
	presentKey := sdk.Key(s.ds, presentID)
	if _, err := s.session.Upsert(ctx, presentKey).
		Set(binName, "myvalue").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", presentID, err)
	}
	// missingID is deliberately never written.

	if _, err := s.session.Query(ctx, s.ds).
		IncludeMissingKeys().
		Execute(); err != nil {
		return fmt.Errorf("query with IncludeMissingKeys: %w", err)
	}

	fmt.Printf("records %s/%s: query built with IncludeMissingKeys (can't verify which came back — see GAP comment)\n", presentID, missingID)
	return nil
}
