package prepend

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstratePrepend ports AppendTest.java's prepend(): delete any
// leftover record, prepend "!" onto the (empty) bin, prepend "World"
// (landing in front of what's already there), read the result back, then
// a combined prepend+get in one call.
//
// GAP: sdk/'s QueryBuilder only filters a whole DataSet — no
// single-key-scoped query exists (same finding #23 addendum already hit
// in sdktestexamples/listexp, mapexp, partition, pointreads,
// streamdisposition, operate, and deletebin). Scoped to the whole
// prepend dataset here instead of the one key the source reads back.
//
// GAP: can't verify the source test's actual assertions —
// rec.getString(binName) == "World!" after the two prepends, and
// rec.operationResult(1).getString() == "Hello World!" for the combined
// prepend+get. Record (sdk/session.go) has no bin-accessor methods at
// all (finding #16), and WriteResult (sdk/stream.go) has no value field
// either — the same limitation already documented for
// sdktestexamples/operate and sdktestexamples/udf. This can only confirm
// each write and the read-back query complete without error.
func (s *Service) DemonstratePrepend(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete leftover record %s: %w", id, err)
	}

	if _, err := s.session.Upsert(ctx, key).
		Prepend(binName, "!").
		ExecuteOne(); err != nil {
		return fmt.Errorf("prepend ! onto record %s: %w", id, err)
	}

	if _, err := s.session.Upsert(ctx, key).
		Prepend(binName, "World").
		ExecuteOne(); err != nil {
		return fmt.Errorf("prepend World onto record %s: %w", id, err)
	}

	stream, err := s.session.Query(ctx, s.ds).Bins(binName).Execute()
	if err != nil {
		return fmt.Errorf("query record %s: %w", id, err)
	}
	rec, err := stream.One(ctx)
	stream.Close()
	if err != nil {
		return fmt.Errorf("one() for record %s: %w", id, err)
	}
	fmt.Printf("record %s read back after two prepends (can't verify %q — see GAP comment): %v\n", id, "World!", rec)

	if _, err := s.session.Upsert(ctx, key).
		Prepend(binName, "Hello ").
		Get(binName).
		ExecuteOne(); err != nil {
		return fmt.Errorf("combined prepend+get on record %s: %w", id, err)
	}
	fmt.Printf("record %s: combined prepend+get accepted (can't verify %q — see GAP comment)\n", id, "Hello World!")
	return nil
}
