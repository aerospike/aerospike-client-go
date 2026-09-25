package queryexamples

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateGenerationCheck shows optimistic-concurrency control via
// IfGeneration: read a record's current generation, commit a write
// conditioned on that generation, then attempt a second conditional write
// against the same now-stale generation and confirm the server rejects it.
func (s *Service) DemonstrateGenerationCheck(ctx context.Context, id int64) error {
	key := sdk.Key(s.customerDS.DataSet(), id)

	record, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d: %w", id, err)
	}
	gen := record.Generation

	// DX GAP: ExecuteOne names the cardinality ("not many"), not the thing
	// you get back. database/sql already solved this exact problem —
	// Query() returns *Rows (many), QueryRow() returns *Row (one) — by
	// naming the singular noun, not a generic quantifier. The equivalent
	// here would read more consistently as something like ExecuteResult()
	// (you get the one WriteResult), matching that precedent; ExecuteOne
	// doesn't say one *what*.
	first, err := s.session.Update(ctx, key).
		OnBin(customerAgeBin).Add(int64(1)).
		IfGeneration(gen).
		ExecuteOne()
	if err != nil {
		return fmt.Errorf("conditional update at generation %d: %w", gen, err)
	}
	if !first.Affected {
		return fmt.Errorf("conditional update at generation %d: did not apply", gen)
	}
	fmt.Printf("first conditional update at generation %d succeeded\n", gen)

	// gen is now stale: the update above advanced the record's real
	// generation, so retrying against the same gen should be rejected.
	// D-19 doesn't pin down whether a generation mismatch surfaces as an
	// error (ErrGenerationMismatch) or as a no-op (err == nil,
	// Affected == false) — both are treated as "correctly rejected" here
	// rather than assuming one mechanism and silently passing if the
	// other is what actually happens.
	second, err := s.session.Update(ctx, key).
		OnBin(customerAgeBin).Add(int64(1)).
		IfGeneration(gen).
		ExecuteOne()
	switch {
	case errors.Is(err, sdk.ErrGenerationMismatch):
		fmt.Println("second conditional update correctly rejected: generation mismatch error")
		return nil
	case err != nil:
		return fmt.Errorf("conditional update at stale generation %d: %w", gen, err)
	case !second.Affected:
		fmt.Println("second conditional update correctly rejected: not applied")
		return nil
	default:
		return fmt.Errorf("conditional update at stale generation %d: expected rejection, got success", gen)
	}
}
