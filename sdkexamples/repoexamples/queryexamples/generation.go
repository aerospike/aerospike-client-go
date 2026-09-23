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

	record, err := s.session.Get(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("get customer %d: %w", id, err)
	}
	gen := record.Generation

	if _, err := s.session.Update(ctx, key).
		OnBin(customerAgeBin).Add(int64(1)).
		IfGeneration(gen).
		ExecuteOne(); err != nil {
		return fmt.Errorf("conditional update at generation %d: %w", gen, err)
	}
	fmt.Printf("first conditional update at generation %d succeeded\n", gen)

	// gen is now stale: the update above advanced the record's real
	// generation, so retrying against the same gen should be rejected.
	_, err = s.session.Update(ctx, key).
		OnBin(customerAgeBin).Add(int64(1)).
		IfGeneration(gen).
		ExecuteOne()
	switch {
	case errors.Is(err, sdk.ErrGenerationMismatch):
		fmt.Println("second conditional update correctly rejected: generation mismatch")
		return nil
	case err != nil:
		return fmt.Errorf("conditional update at stale generation %d: %w", gen, err)
	default:
		return fmt.Errorf("conditional update at stale generation %d: expected a generation mismatch, got none", gen)
	}
}
