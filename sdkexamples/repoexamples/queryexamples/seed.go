package queryexamples

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// checkWrite closes stream and walks it, returning an error if any entry
// failed or didn't apply (D-19: Affected, not just a nil error, is what
// confirms a conditional write actually took effect). Same helper as
// ecommerce's — duplicated rather than shared because these are two
// independent example packages, not a shared library.
func checkWrite(ctx context.Context, stream *sdk.WriteStream, execErr error, op string) error {
	if execErr != nil {
		return fmt.Errorf("%s: %w", op, execErr)
	}
	defer stream.Close()
	for result, err := range stream.Iter(ctx) {
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		if !result.Affected {
			return fmt.Errorf("%s: write to %v did not apply", op, result.Key)
		}
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// SeedCustomer upserts one customer record, so later scenarios have
// something known to operate on.
func (s *Service) SeedCustomer(ctx context.Context, c Customer) error {
	key := sdk.Key(s.customerDS.DataSet(), c.ID)
	if _, err := s.session.Upsert(ctx, key).
		Set(customerNameBin, c.Name).
		Set(customerAgeBin, int64(c.Age)).
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed customer %d: %w", c.ID, err)
	}
	return nil
}
