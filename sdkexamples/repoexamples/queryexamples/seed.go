package queryexamples

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

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
