package queryexamples

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateTTL upserts a record with a short expiration, confirms it
// reads back immediately, waits past the expiration, then confirms the
// server has actually expired it. The wait is real time, not simulated —
// there's no other way to observe server-side expiration, and the source
// Java example does the same thing (Thread.sleep after a 5s TTL).
func (s *Service) DemonstrateTTL(ctx context.Context, id int64) error {
	key := sdk.Key(s.customerDS.DataSet(), id)

	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete customer %d before TTL test: %w", id, err)
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(customerAgeBin, int64(5)).
		ExpireAfter(5 * time.Second).
		ExecuteOne(); err != nil {
		return fmt.Errorf("upsert customer %d with short TTL: %w", id, err)
	}

	record, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d immediately after upsert: %w", id, err)
	}
	fmt.Printf("customer %d expires at %s (in %s)\n", id, record.Expiration, time.Until(record.Expiration))

	time.Sleep(6 * time.Second)

	_, err = s.session.Get(ctx, key, sdk.AllBins)
	if errors.Is(err, sdk.ErrNotFound) {
		fmt.Printf("customer %d expired as expected after TTL\n", id)
		return nil
	}
	if err != nil {
		return fmt.Errorf("get customer %d after TTL: %w", id, err)
	}
	return fmt.Errorf("expected customer %d to have expired, but it still exists", id)
}
