package streamdisposition

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateAddAsync mirrors the source Java test AddTest.java's
// addAsync(): two separate increments via StreamOnError on the write
// side, a query read via StreamOnError on the query side, then a
// combined add+get via StreamOnError again.
//
// GAP: can't verify the incremented/read values actually round-trip
// correctly — Record (sdk/session.go) has no bin-accessor methods at
// all (finding #16). Every step here can only confirm the stream call
// completes without error.
func (s *Service) DemonstrateAddAsync(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete record %s before demo: %w", id, err)
	}

	stream, err := s.session.Upsert(ctx, key).
		Add(binName, 10).
		StreamOnError(sdk.InStream())
	if err != nil {
		return fmt.Errorf("add 10 to record %s: %w", id, err)
	}
	stream.Close()

	stream, err = s.session.Upsert(ctx, key).
		Add(binName, 5).
		StreamOnError(sdk.InStream())
	if err != nil {
		return fmt.Errorf("add 5 to record %s: %w", id, err)
	}
	stream.Close()

	readStream, err := s.session.Query(ctx, s.ds).
		Bins(binName).
		StreamOnError(sdk.InStream())
	if err != nil {
		return fmt.Errorf("query record %s: %w", id, err)
	}
	readStream.Close()

	stream, err = s.session.Upsert(ctx, key).
		Add(binName, 30).
		Get(binName).
		StreamOnError(sdk.InStream())
	if err != nil {
		return fmt.Errorf("add 30 and get on record %s: %w", id, err)
	}
	stream.Close()

	fmt.Printf("record %s: three StreamOnError writes plus one StreamOnError query built (can't verify values — see GAP comment)\n", id)
	return nil
}
