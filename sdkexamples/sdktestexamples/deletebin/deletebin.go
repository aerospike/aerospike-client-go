package deletebin

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateDeleteBin ports DeleteBinTest.java's deleteBin(): write two
// bins, remove one of them, then read the record back — the removed bin
// gone, the other untouched.
//
// GAP: sdk/'s QueryBuilder only filters a whole DataSet — no
// single-key-scoped query exists (same finding #23 addendum already hit
// in sdktestexamples/listexp, mapexp, partition, pointreads,
// streamdisposition, and operate). Scoped to the whole deletebin dataset
// here instead of the one key the source reads back.
//
// GAP: can't verify the source test's actual assertions
// (rec.getValue(binName1) == nil, rec.getString(binName2) == "value2") —
// Record (sdk/session.go) has no bin-accessor methods at all (finding
// #16). This can only confirm the remove-bin write and the read-back
// query both complete without error.
func (s *Service) DemonstrateDeleteBin(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	if _, err := s.session.Upsert(ctx, key).
		Set(binName1, "value1").
		Set(binName2, "value2").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", id, err)
	}

	// Set bin value to null to drop the bin — matches the source test's
	// own comment on this exact line.
	if _, err := s.session.Upsert(ctx, key).
		RemoveBin(binName1).
		ExecuteOne(); err != nil {
		return fmt.Errorf("remove %s from record %s: %w", binName1, id, err)
	}

	stream, err := s.session.Query(ctx, s.ds).Bins(binName1, binName2, "bin3").Execute()
	if err != nil {
		return fmt.Errorf("query record %s: %w", id, err)
	}
	defer stream.Close()

	rec, err := stream.One(ctx)
	if err != nil {
		return fmt.Errorf("one() for record %s: %w", id, err)
	}
	fmt.Printf("record %s read back after removing %s (can't verify bin values — see GAP comment): %v\n", id, binName1, rec)
	return nil
}
