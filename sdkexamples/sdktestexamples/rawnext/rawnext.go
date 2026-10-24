package rawnext

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateBatchWriteResults ports BatchTest.java's batchWriteComplex()
// (active test): one heterogeneous BatchWrite — two Upserts and a Delete
// to three different keys — whose results are pulled one at a time with
// the raw Next(ctx) terminal in request order, exactly like the source
// test's repeated assertTrue(hasNext())/next() pairs, rather than drained
// with Iter's range-over-func.
func (s *Service) DemonstrateBatchWriteResults(ctx context.Context, id1, id2, id3 string) error {
	key1 := sdk.Key(s.ds, id1)
	key2 := sdk.Key(s.ds, id2)
	key3 := sdk.Key(s.ds, id3)

	ops := []sdk.WriteOp{
		sdk.UpsertOp(key1).Set(binName, 100),
		sdk.UpsertOp(key2).Set(binName, 200),
		sdk.DeleteOp(key3),
	}

	stream, err := s.session.BatchWrite(ctx, ops)
	if err != nil {
		return fmt.Errorf("batch write %s/%s/%s: %w", id1, id2, id3, err)
	}
	defer stream.Close()

	for i, key := range []*as.Key{key1, key2, key3} {
		result, err := stream.Next(ctx)
		if err != nil {
			return fmt.Errorf("next() at position %d (%v): %w", i, key, err)
		}
		fmt.Printf("position %d (%v): affected=%v resultCode=%d\n", i, key, result.Affected, result.ResultCode)
	}

	// GAP: the source test closes with assertFalse(rs.hasNext()) to
	// confirm exhaustion. sdk/'s ReadStream/WriteStream have no HasNext()
	// at all (only NavigatableStream does — same gap already documented
	// in sdktestexamples/partition's DX_GAPS.md), and nothing in
	// sdk/PRD.md's §10.14 says what a 4th Next() call returns once a
	// 3-result stream is exhausted — no documented terminal error or
	// nil-sentinel (the same open question already on record for
	// HasMoreChunks/Next in sdk/PRD.md's §10.14 NOTE). Calling it anyway
	// to show the shape, without asserting anything about the result.
	if _, err := stream.Next(ctx); err != nil {
		return fmt.Errorf("next() past exhaustion: %w", err)
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("batch write %s/%s/%s: %w", id1, id2, id3, err)
	}
	return nil
}
