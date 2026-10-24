package partition

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstratePartitionQuery mirrors the source Java test
// shouldPaginateRecordForSpecificPartition(): seed records until exactly
// numRecords of them land in targetPartition (checked via the classic
// client's real (*as.Key).PartitionId(), matching Java's
// Partition.getPartitionId(key.digest)), then query scoped to that one
// partition, paginated in chunks, up to a limit.
//
// GAP: the source test's consumption loop is a genuinely nested
// traversal — while(hasMoreChunks()) { while(hasNext()) { next(); } } —
// not reproduced here, for two separate reasons: (1) ReadStream
// (sdk/stream.go) has no HasNext() at all — only NavigatableStream
// does — so that exact shape isn't constructible against the real type
// regardless of chunking semantics; (2) even if it were, how
// HasMoreChunks is meant to interact with Iter/Next to demarcate a
// chunk boundary is undefined anywhere in sdk/PRD.md (10.7, 10.14) —
// same open question already documented in
// repoexamples/queryexamples/throttling.go. Rather than invent an
// interaction shape around an unbuildable one, this drains the stream
// with the same plain Iter+Err pattern as every other stream in these
// examples, and calls HasMoreChunks separately, once, purely to show
// the method exists.
func (s *Service) DemonstratePartitionQuery(ctx context.Context, targetPartition, numRecords, limit, chunkSize int) error {
	var inserted int64
	for candidate := int64(0); inserted < int64(numRecords); candidate++ {
		key := sdk.Key(s.ds, fmt.Sprintf("pq_%d", candidate))
		if key.PartitionId() != targetPartition {
			continue
		}
		if _, err := s.session.Upsert(ctx, key).
			Set(binName, inserted).
			ExecuteOne(); err != nil {
			return fmt.Errorf("seed record %d in partition %d: %w", inserted, targetPartition, err)
		}
		inserted++
	}

	stream, err := s.session.Query(ctx, s.ds).
		OnPartition(targetPartition).
		Bins(binName).
		Limit(limit).
		ChunkSize(chunkSize).
		Execute()
	if err != nil {
		return fmt.Errorf("query partition %d: %w", targetPartition, err)
	}
	defer stream.Close()

	count := 0
	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil {
			fmt.Printf("  Error: %v\n", rowErr)
			continue
		}
		if _, err := row.Record(); err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}
		count++
	}
	if err := stream.Err(); err != nil {
		return fmt.Errorf("query partition %d: %w", targetPartition, err)
	}

	hasMore, err := stream.HasMoreChunks()
	if err != nil {
		return fmt.Errorf("check for more chunks: %w", err)
	}
	fmt.Printf("partition %d: seeded %d records, queried %d (limit %d, chunk size %d), HasMoreChunks after draining: %v\n",
		targetPartition, inserted, count, limit, chunkSize, hasMore)
	return nil
}
