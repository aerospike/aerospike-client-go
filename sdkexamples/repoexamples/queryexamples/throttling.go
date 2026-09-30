package queryexamples

import (
	"context"
	"fmt"
)

// DemonstrateQueryThrottling runs the same customer scan two ways: capped
// to a fixed rate with RecordsPerSecond, and requested in server-side
// chunks with ChunkSize — the two server-load controls the source Java
// example exercises (recordsPerSecond, chunkSize + hasMoreChunks).
//
// GAP: sdk/PRD.md (10.7, 10.14) gives ChunkSize and HasMoreChunks no more
// detail than one-line table rows — "query cursor", "Keep" — with no text
// anywhere on how HasMoreChunks is meant to pair with Iter/Next to
// actually demarcate a chunk boundary (does one Iter/Next call stay
// within the current chunk, or does it range over the whole stream
// regardless of ChunkSize?). Java's own hasMoreChunks()/forEach() pairing
// doesn't resolve this either, since forEach's chunk-scoping isn't
// PRD-defined for the Go stream. Rather than invent a loop shape around
// that undefined contract, this consumes the chunked stream the same way
// as every other stream in this package (Iter + Err), and calls
// HasMoreChunks separately, once, purely to show the method exists and
// report what it returns — not to demarcate chunks in the loop below.
func (s *Service) DemonstrateQueryThrottling(ctx context.Context) error {
	throttled, err := s.session.Scan(ctx, s.customerDS.DataSet()).
		RecordsPerSecond(1).
		Execute()
	if err != nil {
		return fmt.Errorf("scan customers at 1 rps: %w", err)
	}
	defer throttled.Close()

	count := 0
	for row, rowErr := range throttled.Iter(ctx) {
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
	if err := throttled.Err(); err != nil {
		return fmt.Errorf("scan customers at 1 rps: %w", err)
	}
	fmt.Printf("throttled scan: %d customers at 1 record/sec\n", count)

	chunked, err := s.session.Scan(ctx, s.customerDS.DataSet()).
		ChunkSize(10).
		Execute()
	if err != nil {
		return fmt.Errorf("scan customers in chunks of 10: %w", err)
	}
	defer chunked.Close()

	count = 0
	for row, rowErr := range chunked.Iter(ctx) {
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
	if err := chunked.Err(); err != nil {
		return fmt.Errorf("scan customers in chunks of 10: %w", err)
	}
	fmt.Printf("chunked scan: %d customers requested in chunks of 10\n", count)

	hasMore, err := chunked.HasMoreChunks()
	if err != nil {
		return fmt.Errorf("check for more chunks: %w", err)
	}
	fmt.Printf("HasMoreChunks after fully draining the stream: %v\n", hasMore)
	return nil
}
