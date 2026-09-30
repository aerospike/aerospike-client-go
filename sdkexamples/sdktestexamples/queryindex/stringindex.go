package queryindex

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

const (
	stringIndexName = "queryindex"
	stringBinName   = "querybin"
	stringKeyPrefix = "querykey"
	stringValue     = "queryvalue"
	stringSize      = 5
)

// DemonstrateStringIndex mirrors the source Java test file
// QueryStringTest.java (active, 2 tests: queryString,
// queryStringEmptyBinName): create a string secondary index, seed 5
// records, then run the same equality filter query twice — once
// projecting the bin, once reading all bins — matching both tests in
// one lifecycle (create → seed → both queries → drop), same setup Java
// shares across both via @BeforeAll/@AfterAll.
//
// GAP: can't verify the query actually matched or what came back —
// Record (sdk/session.go) has no bin-accessor methods at all (finding
// #16). Both queries here can only confirm the call completes.
//
// GAP: the source test wraps Create in a try/catch that specifically
// ignores ResultCode.INDEX_ALREADY_EXISTS, making repeat runs idempotent.
// Checked sdk/errors.go directly: none of its 8 sentinel errors cover
// "index already exists," and sdk/PRD.md never discusses this scenario
// either — there's no PRD-grounded way to distinguish that specific
// failure from any other Create error, so the retry-and-ignore pattern
// isn't reproduced here at all.
func (s *Service) DemonstrateStringIndex(ctx context.Context) error {
	task, err := s.session.Index(ctx, s.ds).
		OnBin(stringBinName).
		Named(stringIndexName).
		String().
		Create(ctx)
	if err != nil {
		return fmt.Errorf("create string index %s: %w", stringIndexName, err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for string index %s: %w", stringIndexName, err)
	}

	for i := 1; i <= stringSize; i++ {
		key := sdk.Key(s.ds, fmt.Sprintf("%s%d", stringKeyPrefix, i))
		value := fmt.Sprintf("%s%d", stringValue, i)
		if _, err := s.session.Upsert(ctx, key).
			Set(stringBinName, value).
			ExecuteOne(); err != nil {
			return fmt.Errorf("seed record %d: %w", i, err)
		}
	}

	filter := fmt.Sprintf("%s3", stringValue)
	ael := fmt.Sprintf("$.%s == '%s'", stringBinName, filter)

	// queryString: project the filtered bin explicitly.
	if _, err := s.session.Query(ctx, s.ds).
		Bins(stringBinName).
		WhereAEL(ael).
		Execute(); err != nil {
		return fmt.Errorf("query string index with bin projection: %w", err)
	}

	// queryStringEmptyBinName: same filter, no bin projection (read all bins).
	if _, err := s.session.Query(ctx, s.ds).
		WhereAEL(ael).
		Execute(); err != nil {
		return fmt.Errorf("query string index without bin projection: %w", err)
	}

	// Matches Java's real dropIndex(set, indexName) exactly — only the
	// index's name is needed to drop it, not its bin (unlike Create,
	// which needs OnBin to declare the index in the first place).
	if err := s.session.Index(ctx, s.ds).
		Named(stringIndexName).
		Drop(ctx); err != nil {
		return fmt.Errorf("drop string index %s: %w", stringIndexName, err)
	}

	fmt.Printf("string index %s: created, queried both ways, dropped (can't verify matches — see GAP comment)\n", stringIndexName)
	return nil
}
