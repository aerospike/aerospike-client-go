package mapexp

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateSortedMapEquality mirrors the source Java test
// sortedMapEquality(): write a map (Java's source uses a TreeMap; see
// GAP below), then run an AEL-string filter query checking the whole
// map's equality against a literal.
//
// GAP: Java writes a TreeMap, which java.util.SortedMap makes the SDK
// automatically pack as a KEY_ORDERED map, and later asserts the
// read-back map's type is LINKED (AerospikeMap.Type.LINKED). Go has no
// way to request key-ordered storage (sdk/FUNCTIONAL_GAPS.md finding
// #15 — the only sdk/ method that could have, MapSetPolicy, has since
// been removed entirely, finding #25) or an AerospikeMap-equivalent
// typed wrapper to infer it from the input shape either (finding #22).
// This writes a plain map[string]any and can neither request nor verify
// ordering.
//
// GAP: the source test scopes this query to one key
// (session.query(key)). sdk/'s QueryBuilder only filters a whole
// DataSet (Session.Query(ctx, ds *DataSet)) — no single/multi-key-scoped
// filtered read exists (same finding #23 addendum already hit in
// sdktestexamples/listexp). Scoped to the whole dataset here instead.
//
// GAP: can't verify the query actually matched or what came back —
// Record (sdk/session.go) has no bin-accessor methods at all
// (finding #16), so there's no way to read the map back regardless of
// query outcome.
func (s *Service) DemonstrateSortedMapEquality(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"key1": "e",
		"key2": "d",
		"key3": "c",
		"key4": "b",
		"key5": "a",
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(binName, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write record %s: %w", id, err)
	}

	ael := fmt.Sprintf("$.%s.get(type: MAP) == {'key1': 'e', 'key2': 'd', 'key3': 'c', 'key4': 'b', 'key5': 'a'}", binName)

	if _, err := s.session.Query(ctx, s.ds).
		Bins(binName).
		WhereAEL(ael).
		Execute(); err != nil {
		return fmt.Errorf("query record %s with AEL map-equality filter: %w", id, err)
	}

	fmt.Printf("record %s: AEL map-equality filter query built (can't verify contents or ordering — see GAP comment)\n", id)
	return nil
}
