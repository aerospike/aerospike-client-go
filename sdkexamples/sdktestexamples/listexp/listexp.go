package listexp

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateModifyWithContext mirrors the source Java test
// modifyWithContext(): write nested lists into three bins, then build two
// equivalent filter expressions — one reading binB/binC, one using
// literal values instead — both computing "append X onto binA's nested
// sub-list (at CTX index 4), then check the result's size" and passing
// that as a query Where() predicate.
//
// GAP: can't verify the query actually matched (or what came back) —
// sdk/FUNCTIONAL_GAPS.md finding #23: QueryBuilder has no
// FailOnFilteredOut (the source test calls it after Where(), on both
// queries); and ReadResult (sdk/stream.go) has no ResultCode() to read
// back the filtered/matched status even if it did. Both queries here can
// only confirm the call itself completes without error.
//
// GAP: the source test calls .bin(binA).listAppendItems(listA) /
// .bin(binB).listAppendItems(listB) to seed these bins. WriteBinBuilder's
// ListAppendItems no longer exists (sdk/FUNCTIONAL_GAPS.md finding #25 —
// removed, it was never PRD-grounded). Substituted with plain Set(name,
// v), which is PRD-grounded (§10.5) and behaviorally identical here
// since these are fresh, just-truncated bins — append onto nothing is
// the same as assign — but Set can't express "append onto an existing
// list" in general the way ListAppendItems did.
func (s *Service) DemonstrateModifyWithContext(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	listSubA := []any{"e", "d", "c", "b", "a"}
	listA := []any{"a", "b", "c", "d", listSubA}
	listB := []any{"x", "y", "z"}

	if _, err := s.session.Upsert(ctx, key).
		Set(binA, listA).
		Set(binB, listB).
		Set(binC, "M").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed record %s: %w", id, err)
	}

	navCtx := as.CtxListIndex(4)
	policy := as.DefaultListPolicy()

	// Filter 1: append binC's value onto binA's sub-list (at nav index
	// 4), then append binB's items onto that, then check size == 9.
	modifiedA := as.ExpListAppend(policy, as.ExpStringBin(binC), as.ExpListBin(binA), navCtx)
	appended := as.ExpListAppendItems(policy, as.ExpListBin(binB), modifiedA, navCtx)
	where := as.ExpEq(as.ExpListSize(appended, navCtx), as.ExpIntVal(9))

	// GAP: the source test scopes this query to one key (session.query(keyA)).
	// sdk/'s QueryBuilder only filters a whole DataSet (Session.Query(ctx,
	// ds *DataSet)) — no single/multi-key-scoped filtered read exists
	// (same finding #23 addendum as DemonstrateListMapFilterInListBin
	// below). Scoped to the whole dataset here instead.
	if _, err := s.session.Query(ctx, s.ds).
		Bins(binA).
		Where(where).
		Execute(); err != nil {
		return fmt.Errorf("query with bin-based filter: %w", err)
	}

	// Filter 2: same shape, using literal values (Exp.val-equivalents)
	// instead of reading binB/binC — same expression capability.
	modifiedA2 := as.ExpListAppend(policy, as.ExpStringVal("M"), as.ExpListBin(binA), navCtx)
	appended2 := as.ExpListAppendItems(policy, as.ExpListValueVal("x", "y", "z"), modifiedA2, navCtx)
	where2 := as.ExpEq(as.ExpListSize(appended2, navCtx), as.ExpIntVal(9))

	if _, err := s.session.Query(ctx, s.ds).
		Bins(binA).
		Where(where2).
		Execute(); err != nil {
		return fmt.Errorf("query with value-based filter: %w", err)
	}

	fmt.Printf("record %s: nested list-append size filter built both ways (bin- and value-based) — see GAP comment\n", id)
	return nil
}

// DemonstrateListMapFilterInListBin mirrors
// listExpressionFilterMapElementInListBin(): write a list of maps into
// two records — one whose first map's "name" is "alice", one whose isn't
// — then build a filter expression reading "name" out of the map at list
// index 0 and comparing it to "alice".
//
// GAP: the source test scopes this query to exactly the two keys just
// written (session.query(List.of(keyMatch, keyFiltered))) and calls
// failOnFilteredOut() to see the non-matching key come back tagged
// FILTERED_OUT rather than silently absent. Neither is available:
// QueryBuilder has no key-scoped query and no FailOnFilteredOut
// (sdk/FUNCTIONAL_GAPS.md finding #23), and ReadResult has no
// ResultCode() to read one back even if it did. Scoped to the whole
// dataset here; can only confirm the filtered query call completes.
//
// GAP (behavioral, not just missing-verification): whole-dataset scoping
// isn't just weaker than Java's two-key scoping, it also changes what the
// query would actually touch on a real server — cmd/main.go runs
// DemonstrateModifyWithContext (which writes key "A", binA holding a
// plain string/nested-list value, not a list of maps) into this same
// dataset first. Java's exact key-list scoping guarantees only the two
// intended records are ever evaluated; this whole-dataset query would
// also evaluate this filter's MapExp.getByKey-on-list-index-0 expression
// against key "A"'s unrelated binA. Harmless today (Execute() is a stub),
// but worth knowing before ever pointing this at a live server.
func (s *Service) DemonstrateListMapFilterInListBin(ctx context.Context, matchID, noMatchID string) error {
	keyMatch := sdk.Key(s.ds, matchID)
	keyNoMatch := sdk.Key(s.ds, noMatchID)

	listOfMaps := []any{
		map[string]any{"name": "alice", "age": 30},
		map[string]any{"name": "bob", "age": 25},
	}
	if _, err := s.session.Upsert(ctx, keyMatch).
		Set(binA, listOfMaps).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write matching record %s: %w", matchID, err)
	}

	listNoMatch := []any{
		map[string]any{"name": "charlie", "age": 40},
		map[string]any{"name": "dave", "age": 35},
	}
	if _, err := s.session.Upsert(ctx, keyNoMatch).
		Set(binA, listNoMatch).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write non-matching record %s: %w", noMatchID, err)
	}

	// Filter: get "name" from the map at list index 0, check == "alice".
	firstMap := as.ExpListGetByIndex(as.ListReturnTypeValue, as.ExpTypeMAP, as.ExpIntVal(0), as.ExpListBin(binA))
	name := as.ExpMapGetByKey(as.MapReturnType.VALUE, as.ExpTypeSTRING, as.ExpStringVal("name"), firstMap)
	filter := as.ExpEq(name, as.ExpStringVal("alice"))

	if _, err := s.session.Query(ctx, s.ds).
		Bins(binA).
		Where(filter).
		Execute(); err != nil {
		return fmt.Errorf("filtered query: %w", err)
	}

	fmt.Printf("records %s/%s: list-of-maps filter expression built (get map key from list element) — see GAP comment\n", matchID, noMatchID)
	return nil
}
