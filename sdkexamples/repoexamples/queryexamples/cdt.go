package queryexamples

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateListMapOperations exercises the top-level list and map CDT
// mutation vocabulary — the part of the PRD's CDT surface (10.15) that's
// actually defined: OnBin(name) then a list/map terminal, single record,
// single level.
//
// GAP: three things the source Java example demonstrates in this same
// scenario have no PRD-backed equivalent, so they're left out entirely
// rather than approximated with an invented convention:
//
//  1. Reading a CDT sub-element's size/value/range via the query path
//     (Java: session.query(id).bin("scores").listSize().execute()). Our
//     QueryBuilder.OnBin(name) *QueryBinBuilder (10.7) has no CDT terminal
//     vocabulary at all — just a placeholder SelectAs method — so there's
//     no query-side way to do this.
//  2. Navigating into a nested map key that itself holds a list, then
//     running a list op there (Java:
//     bin("nested").onMapKey("team1").onMapKey("members").listSize()).
//     CDTNavBuilder (returned by OnMapKey/OnListIndex/etc., 10.15) has no
//     list methods — only WriteBinBuilder does, and OnBin's return value
//     is never reachable again once you've navigated past it.
//  3. Creating a new, explicitly-ordered list at a nested position (Java:
//     listCreate(ListOrder.ORDERED)) — no equivalent method exists
//     anywhere in the PRD's CDT vocabulary.
//
// All three would require inventing surface the PRD doesn't define, which
// is exactly what was asked not to happen here.
func (s *Service) DemonstrateListMapOperations(ctx context.Context, id int64) error {
	key := sdk.Key(s.customerDS.DataSet(), id)

	seedStream, err := s.session.Upsert(ctx, key).
		Set(customerNameBin, "CDT-Test").
		OnBin(cdtScoresBin).SetTo([]any{95, 82, 73, 88, 91}).
		OnBin(cdtTagsBin).SetTo([]any{"java", "python", "rust"}).
		OnBin(cdtInventoryBin).SetTo(map[any]any{"apples": 10, "bananas": 5, "cherries": 20}).
		Execute()
	if err := checkWrite(ctx, seedStream, err, "seed CDT test record"); err != nil {
		return err
	}

	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListAppendItems([]any{77, 65, 99}).
		ExecuteOne(); err != nil {
		return fmt.Errorf("append scores: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtTagsBin).ListInsert(1, "go").
		ExecuteOne(); err != nil {
		return fmt.Errorf("insert tag: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtTagsBin).ListSet(0, "kotlin").
		ExecuteOne(); err != nil {
		return fmt.Errorf("set tag: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListInsertItems(2, []any{100, 200}).
		ExecuteOne(); err != nil {
		return fmt.Errorf("insert scores: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListIncrement(0, 5).
		ExecuteOne(); err != nil {
		return fmt.Errorf("increment score: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListSort().
		ExecuteOne(); err != nil {
		return fmt.Errorf("sort scores: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListRemove(0).
		ExecuteOne(); err != nil {
		return fmt.Errorf("remove score: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListRemoveRange(4, 2).
		ExecuteOne(); err != nil {
		return fmt.Errorf("remove score range: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListPop(0).
		ExecuteOne(); err != nil {
		return fmt.Errorf("pop score: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtScoresBin).ListTrim(0, 3).
		ExecuteOne(); err != nil {
		return fmt.Errorf("trim scores: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtTagsBin).ListClear().
		ExecuteOne(); err != nil {
		return fmt.Errorf("clear tags: %w", err)
	}

	if _, err := s.session.Update(ctx, key).
		OnBin(cdtInventoryBin).MapUpsertItems(map[any]any{"dates": 15, "elderberries": 8}).
		ExecuteOne(); err != nil {
		return fmt.Errorf("upsert inventory items: %w", err)
	}
	if _, err := s.session.Update(ctx, key).
		OnBin(cdtInventoryBin).MapSetPolicy(as.MapOrder.KEY_ORDERED).
		ExecuteOne(); err != nil {
		return fmt.Errorf("set inventory map policy: %w", err)
	}

	return nil
}
