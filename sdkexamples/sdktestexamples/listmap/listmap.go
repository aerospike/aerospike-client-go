package listmap

import (
	"context"
	"fmt"
	"sort"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateListBinsValues mirrors the source Java test
// aerospikeListBinsValues(): sort a list, write it via the multi-bin
// SetBinsTo entry point, then read it back.
//
// GAP: Java builds the list as an AerospikeList<String>, whose .sort()
// call sorts the elements AND records ListOrder.ORDERED for the server
// to store the list in ordered form. Go has no equivalent typed
// collection at all (sdk/FUNCTIONAL_GAPS.md finding #22) — this can only
// sort the plain []string client-side before writing; there is no
// PRD-defined way to also tell the server this list should be stored
// ordered.
func (s *Service) DemonstrateListBinsValues(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	list := []string{"e", "d", "c", "b", "a"}
	sort.Strings(list)

	if _, err := s.session.Upsert(ctx, key).
		SetBinsTo([]string{binListBinValues}, []any{list}).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write sorted list to record %s: %w", id, err)
	}

	// GAP: Record (sdk/session.go) has no bin-accessor methods at all
	// (sdk/FUNCTIONAL_GAPS.md finding #16) — this can confirm the read
	// succeeded, but not that the list came back as ["a","b","c","d","e"].
	if _, err := s.session.Get(ctx, key, []string{binListBinValues}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: sorted list written via SetBinsTo (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateListOps mirrors aerospikeListOps(): same sorted list as
// DemonstrateListBinsValues, written through the single-bin chain entry
// point (Set) instead of the multi-bin one (SetBinsTo) — both exist in
// sdk/writesegmentbuilder.go and are exercised here for parity with the
// two separate Java tests.
//
// GAP: same list-ordering limitation as DemonstrateListBinsValues.
func (s *Service) DemonstrateListOps(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	list := []string{"e", "d", "c", "b", "a"}
	sort.Strings(list)

	if _, err := s.session.Upsert(ctx, key).
		Set(binListBinValues, list).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write sorted list to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binListBinValues}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: sorted list written via Set (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateMapBinsValues mirrors aerospikeMapBinsValues(): write a map
// via the multi-bin SetBinsTo entry point.
//
// GAP: Java builds the map as an AerospikeMap<String,Integer> with
// Type.UNORDERED, whose getType() lets the test confirm the server-stored
// ordering came back UNORDERED. Go has no equivalent typed map wrapper
// (finding #22) — this can only write a plain map[string]any, with no way
// to express or verify its stored ordering.
func (s *Service) DemonstrateMapBinsValues(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"joe":     90,
		"jim":     76,
		"charlie": 78,
	}

	if _, err := s.session.Upsert(ctx, key).
		SetBinsTo([]string{binMapBinValues}, []any{m}).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binMapBinValues}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: map written via SetBinsTo (can't verify contents or ordering — see GAP comment)\n", id)
	return nil
}

// DemonstrateMapOps mirrors aerospikeMapOps(): same map data as
// DemonstrateMapBinsValues, written through the single-bin chain entry
// point (Set) instead of SetBinsTo.
//
// GAP: same map-ordering limitation as DemonstrateMapBinsValues.
func (s *Service) DemonstrateMapOps(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"charlie": 78,
		"jim":     76,
		"joe":     90,
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(binMapBinValues, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binMapBinValues}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: map written via Set (can't verify contents or ordering — see GAP comment)\n", id)
	return nil
}

// DemonstrateListStrings mirrors listStrings(): a plain string list, no
// ordering/typed-collection involved — the least gap-affected test in
// this file, included for full 1:1 file coverage.
func (s *Service) DemonstrateListStrings(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	list := []string{"string1", "string2", "string3"}

	if _, err := s.session.Upsert(ctx, key).
		Set(binListStrings, list).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write string list to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binListStrings}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: string list written (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateListComplex mirrors listComplex(): a mixed-type list
// (string, int, blob). The source Java test also includes a fourth
// element, Value.getAsGeoJSON(geopoint) — omitted here entirely: GeoJSON
// has no PRD-defined representation in sdk/ at all
// (sdk/FUNCTIONAL_GAPS.md finding #21), so there is nothing to write in
// its place without inventing sdk/ surface, which is out of scope for an
// example (see DX_GAPS.md).
func (s *Service) DemonstrateListComplex(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	blob := []byte{3, 52, 125}
	list := []any{"string1", 2, blob}

	if _, err := s.session.Upsert(ctx, key).
		Set(binListComplex, list).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write mixed list to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binListComplex}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: mixed list written, minus its GeoJSON element (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateMapStrings mirrors mapStrings(): a plain string-to-string
// map, no ordering/typed-collection involved.
func (s *Service) DemonstrateMapStrings(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"key1": "string1",
		"key2": "loooooooooooooooooooooooooongerstring2",
		"key3": "string3",
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(binMapStrings, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write string map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binMapStrings}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: string map written (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateMapComplex mirrors mapComplex(): a mixed-type map (string,
// int, blob, nested list, bools).
func (s *Service) DemonstrateMapComplex(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	blob := []byte{3, 52, 125}
	nested := []any{100034, 12384955, 3, 512}
	m := map[string]any{
		"key1": "string1",
		"key2": 2,
		"key3": blob,
		"key4": nested,
		"key5": true,
		"key6": false,
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(binMapComplex, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write mixed map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binMapComplex}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: mixed map written (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateListMapCombined mirrors listMapCombined(): a list containing
// a nested list and a nested map, the nested map itself containing
// another nested list.
func (s *Service) DemonstrateListMapCombined(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	blob := []byte{3, 52, 125}
	inner := []any{"string2", 5}
	innerMap := map[string]any{
		"a":    1,
		"2":    "b",
		"3":    blob,
		"list": inner,
	}
	list := []any{"string1", 8, inner, innerMap}

	if _, err := s.session.Upsert(ctx, key).
		Set(binListMap, list).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write combined list/map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binListMap}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: combined list/map written (can't verify contents — see GAP comment)\n", id)
	return nil
}

// DemonstrateKeyOrderedMap mirrors keyOrderedMapWithVariousKeyTypes():
// Java passes a plain TreeMap, which java.util.SortedMap makes the SDK
// automatically pack as a KEY_ORDERED map — no explicit ListPolicy/
// MapPolicy call needed for this shape.
//
// GAP: sdk/writesegmentbuilder.go's WriteBinBuilder.MapSetPolicy(policy
// any) — the only sdk/ method that could have expressed KEY_ORDERED
// explicitly (finding #15) — has since been removed entirely: it was
// never PRD-grounded (finding #25). Go's plain map[string]any also has
// no ordering concept to infer from the way Java's SortedMap does either
// (finding #22). This writes a plain map and can't request or verify
// key-ordered storage at all, by any route.
func (s *Service) DemonstrateKeyOrderedMap(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"alpha": "value1",
		"beta":  42,
		"gamma": "value3",
	}

	if _, err := s.session.Upsert(ctx, key).
		Set(binKeyOrderedMap, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("write map to record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binKeyOrderedMap}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: map written, key-ordering unrequestable and unverifiable (see GAP comment)\n", id)
	return nil
}

// DemonstrateSortedMapReplace mirrors sortedMapReplace(): write a map
// using Session.Replace (Java's session.replace(key)) instead of Upsert,
// then read it back.
func (s *Service) DemonstrateSortedMapReplace(ctx context.Context, id string) error {
	key := sdk.Key(s.ds, id)

	m := map[string]any{
		"1": "s1",
		"2": "s2",
		"3": "s3",
	}

	if _, err := s.session.Replace(ctx, key).
		Set(binSortedMap, m).
		ExecuteOne(); err != nil {
		return fmt.Errorf("replace record %s: %w", id, err)
	}

	if _, err := s.session.Get(ctx, key, []string{binSortedMap}); err != nil {
		return fmt.Errorf("read back record %s: %w", id, err)
	}
	fmt.Printf("record %s: map written via Replace (can't verify contents — see GAP comment)\n", id)
	return nil
}
