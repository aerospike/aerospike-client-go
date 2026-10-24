// This file documents, rather than demonstrates, list/map CDT mutation.
// There is no function here: there is nothing real left to call.
//
// GAP: this file previously demonstrated the 18 flat, un-navigated
// list/map operations on WriteBinBuilder — ListAppendItems, ListInsert,
// ListSet, ListInsertItems, ListIncrement, ListSort, ListRemove,
// ListRemoveRange, ListPop, ListTrim, ListClear, MapUpsertItems,
// MapSetPolicy, and five siblings never exercised here (ListSize,
// ListGet, ListGetRange, MapSize, MapClear) — every one of them. All 18
// have been removed from sdk/writesegmentbuilder.go: none was ever named
// anywhere in sdk/PRD.md. Caught when a user asked, while reviewing an
// unrelated package, "ListAppendItems is never mentioned in the PRD,
// where did you even get it?" — a question that turned into an
// exhaustive audit (sdk/FUNCTIONAL_GAPS.md finding #25). §10.15 (the
// PRD's own CDT catalog) positively enumerates what it keeps — CDT
// navigation (OnMapKey, OnListIndex, range/relative forms), terminals
// (GetValues, GetKeys, Count, Remove, RemoveAnd, GetAllOther*,
// GetAsOrderedMap, GetExists), and writes at a navigated position
// (SetTo/Insert/Update/Add) — and gives HLL/bitwise/string an explicit
// blanket "Keep the complete alpha set" clause. List/map operations that
// don't go through navigation get no such clause and aren't named
// individually either.
//
// The three gaps this file previously also documented alongside those
// operations — no query-side CDT read terminal (finding #2), no
// multi-level navigation at all (finding #19), no list-level ordering
// method or nested-position collection creation — are unaffected by this
// removal and remain documented at sdk/FUNCTIONAL_GAPS.md findings #2 and
// #19. What's gone is different: not "some operations are missing at a
// nested position," but "the entire un-navigated list/map mutation
// vocabulary this file was built around doesn't exist in sdk/ at all."
// A demo built by falling back to Set(name, wholeNewValue) for every one
// of these wouldn't actually show list/map *mutation* — it would just be
// the same whole-bin-overwrite already demonstrated more thoroughly in
// sdktestexamples/listmap. Nothing here does that.
package queryexamples
