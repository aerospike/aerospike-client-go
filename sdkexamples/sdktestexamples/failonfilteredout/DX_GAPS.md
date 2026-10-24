# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`).

## FailOnFilteredOut's error sentinel isn't confirmed in the PRD

`ErrFilterExpression` is used here as the sentinel a filtered-out write
maps to — a natural fit by name, and the source test's own result code
is literally `ResultCode.FILTERED_OUT` — but `sdk/PRD.md`'s error
sentinels section (10.17) never confirms `FailOnFilteredOut` actually
reuses this sentinel specifically. Treated as the reasonable default,
same reasoning as the `ErrNotFound` GAP already documented in
`commonexamples/indexes.go` and `sdktestexamples/replaceifexists`. This
is also the first package in this repo to exercise `ErrFilterExpression`
at all — it was previously declared but unused anywhere, despite
`sdk/COVERAGE.md`'s Errors section claiming "all eight sentinel errors"
were already touched (they weren't — see the COVERAGE.md fix alongside
this package).

## The query-side counterpart has no Go method to call at all

The same source file's `getExcept()` test exercises `FailOnFilteredOut`
on the *query* side (`session.query(key).where(...).failOnFilteredOut()
.execute()`) — real in Java, but `sdk/query.go`'s `QueryBuilder` has no
`FailOnFilteredOut` method at all, only `WriteSegmentBuilder` does.
Reinforces the already-documented finding #23 (`FailOnFilteredOut` is
real on the write side but ironically absent on the query side) —
nothing to build here, no Go method exists to call.
