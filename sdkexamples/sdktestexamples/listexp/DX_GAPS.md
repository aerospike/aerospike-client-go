# DX gaps found while building this example

Functional gaps are tracked centrally in `sdk/FUNCTIONAL_GAPS.md` —
findings #16, #23, #24, and #26 all bite in this package; inline `GAP`
comments point to the exact finding at each call site. This file covers
the exclusions and substitutions made building it.

## 3 of the source file's 5 tests are not demonstrated at all

`listExpressionWithReturnTypeIndex`, `relativeRankListExpressionOrder`,
and `expReturnsList` all depend on `selectFrom(exp)` (projecting a
computed expression as a read-time output bin) or `upsertFrom(exp)`
(writing a bin from a computed expression) — neither exists anywhere in
`sdk/` (finding #24). There's no substitute: projecting/writing a
*computed* value is the entire point of each of these three tests, not
an incidental detail that can be worked around. Left out entirely.

## Both demonstrated tests drop `.failOnFilteredOut()` and key-scoping

The source test calls `.failOnFilteredOut()` on every query, and scopes
each query to one or two specific keys
(`session.query(keyA)`/`session.query(List.of(keyMatch, keyFiltered))`)
rather than a whole dataset. Neither is available in `sdk/`
(finding #23 and its addendum): `QueryBuilder` has no
`FailOnFilteredOut` and no way to scope a filtered query to specific
keys (`Query(ctx, ds)` filters a whole `DataSet`; `BatchGet` takes no
filter expression). Both demos here scope their filtered query to the
whole `listexp` dataset instead, and can only confirm the query call
itself completes — not whether it matched, or what came back.

This isn't just a weaker guarantee — it's a real behavioral difference.
`cmd/main.go` runs both demo functions against the same dataset, so
`DemonstrateListMapFilterInListBin`'s whole-dataset query would, on a
real server, also evaluate its map-in-list filter against key "A" (left
behind by `DemonstrateModifyWithContext`, whose binA holds a plain
string/nested-list value, not a list of maps) — something Java's precise
two-key scoping structurally can't do. Harmless while `Execute()` is a
stub; worth knowing before this is ever pointed at a live server.

## What *is* demonstrated — and what "PRD-grounded" actually covers here

The actual list/map expression-building capability — nested `CTX`
navigation, `ListExp`/`MapExp`-equivalent functions
(`ExpListAppend`/`ExpListAppendItems`/`ExpListSize`/`ExpListGetByIndex`/
`ExpMapGetByKey`), and passing the result into `Where(exp)` — is
demonstrated faithfully, including the two-variant (bin-based vs.
value-based) filter construction from `modifyWithContext`. Confirmed
byte-for-byte, not just by name: the new Java SDK's
`com.aerospike.client.sdk.exp.ListExp.append` and Go's classic client's
`ExpListAppend` pack the identical opcode (1) and argument order
(`value, policy.attributes, policy.flags, ctx`) — genuinely the same
wire operation, independently reimplemented.

But "PRD-grounded" here is narrower than it first reads: only the
`Where(exp *as.Expression)` *parameter type* is named in `sdk/PRD.md` —
every constructor function actually used to build a non-trivial
`*as.Expression` (`CtxListIndex`, `DefaultListPolicy`, `ExpEq`,
`ExpListAppend`, etc.) belongs to the classic client and is never
mentioned in the PRD at all (finding #26). Building this package
required already knowing that separate package's API, not just `sdk/`'s.
