# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite; this file adds the cross-cutting notes. Both
`IncludeMissingKeys` findings below are promoted to
`sdk/FUNCTIONAL_GAPS.md` findings #31 and #32 — summarized here, full
writeup there.

## IncludeMissingKeys has no key-list-scoped home at all (finding #31)

The source Java test (`BatchTest.java`'s `batchExists()`) scopes
`includeMissingKeys()` to a specific list of keys via
`session.exists(keys)`. `sdk/query.go`'s `IncludeMissingKeys()` only
exists on `QueryBuilder`, which scopes to a whole `DataSet` — there is no
way to attach it to a specific key list. `BatchGet(ctx, keys, bins)` —
the real Go entry point for "read exactly these keys" — returns a
`*ReadStream` directly with no further builder to configure, so it
can't take this modifier either, even in principle. Demonstrated scoped
to the whole dataset instead (same substitution already used in
`sdktestexamples/listexp` and `partition` for the same key-list-vs-dataset
gap), with one key deliberately left unwritten so "missing" is real.

## The write-side IncludeMissingKeys is not demonstrated, and its purpose is unclear (finding #32)

`sdk/writesegmentbuilder.go`'s `WriteSegmentBuilder.IncludeMissingKeys()`
sits on the *single-key* write builder (entered via
`Upsert`/`Insert`/`Update`/`Replace(ctx, key)`) — but "missing keys"
(plural) doesn't obviously apply to a builder scoped to one key.
`sdk/PRD.md`'s own §10.5 table names the method with no supporting prose
beyond the one row, so there's nothing to check the intended semantics
against. Not built — only the query-side form (whole-dataset, matching
real Java usage) is demonstrated in this package.

## GetHeader has no real 1:1 Java method — it's a deliberate Go-side convenience

Java's real `Session` class has no `getHeader()`-equivalent method at
all (checked directly — zero matches in
`client/src/main/java/.../Session.java`). The source test named
`getHeader` achieves the same result via `session.query(key).withNoBins()`,
then reading `rec.generation`/`rec.expiration` directly. `sdk/PRD.md`'s
own §10.4 text explicitly frames `GetHeader` as the Go-side convenience
for this ("Point Get / BatchGet take []string (nil = all bins).
Header-only is GetHeader.") — a deliberate redesign per D-27, not a
literal port. Demonstrated both ways in this package:
`DemonstrateGetHeader` uses Go's own dedicated method; `DemonstrateQueryWithNoBins`
mirrors Java's actual mechanism.

## Can't verify anything written actually round-trips correctly

Same as every other package under `sdktestexamples/`: all three
`Demonstrate*` functions here can confirm a write/read call completed
without error, but `Record` (`sdk/session.go`) has no bin-accessor or
metadata-accessor methods at all (finding #16) — none can confirm a
generation, expiration, or missing-key status actually came back as
expected.
