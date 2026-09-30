# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file covers the package-wide notes.

## Java's index create/drop is a flat call, sdk/'s is fluent — a real, deliberate redesign, not a gap

`session.createIndex(set, indexName, binName, IndexType.STRING,
IndexCollectionType.DEFAULT)` / `session.dropIndex(set, indexName)` in
Java vs. `Session.Index(ctx, ds).OnBin(name).Named(name).String().Create(ctx)`
/ `.Drop(ctx)` in `sdk/`. Confirmed this is intentional, not a gap: D-27
says fluent parity with Java is not a goal, and `sdk/index.go`'s
`IndexBuilder` is a real, fully PRD-named type (§10.13). Built against
`sdk/`'s own real chain throughout, not a literal translation of Java's
flat call shape.

`Drop` in particular only needs `.Named(indexName)`, not `.OnBin(...)` —
matching Java's `dropIndex(set, indexName)`, which needs only the name.
An earlier draft of this function included `.OnBin(...)` before `.Drop(ctx)`
unnecessarily (caught on a parity re-check); fixed.

## No PRD-grounded way to make index creation idempotent

The source test wraps `createIndex` in a try/catch that specifically
ignores `ResultCode.INDEX_ALREADY_EXISTS`, so repeat test runs don't
fail. Checked `sdk/errors.go` directly: none of its 8 sentinel errors
cover "index already exists," and `sdk/PRD.md` never discusses this
scenario either. There's no PRD-grounded way to distinguish that
specific failure from any other `Create` error, so the retry-and-ignore
pattern isn't reproduced here — `DemonstrateStringIndex` will error on a
second run against a real server, unlike the source test.

## Can't verify anything written actually round-trips correctly

Same as every other package under `sdktestexamples/`: `DemonstrateStringIndex`
can confirm the create/query/drop calls complete without error, but
`Record` (`sdk/session.go`) has no bin-accessor methods at all (finding
#16), so it can't confirm the filtered query actually matched the 1
expected record out of 5, the way the source Java test's assertions do.

## Built one file at a time

Currently covers `QueryStringTest.java` only. `QueryGeoTest.java`,
`QueryBlobTest.java`, and `QueryCollectionTest.java` are queued next, one
at a time, each adding its own `Demonstrate*Index` function and source
file to this same package.
