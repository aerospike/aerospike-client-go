# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file adds the cross-cutting notes.

## The real test's distinct per-position result codes can't be reproduced

`BatchTest.batchWriteComplex()`'s whole reason for stepping through
`next()` manually — rather than looping — is that each of its four batch
entries lands a *different* result code (`OK`, `INVALID_NAMESPACE`, `OK`,
`OK`), and the test is checking exactly that. `sdk/` is a fully opaque
stub (every `DataSet`/`Key`/stream call returns `nil` unconditionally),
so there's no way to make one entry behave differently from another —
this demo simplifies to two `Upsert`s and a `Delete` on three ordinary
keys. The shape being demonstrated (pull results one at a time, in
request order, via the raw `Next(ctx)` terminal) survives intact; the
specific result-code values the real test asserts do not.

## No `HasNext()` on `ReadStream`/`WriteStream`, and `Next()`'s exhaustion signal is undefined

The source test confirms it drained everything with
`assertFalse(rs.hasNext())`. `sdk/`'s `ReadStream`/`WriteStream` have no
`HasNext()` at all — only `NavigatableStream` does (same gap already
documented in `sdktestexamples/partition`'s `DX_GAPS.md`) — and
`sdk/PRD.md`'s §10.14 never says what calling `Next()` again past the
last real result returns (no documented terminal error or nil-sentinel;
same open question already on record for `HasMoreChunks`/`Next`
interaction in that section's own NOTE). `DemonstrateBatchWriteResults`
calls `Next()` a 4th time to show the shape, but asserts nothing about
what comes back.

## The other two real "manual next()" call sites are Go-idiom non-gaps, not additional grounding

Java's own test suite has exactly two other active files that call
`next()` outside a `hasNext()` loop: `ReplaceTest.java` and
`TouchTest.java`. Both do it because Java expresses "read/touch one key
and get the outcome" as `session.query(key).execute()` /
`session.touch(key).execute()` — a stream, walked once. `sdk/`'s
`Session.Get`/`Session.Touch` return the value directly and never
produce a stream at all for these single-key cases — a real,
already-idiomatic Go difference (confirmed real, not invented — see
`sdk/session.go`), so porting either test as-is would mean forcing a
stream into a call shape Go deliberately doesn't have, just to touch
`Next`. Not used as grounding for this package; `BatchTest.java`'s
`batchWriteComplex()` is the one real scenario where Go's own
`BatchWrite`/`WriteOp` genuinely produce a multi-result stream worth
manually stepping through.
