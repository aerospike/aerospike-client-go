# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. Both findings here are reinforcements of
already-documented gaps — no new central entries needed.

## WriteResult has no value field (fourth/fifth site)

Both `DemonstrateDeleteRecord` and `DemonstrateTouchRecord` embed a
`Get(binName)` inside a multi-op write, exactly like the source Java
test does — but can't read the fetched value back, since `ExecuteOne()`
returns `(WriteResult, error)` and `WriteResult` has no value field at
all. Same limitation as `ecommerce/products.go`'s `RecordProductRatings`,
`cookbookexamples/onetomany`'s `DeleteListing`, and
`cookbookexamples/leaderboard`'s `GetScoresAroundPlayer`.

## Entry verb (Upsert) doesn't describe what the chain actually does (delete)

`DemonstrateDeleteRecord`'s combined read+delete call starts with
`Upsert(ctx, key)` even though the chain ends in `DeleteRecord()` — every
`WriteSegmentBuilder` chain must start with a record-exists-policy verb
(`Upsert`/`Insert`/`Update`/`Replace`/`ReplaceIfExists`), even when the
actual embedded operation is a whole-record delete, so the entry verb is
effectively vestigial for this shape. Not a reaction unique to reading Go:
the source Java test wraps the identical call in a specially-named
helper (`upsertForScDurableRecordDelete`) specifically to explain why an
"upsert" is really a delete — real, first-party evidence the raw shape
is genuinely confusing, in both languages.

## Set after DeleteRecord in the same chain looks like it resurrects the record

The delete-and-recreate call goes further than the plain entry-verb
mismatch above: `DeleteRecord()` is immediately followed by
`Set(binName2, ...)` in the same chain, which reads like writing to a
record that was just deleted in the line above — it isn't, the whole
chain is one atomic operate (delete, then write fresh bins, one round
trip), but nothing in the call shape signals that. A reader has to
already know the semantics to trust `Set` after `DeleteRecord` isn't a
bug. Same root cause: the builder has no vocabulary for "atomically
replace this record's entire contents" — that intent has to be spelled
out as delete-then-write and inferred from op order alone.

## A Get() with no observable effect looks like dead code

`DemonstrateTouchRecord`'s combined call includes `Get(binName1)`, but
given the WriteResult limitation above, that call's fetched value can
never reach the caller — it compiles, matches the source Java shape,
and genuinely does nothing observable in this program. Without already
knowing that backstory, it reads like a line someone forgot to remove,
not a deliberate choice to mirror the source's intent (demonstrating
that a read can be combined with a whole-record touch in one round
trip). Smaller, related ambiguity in the same chain: the PRD never says
what `TouchRecord()` does once an explicit `ExpireAfter` is also given
in the same call — left as-is rather than guessed at.

## Single-key query scoping doesn't exist, again (DemonstrateReadBack)

The source's closing read-back is `session.query(key).execute()` — a
single-key-scoped query. `sdk/`'s `QueryBuilder` only filters a whole
`DataSet` (same finding #23 addendum already hit in `listexp`, `mapexp`,
`partition`, `pointreads`, and `streamdisposition`) — scoped to the
whole `operate` dataset here instead of the one key the source reads.

## getFirstRecord()/One() vs. Get()/ExecuteOne() — a real, checked distinction, not an assumption

`One(ctx)` is genuinely not redundant with Go's own single-key
shortcuts, but only for a specific reason: `Get(ctx, key, bins)` and
`ExecuteOne()` take a plain bin-name list, nothing else. Every real
`getFirstRecord()` call site found elsewhere in the source tree that
uses a *bare* single-key read with no filter or projection also has a
direct Go equivalent already in use (this package's own
`DemonstrateDeleteRecord`/`DemonstrateTouchRecord` use `ExecuteOne()`
for exactly that reason). `One()` earns its keep specifically when a
`.where()` filter or a CDT/ael projection (`.bin(x).selectFrom(ael)`,
`.bin(x).listSize()`, etc. — all through the query-builder path only)
is combined with "and there's exactly one result, give it to me
directly." `DemonstrateReadBack` here is the plain, unfiltered case
(matching the source line's own simplicity) — real and correctly using
`One()`, but not itself an example of the filter/projection distinction;
noted here rather than overclaimed.

## Record is opaque — can't verify TTL was refreshed

`DemonstrateTouchRecord` can't confirm the touched record's TTL actually
changed — `Record` (`sdk/session.go`) has no expiration field or method
(`sdk/FUNCTIONAL_GAPS.md` finding #16). What *is* independently
verifiable and genuinely checked here: `DemonstrateDeleteRecord` confirms
deletion (and later recreation) via `Session.Exists` — a real, working
check, not degraded by either gap above.
