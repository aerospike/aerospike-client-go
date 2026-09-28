# DX gaps found while building this example

Functional gaps are tracked centrally in `sdk/FUNCTIONAL_GAPS.md` —
finding #28 covers everything in this package (both the `Filter`/
`Partition` non-finding and the `OnPartitionRange` parameter-naming
mismatch); inline `GAP` comments point to the exact section.

## Partition(pf *as.PartitionFilter) is not demonstrated

Same root cause as `Filter(f *as.Filter)` (`sdktestexamples/listexp`,
finding #28): the real Java `PartitionFilter` is built internally by the
query command from the fluent builder's own start/end partition fields,
never constructed by a caller and handed to a query directly. `OnPartition`
and `OnPartitionRange` — the two methods on the same PRD row that *are*
demonstrated here — don't share this problem; they're the real,
fluent, caller-facing entry points.

## OnPartitionRange's stub parameter is named inconsistently with the PRD's own text

`sdk/query.go`'s `OnPartitionRange(begin, count int)` names its second
parameter `count`; `sdk/PRD.md`'s own §10.7 table spells the same row
`OnPartitionRange(begin, end)`. Checked Java's real `onPartitionRange`
directly — it's start-inclusive/end-exclusive, matching the PRD's row
text, not a count. `DemonstratePartitionRangeQuery` passes `begin=0`
specifically so the two possible readings of the stub's own parameter
can't produce a different query — this doesn't resolve the ambiguity,
it just avoids depending on which reading is correct.

## The source test's nested hasMoreChunks/hasNext loop isn't reproduced — for two stacked reasons

The Java test drains results with a genuinely nested traversal:
`while(hasMoreChunks()) { while(hasNext()) { next(); } }`. Two separate
reasons that shape doesn't appear here: `ReadStream` (`sdk/stream.go`)
has no `HasNext()` at all — only `NavigatableStream` does — so the exact
shape isn't constructible against the real type in the first place; and
separately, even setting that aside, how `HasMoreChunks` is meant to
interact with `Iter`/`Next` to demarcate a chunk boundary is undefined
anywhere in `sdk/PRD.md` (same open question as
`repoexamples/queryexamples/throttling.go`). `DemonstratePartitionQuery`
drains the stream with the same plain `Iter`+`Err` pattern as every
other stream in these examples and calls `HasMoreChunks` once
afterward, purely to show it exists.
