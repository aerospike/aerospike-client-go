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
— the method on the same PRD row that *is* demonstrated here — doesn't
share this problem; it's the real, fluent, caller-facing entry point.

## OnPartitionRange is not demonstrated either — no real Java test, standing rule applied

`OnPartitionRange` is real (`onPartition(id)` is literally implemented as
`onPartitionRange(id, id+1)` in `QueryBuilder.java`), and its stub
parameter naming has its own documented mismatch against the PRD's own
text (`sdk/query.go` says `count`, `sdk/PRD.md`'s §10.7 table says `end`
— finding #28 has the full writeup). But no dedicated Java test
exercises the range form directly — only `onPartition`'s single-partition
form is test-covered. An earlier version of this package built a range
demo directly from the method signature and the PRD's own Look example
anyway; removed after the standing rule was clarified: code here only
ports a real Java *test*, never a bare signature, however well-grounded
it looks otherwise. Stays documented, not built.

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
