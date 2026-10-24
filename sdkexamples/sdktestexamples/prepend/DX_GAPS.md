# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite.

## Single-key query scoping doesn't exist, again

The source test's read-back is `session.query(key).readingOnlyBins(...)`
— a single-key-scoped query. `sdk/`'s `QueryBuilder` only filters a whole
`DataSet` (same finding #23 addendum already hit in `listexp`, `mapexp`,
`partition`, `pointreads`, `streamdisposition`, `operate`, and
`deletebin`) — scoped to the whole `prepend` dataset here instead of the
one key the source reads back.

## Can't verify either of the source test's two assertions

The source test's whole point is checking that repeated prepends
accumulate in the right order (`"World!"` after two prepends, not
`"!World"`) and that a combined prepend+get returns the post-prepend
value (`"Hello World!"` via `rec.operationResult(1).getString()`).
Neither is checkable here: `Record` (`sdk/session.go`) has no
bin-accessor methods at all (finding #16), and `WriteResult`
(`sdk/stream.go`) has no value field either — the same limitation
already documented for `sdktestexamples/operate` and `sdktestexamples/udf`.
This can only confirm each write and the read-back query complete
without error, not that prepend order or the combined op's result are
actually correct.
