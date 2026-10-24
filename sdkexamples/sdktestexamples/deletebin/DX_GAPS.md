# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite.

## Single-key query scoping doesn't exist, again

The source test's read-back is `session.query(key).readingOnlyBins(...)`
— a single-key-scoped query. `sdk/`'s `QueryBuilder` only filters a whole
`DataSet` (same finding #23 addendum already hit in `listexp`, `mapexp`,
`partition`, `pointreads`, `streamdisposition`, and `operate`) — scoped
to the whole `deletebin` dataset here instead of the one key the source
reads back.

## Can't verify the actual point of the test

The source test exists specifically to assert that the removed bin reads
back as `null` while the untouched bin keeps its value
(`rec.getValue(binName1) == null`, `rec.getString(binName2) ==
"value2"`) — that comparison is the entire content of `deleteBin()`.
`Record` (`sdk/session.go`) has no bin-accessor methods at all (finding
#16), so none of that is checkable here. This can only confirm the
remove-bin write and the read-back query both complete without error —
the shape compiles and runs the same three-step sequence, but the one
thing the source test actually verifies is unverifiable in this stub.
