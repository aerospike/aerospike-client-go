# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`).

## ReplaceIfExists's not-found error code isn't confirmed in the PRD

The source test asserts a specific result code
(`ResultCode.KEY_NOT_FOUND_ERROR`) when `replaceIfExists` targets a
missing key. `sdk/PRD.md`'s error sentinels section (10.17) doesn't say
which of the eight sentinels `ReplaceIfExists` returns for this case.
`ErrNotFound` is used here as the reasonable default — it's the general
"doesn't exist" sentinel used everywhere else in this repo (`Get`,
`BatchGet` miss, `Drop` of a missing index) — but nothing confirms
`ReplaceIfExists` reuses it specifically.

## Single-key query scoping doesn't exist, again

The source test's read-back is `session.query(key).execute()` — a
single-key-scoped query. `sdk/`'s `QueryBuilder` only filters a whole
`DataSet` (same finding #23 addendum already hit in `listexp`, `mapexp`,
`partition`, `pointreads`, `streamdisposition`, `operate`, and
`deletebin`) — scoped to the whole `replaceifexists` dataset here
instead of the one key the source reads back.

## Can't verify the successful replace's actual effect

The source test's `replaceOnlyModifiesOpType()` reads the record back
afterward and asserts `bin1`/`bin2` are gone and `bin3` == `"value3"` —
confirming the replace genuinely replaced rather than merged. `Record`
(`sdk/session.go`) has no bin-accessor methods at all (finding #16), so
that verification isn't possible here — the read-back query itself is
still performed (`Query(ctx, ds).Execute()` + `One(ctx)`, matching the
source's own `execute()` + `next().recordOrThrow()`), it just can't
confirm what comes back.
