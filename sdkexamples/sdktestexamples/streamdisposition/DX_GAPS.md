# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file adds the cross-cutting notes.

## Query is scoped to the whole dataset, not the specific key Java uses

The source test's query step is `session.query(key)` — a single-key
query. `sdk/`'s `QueryBuilder` only filters a whole `DataSet` — no
single/multi-key-scoped filtered read exists (same finding #23 addendum
already hit in `listexp`, `mapexp`, `partition`, and `pointreads`).
Scoped to the whole `streamdisposition` dataset here instead.

## Can't verify anything written actually round-trips correctly

Same as every other package under `sdktestexamples/`: every step here
can confirm the stream call completed without error, but `Record`
(`sdk/session.go`) has no bin-accessor methods at all (finding #16), so
none of them can confirm the incremented value, the queried value, or
the combined add+get result actually match what the source Java test's
assertions check (15 after two adds, 45 after the add+get).
