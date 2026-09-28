# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file is a short index of the two found here.

## No read-side entry point for CDT navigation reaches WriteResult's value-less terminal

`GetScoresAroundPlayer` needs the map keys immediately around a player's
own scoreboard entry. `OnMapKeyRelativeIndexRange` (`sdk/
writesegmentbuilder.go`) is real and matches the source Java example's
AEL relative-range selector exactly in intent — but it's a write-bin
navigation method, so its `GetKeys()` terminal still ends at
`ExecuteOne() (WriteResult, error)`, and `WriteResult` has no value
field. So the call can be made and can confirm it was accepted, but the
actual keys it read can never come back to the caller.

Same root limitation already documented in `ecommerce/products.go`'s
`RecordProductRatings` GAP and `cookbookexamples/onetomany`'s
`DeleteListing` — this is the third place it's been hit, not a new
finding.

## OnMapIndexRange doesn't exist — CONFIRMED, promoted to sdk/FUNCTIONAL_GAPS.md

The source Java example spills over into neighboring score buckets when
the requested range overflows the current one, via
`onMapIndexRange(index, count)` — an index-based range read, distinct
from the key-relative range above. Checked `sdk/writesegmentbuilder.go`
directly: no `OnMapIndexRange` exists under any name. The PRD's own
§10.15 text only gestures at "range/list/relative forms... …" via an
ellipsis, never naming this one specifically — not enough to build
against, so the overflow-across-buckets case isn't attempted here; only
the single-bucket case is built.

See `sdk/FUNCTIONAL_GAPS.md` finding #17.
