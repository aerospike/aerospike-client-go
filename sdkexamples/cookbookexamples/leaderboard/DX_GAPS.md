# DX gaps found while building this example

Individual gaps are documented inline (`grep -rn "GAP" *.go`) at the call
site where they bite. This file is a short index of what's found here.

## GetScoresAroundPlayer removed — OnMapKeyRelativeIndexRange was never actually PRD-grounded

Originally built around `OnMapKeyRelativeIndexRange`
(`sdk/writesegmentbuilder.go`), described here as "real and matches the
source Java example's AEL relative-range selector exactly in intent." At
the time, its terminal (`GetKeys()`) still ending at `ExecuteOne()
(WriteResult, error)` — no value field — was flagged as the limiting
gap: the call could be made and confirmed accepted, but the actual keys
it read could never come back.

That framing understated the real problem. `OnMapKeyRelativeIndexRange`
itself was never PRD-grounded to begin with — it was only ever gestured
at by the same "range/list/relative forms... OnMapKeyRange,
OnListIndexRange, …" ellipsis this file's own second finding below
already correctly ruled insufficient for `OnMapIndexRange`, just never
applied consistently to this method too (`sdk/FUNCTIONAL_GAPS.md`
finding #27, prompted by a user question that triggered a full-`sdk/`
re-audit). The method has been removed entirely, and
`GetScoresAroundPlayer` along with it — see its doc comment in
`scoreboard.go` for the full reasoning, including why no substitute
(`OnMapKeyRange` with computed bounds, say) actually works for this
map's score-derived keys.

## OnMapIndexRange doesn't exist — CONFIRMED, promoted to sdk/FUNCTIONAL_GAPS.md

The source Java example spills over into neighboring score buckets when
the requested range overflows the current one, via
`onMapIndexRange(index, count)` — an index-based range read, distinct
from the key-relative range above. Checked `sdk/writesegmentbuilder.go`
directly: no `OnMapIndexRange` exists under any name. The PRD's own
§10.15 text only gestures at "range/list/relative forms... …" via an
ellipsis, never naming this one specifically — not enough to build
against. Moot now alongside the removal above — there's no
single-bucket case left to spill over from either.

See `sdk/FUNCTIONAL_GAPS.md` findings #17 and #27.
