# DX gaps found while building this example

Functional gaps are tracked centrally in `sdk/FUNCTIONAL_GAPS.md` —
findings #15, #16, #22, #23, #24, and #25 all bite in this package;
inline `GAP` comments point to the exact finding at each call site. This
file covers the one excluded test.

## 1 of the source file's 2 tests is not demonstrated at all

`invertedMapExp` depends entirely on `.bin(binName).selectFrom(readExp)`
— projecting a computed expression's result into a read-time output
bin. The source test itself tries an AEL *string* form
(`"$.m.{=2}.get(return: ORDERED_MAP)"`) with the object-based form
(`Exp.build(MapExp.removeByValue(...))`) commented out immediately
above it — suggesting even the Java SDK's own authors found the
string form more reliable here, or the object form untested/unfinished.
Either way, neither `selectFrom` overload exists anywhere in `sdk/`
(finding #24, confirmed there to cover both spellings). There's no
substitute: projecting the *computed* inverted-filter result is the
entire point of this test, not an incidental detail. Left out entirely,
same treatment as the three excluded tests in `sdktestexamples/listexp`.

## What *is* demonstrated

`sortedMapEquality`'s AEL-string filter query (`WhereAEL`) is fully
buildable and PRD-grounded — the first file in this review where a
Java test's `where(String)` maps directly onto a real, named `sdk/`
method rather than requiring a reach into the classic client's separate
expression-builder API (contrast with `sdktestexamples/listexp`,
finding #26). What's missing is unrelated to expression-building at
all: requesting/verifying map ordering (findings #15, #22, #25) and
reading any value back at all (finding #16).
