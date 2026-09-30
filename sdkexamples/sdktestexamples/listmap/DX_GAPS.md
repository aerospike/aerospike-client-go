# DX gaps found while building this example

Functional (Java-vs-Go capability) gaps are tracked centrally in
`sdk/FUNCTIONAL_GAPS.md` — findings #15, #16, #19, #21, and #22 all bite
in this package; inline `GAP` comments point to the exact finding at
each call site. This file covers DX-only observations and the two
source tests intentionally excluded.

## Two of the 13 source tests are not demonstrated at all

- `geoJsonReadBackAsSameType`, and the GeoJSON element of `listComplex`:
  GeoJSON has no PRD-defined representation anywhere in `sdk/`
  (`sdk/FUNCTIONAL_GAPS.md` finding #21). There is no substitute value
  that demonstrates the same capability — including one would mean
  either inventing `sdk/` surface (against the standing rule) or writing
  something that isn't actually GeoJSON, which would misrepresent what's
  being demonstrated. Left out entirely rather than faked.
- `keyOrderedMapNonScalarKeyCausesParameterError`: a server-side
  validation test (a non-comparable map key should cause a server
  `PARAMETER_ERROR`), not a question of `sdk/` API shape — it doesn't
  exercise any capability `sdk/` does or doesn't expose, it exercises
  server behavior given already-expressible input. Out of scope for an
  example whose purpose is surfacing API-shape gaps.

## No way to verify anything written actually round-trips correctly

Every `Demonstrate*` function in this package can confirm a write and a
subsequent read both completed without error, but none can confirm the
list/map contents, ordering, or types came back as written — `Record`
(`sdk/session.go`) has zero bin-accessor methods (finding #16). The
source Java tests are almost entirely built around asserting exactly
this (`assertEquals(receivedList.get(0), "a")`, `getType()`, etc.) — the
Go versions here demonstrate only the write/read call shapes, not the
round-trip correctness the Java tests actually check.

## Two Java tests per capability, one Go function each, mostly identical bodies

`aerospikeListBinsValues`/`aerospikeListOps` and
`aerospikeMapBinsValues`/`aerospikeMapOps` differ from each other only in
which write entry point is used (`SetBinsTo` vs `Set`) — kept as four
separate functions (not collapsed into two) to preserve 1:1 traceability
to the four separate Java tests, consistent with how `sdktestexamples/udf`
was restructured earlier. A reader skimming just the function bodies
without the doc comments could mistake this for accidental duplication
rather than a deliberate parity choice.
