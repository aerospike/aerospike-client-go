// Package listexp demonstrates building list/map filter expressions —
// nested CDT navigation (CTX) combined with ListExp/MapExp-equivalent
// expression functions, passed into Where(exp) — matching the real SDK's
// own ListExpTest.java (active, 5 tests).
//
// Source: examples/ and the use-case-cookbook never touch list/map
// filter expressions at all — confirmed by direct grep across both, same
// as every other package under sdktestexamples/. The only real coverage
// found anywhere is this active test file.
//
// Three facts shape what's demonstrated here, all recorded in
// sdk/FUNCTIONAL_GAPS.md:
//
//   - The PRD's own Where(exp *as.Expression) signature (§10.5, §10.7)
//     names *as.Expression as the parameter type, so passing a value of
//     that type is fully PRD-grounded — not a gap. But the PRD never
//     documents (or even names) any of the actual classic-client
//     constructor functions used below to build one — CtxListIndex,
//     DefaultListPolicy, ExpEq, ExpListAppend, ExpListAppendItems,
//     ExpListBin, ExpListGetByIndex, ExpListSize, ExpListValueVal,
//     ExpMapGetByKey, ExpStringBin, ExpStringVal, ExpTypeMAP,
//     ExpTypeSTRING, ListReturnTypeValue, MapReturnType. A caller has to
//     already know the classic client's separate, differently-named API
//     to build anything beyond a trivial *as.Expression (finding #26).
//   - Three of the five source tests are NOT demonstrated here at all:
//     they depend on upsertFrom(exp)/selectFrom(exp) (writing a bin from,
//     or projecting, a computed expression), which the PRD never defines
//     in either direction (finding #24). The other two omit
//     .failOnFilteredOut(), which exists on the write DSL but has no
//     query-side equivalent at all (finding #23) — see DX_GAPS.md.
package listexp

const (
	binA = "A"
	binB = "B"
	binC = "C"
)
