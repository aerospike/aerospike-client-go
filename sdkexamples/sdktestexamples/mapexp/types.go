// Package mapexp demonstrates an AEL-string filter query against a map
// bin, matching the real SDK's own MapExpTest.java (active, 2 tests).
//
// Source: examples/ and the use-case-cookbook never touch map filter
// expressions at all — confirmed by direct grep across both, same as
// every other package under sdktestexamples/. The only real coverage
// found anywhere is this active test file.
//
// Only 1 of its 2 tests is demonstrated here. invertedMapExp depends
// entirely on .bin(binName).selectFrom(readExp) — projecting a computed
// expression as a read-time output bin — which the PRD never defines in
// any form, string or object (sdk/FUNCTIONAL_GAPS.md finding #24,
// confirmed there to generalize beyond ListExpTest.java's
// Expression-typed overload to this file's AEL-string one too). See
// DX_GAPS.md.
package mapexp

const binName = "m"
