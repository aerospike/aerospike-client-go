// Package listmap demonstrates writing and reading list/map bin values —
// plain collections, nested combinations, and a key-ordered map — matching
// the real SDK's own ListMapTest.java (active, 13 tests).
//
// Source: examples/ and the use-case-cookbook never touch list/map bin
// values as a dedicated topic — confirmed by direct grep across both, same
// as every other package under sdktestexamples/. The only real coverage
// found anywhere is this active test file.
//
// Two of its 13 tests are intentionally not demonstrated here, both
// documented in DX_GAPS.md: geoJsonReadBackAsSameType and the GeoJSON
// element of listComplex (sdk/FUNCTIONAL_GAPS.md finding #21 — GeoJSON has
// no PRD-defined representation at all), and
// keyOrderedMapNonScalarKeyCausesParameterError (a server-side validation
// test, not an sdk/ API-shape question).
package listmap

// Bin names match the source Java test's literals exactly, one const per
// distinct bin, for direct traceability back to ListMapTest.java.
const (
	binListBinValues = "listbin"  // aerospikeListBinsValues, aerospikeListOps
	binMapBinValues  = "listbin"  // aerospikeMapBinsValues, aerospikeMapOps (Java reuses "listbin" for maps too)
	binListStrings   = "listbin1" // listStrings
	binListComplex   = "listbin2" // listComplex
	binMapStrings    = "mapbin1"  // mapStrings
	binMapComplex    = "mapbin2"  // mapComplex
	binListMap       = "listmapbin"
	binKeyOrderedMap = "komap"
	binSortedMap     = "mapbin" // sortedMapReplace
)
