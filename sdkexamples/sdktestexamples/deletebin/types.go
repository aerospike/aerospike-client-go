// Package deletebin demonstrates RemoveBin — dropping a single bin from a
// record while leaving the rest of the record intact — matching the real
// SDK's own DeleteBinTest.java (active, 1 test): write two bins, remove
// one of them, then read the record back and confirm the removed bin is
// gone while the other survives.
//
// Source: examples/ and the use-case-cookbook never touch bin removal at
// all — confirmed by direct grep across both, same as every other package
// under sdktestexamples/. The only real usage found anywhere is this
// active test. Real Java shape: `.bin(binName1).remove()` on the
// per-bin builder — Go's `RemoveBin(name)` collapses that two-step into
// one call, the same flattening already used for GetHeader
// (query(key).withNoBins() -> GetHeader).
package deletebin

const (
	binName1 = "bin1"
	binName2 = "bin2"
)
