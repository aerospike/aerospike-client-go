// Package partition demonstrates partition-scoped queries — targeting a
// single partition — matching the real SDK's own
// QueryWithPartitionPaginationTest.java (active, 1 test).
//
// Source: examples/ and the use-case-cookbook never touch partition-scoped
// queries at all — confirmed by direct grep across both, same as every
// other package under sdktestexamples/. The only real coverage found
// anywhere is the one active test above.
//
// GAP: sdk/PRD.md's §10.7 also names OnPartitionRange on the same row as
// OnPartition — not demonstrated here. Real, declared in main source
// (onPartition(id) is literally implemented as onPartitionRange(id,
// id+1) in QueryBuilder.java), but no dedicated Java test exercises the
// range form directly. Standing rule: code in this repo only ports a
// real Java test, not a bare method signature, however simple or
// well-grounded the signature looks — so this stays documented, not
// built, until a real test surfaces. (An earlier version of this
// package did build a range demo directly from the signature; removed
// for exactly this reason — see sdk/FUNCTIONAL_GAPS.md finding #28's
// update.)
//
// sdk/PRD.md's §10.7 also names a third method on the same row,
// Partition(pf *as.PartitionFilter) — not demonstrated here. Checked
// directly: the real Java PartitionFilter is constructed internally by
// QueryCommand from the fluent builder's own start/end partition fields,
// never built by a caller and handed to a query directly. Same
// internal-plumbing-surfaced-as-public-API shape already found for
// Filter(f *as.Filter) (sdk/FUNCTIONAL_GAPS.md finding #28) — nothing
// real to demonstrate for the raw PartitionFilter-object form.
package partition

const binName = "bin1"
