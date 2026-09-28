// Package partition demonstrates partition-scoped queries — targeting a
// single partition, and a partition range — matching the real SDK's own
// QueryWithPartitionPaginationTest.java (active, 1 test) and the sibling
// onPartitionRange method it doesn't separately test but is grounded in
// the same real, active QueryBuilder.java source.
//
// Source: examples/ and the use-case-cookbook never touch partition-scoped
// queries at all — confirmed by direct grep across both, same as every
// other package under sdktestexamples/. The only real coverage found
// anywhere is the one active test above, plus onPartitionRange's own
// main-source implementation (onPartition(id) is literally defined as
// onPartitionRange(id, id+1), start-inclusive/end-exclusive).
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
