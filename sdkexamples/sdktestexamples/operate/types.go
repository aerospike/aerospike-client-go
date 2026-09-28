// Package operate demonstrates whole-record operate verbs — DeleteRecord
// and TouchRecord — combined with reading a bin and DurableDelete in the
// same multi-op write, matching the real SDK's own OperateTest.java
// (active, 5 currently-passing tests; not a disabled/stale file, unlike
// the bitwise/HLL test files checked and deferred earlier this session).
//
// Source: examples/ and the use-case-cookbook never touch DeleteRecord,
// TouchRecord, or DurableDelete at all — confirmed by direct grep across
// both, same as every other package under sdktestexamples/. The only
// real usage found anywhere is this active test.
package operate

const binName1 = "optintbin1"
