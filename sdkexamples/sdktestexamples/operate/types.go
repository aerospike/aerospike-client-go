// Package operate demonstrates whole-record operate verbs — DeleteRecord
// and TouchRecord — combined with reading a bin and DurableDelete in the
// same multi-op write, matching the real SDK's own OperateTest.java
// (active, 5 currently-passing tests; not a disabled/stale file, unlike
// the bitwise/HLL test files checked and deferred earlier this session).
// Also demonstrates the raw stream terminal One(ctx) — the same test's
// closing read-back line, `session.query(key).execute().getFirstRecord()`.
//
// Source: examples/ and the use-case-cookbook never touch DeleteRecord,
// TouchRecord, or DurableDelete at all — confirmed by direct grep across
// both, same as every other package under sdktestexamples/. The only
// real usage found anywhere is this active test. `getFirstRecord()`
// itself (One(ctx)'s real Java counterpart) is pervasive elsewhere
// though — 30+ hits across the cookbook, examples dir, and this same
// active test suite (OperateTest.java, PutGetTest.java, UdfTest.java,
// DurableDeleteTests.java) — always as the terminal on a
// `session.query(key)`-as-stream read. Checked whether that's a
// distinct capability from Go's own single-key Get()/ExecuteOne(): it
// is, when paired with query-builder-only features a plain Get can't
// express (a `.where()` filter, or a CDT/ael projection like
// `.bin(x).selectFrom(ael)`, seen throughout the cookbook) — not when
// it's a bare unfiltered single-key read, which Get() already covers
// more directly. This package's own DemonstrateDeleteRecord already
// made that exact call for its multi-op writes, using ExecuteOne()
// rather than Execute()+One() — consistent with the same finding.
package operate

const binName1 = "optintbin1"
