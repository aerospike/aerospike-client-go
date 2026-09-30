// Package pointreads demonstrates three read modifiers untouched
// anywhere else in these examples (sdk/COVERAGE.md): Session.GetHeader,
// QueryBuilder.WithNoBins, and QueryBuilder.IncludeMissingKeys — drawn
// from three separate, real, active files in the SDK's own test suite
// rather than one, since each is a small, independent capability rather
// than parts of one coherent test:
//
//   - GetHeader: PutGetTest.java's getHeader() test. Notably, Java's
//     real Session has no dedicated getHeader method at all — that test
//     achieves the same "read metadata, not bin values" result via
//     session.query(key).withNoBins(), then reads rec.generation/
//     rec.expiration directly. sdk/PRD.md's own §10.4 table explicitly
//     offers GetHeader as a Go-side convenience for the same capability
//     ("Point Get / BatchGet take []string (nil = all bins). Header-only
//     is GetHeader.") — a deliberate redesign, not a literal port,
//     consistent with D-27 (fluent parity is not a goal). Demonstrated
//     directly via Go's own dedicated method.
//   - WithNoBins: the same underlying capability, demonstrated the way
//     Java's real test actually builds it (query + withNoBins) —
//     TouchTest.java's touch()/touchOperate() tests, active.
//   - IncludeMissingKeys: BatchTest.java's batchExists() test, active —
//     session.exists(keys).includeMissingKeys(), reporting which of a
//     specific key list don't exist rather than silently omitting them.
//
// GAP: Java's IncludeMissingKeys usage is scoped to a specific list of
// keys (a batch exists check). sdk/'s IncludeMissingKeys only exists on
// QueryBuilder (whole-DataSet scope) and WriteSegmentBuilder (single-key
// write scope, and it's unclear from the PRD's own text — no supporting
// prose beyond the one table row — what "missing keys" even means for a
// single-key write). Neither matches Java's specific-key-list scenario;
// BatchGet(ctx, keys, bins) — Go's real multi-key-list read entry point
// — returns a *ReadStream directly, with no builder to attach
// IncludeMissingKeys to at all. Demonstrated here scoped to the whole
// dataset instead, same substitution already used in
// sdktestexamples/listexp and partition for the same
// key-list-vs-dataset gap.
package pointreads

const binName = "mybin"
