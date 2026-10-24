// Package failonfilteredout demonstrates the write-side FailOnFilteredOut
// — turning a filter-expression write that doesn't match into an error,
// instead of the default silent no-op — matching the real SDK's own
// FilterExpTest.java (active), specifically the write half of
// putExcept(): a filtered upsert that matches applies normally, and one
// that doesn't match raises FILTERED_OUT because FailOnFilteredOut is
// set.
//
// Source: examples/ and the use-case-cookbook never touch
// FailOnFilteredOut at all — confirmed by direct grep across both, same
// as every other package under sdktestexamples/. The only real usage
// found anywhere is this active test.
//
// GAP: the same test file's getExcept() exercises FailOnFilteredOut on
// the *query* side too (session.query(key).where(...).failOnFilteredOut()
// .execute()) — real in Java, but sdk/query.go's QueryBuilder has no
// FailOnFilteredOut method at all, only WriteSegmentBuilder does.
// Reinforces finding #23 (already documented: FailOnFilteredOut is real
// on the write side but ironically absent on the query side) — not
// something this package can demonstrate, since there's no Go method to
// call.
package failonfilteredout

const binA = "A"
