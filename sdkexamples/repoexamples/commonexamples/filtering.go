// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: CommonExample.java has a "Test filtering out" section that is
// entirely read-side: session.query(set.ids(2)).where(...) — querying one
// specific, already-known key with a filter, four ways:
//
//  1. Filter matches (name == "Bob", the record's actual name) → returned
//     normally.
//  2. Filter doesn't match (name == "Fred") → default: silently absent
//     from the result (getFirst() comes back empty).
//  3. Same mismatch + includeMissingKeys() → the record reappears in the
//     result, flagged as filtered-out rather than a genuine match.
//  4. Same mismatch + failOnFilteredOut() → throws.
//
// This has no reachable Go equivalent. Checked Session.Query's actual
// signature (sdk/session.go): func (s *Session) Query(ctx
// context.Context, ds *DataSet) *QueryBuilder — a dataset only, never
// specific keys, confirmed by the PRD's own §10.7 annotation on this row:
// "Change — dataset only, not keys". BatchGet takes specific keys but has
// no filter parameter at all. So there is no Go call that combines "these
// specific keys" with "and filter them, with control over whether a
// non-matching-but-present key stays visible" — the combination Java's
// four-scenario demo is built entirely around.
//
// QueryBuilder.IncludeMissingKeys() and WriteSegmentBuilder.
// FailOnFilteredOut()/IncludeMissingKeys() do exist elsewhere in sdk/, but
// on different, single-key or whole-dataset shapes that don't reproduce
// this specific-keys-plus-filter scenario — using them to build something
// else and calling it a stand-in for this would misrepresent what Java
// actually demonstrates, so nothing here does that.
package commonexamples
