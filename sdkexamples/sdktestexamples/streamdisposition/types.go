// Package streamdisposition demonstrates StreamOnError — the lazy,
// error-disposition-aware stream terminal, on both the write and query
// builders — matching the real SDK's own AddTest.java (active, 8+
// tests), specifically addAsync().
//
// Source: examples/ and the use-case-cookbook never touch StreamOnError
// at all — confirmed by direct grep across both, same as every other
// package under sdktestexamples/. The only real coverage found anywhere
// is this active test.
//
// Real Java capability, confirmed directly in main source
// (query/QueryBuilder.java): Java's synchronous execute() has
// execute()/execute(ErrorStrategy)/execute(ErrorHandler) siblings, and a
// *separate* executeAsync()/executeAsync(ErrorStrategy)/
// executeAsync(ErrorHandler) family — "executes the query asynchronously
// ... populated as results arrive from the server" (real javadoc text).
// sdk/PRD.md's own "Lazy | Stream() / StreamOnError" framing matches
// this real async/lazy distinction, not a fictional one.
// ErrorStrategy.IN_STREAM (used throughout AddTest.java) maps directly
// onto sdk/dispositions.go's already-real InStream() constructor.
//
// GAP: the source test scopes its query to one key (session.query(key)).
// sdk/'s QueryBuilder only filters a whole DataSet — no single-key-scoped
// query exists (same finding #23 addendum already hit in
// sdktestexamples/listexp, mapexp, partition, and pointreads). Scoped to
// the whole dataset here instead.
package streamdisposition

const binName = "addbin"
