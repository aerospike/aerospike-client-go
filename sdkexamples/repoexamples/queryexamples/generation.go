// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: optimistic-concurrency control via IfGeneration(gen Generation)
// (10.5, D-9) is real, PRD-defined API — but there is no PRD-defined way
// to obtain a Generation value in the first place. Record (sdk/
// session.go) is completely opaque: no fields, no methods. The PRD's own
// Look example for IfGeneration calls rec.Gen() — implying Record should
// expose one, as a method — but Record's actual shape is never specified
// anywhere in the catalog, and guessing at it isn't something to do
// unprompted (already tried once this session: adding Generation/
// Expiration fields directly, reverted after being flagged as an
// unauthorized invention beyond what the PRD defines).
//
// Without a way to read a record's current generation, there is no
// PRD-grounded way to demonstrate "read a record, then conditionally
// write against that exact generation, then show a stale generation gets
// rejected" — the entire scenario this concept exists for. A demo built
// around a fabricated or arbitrary Generation value wouldn't actually
// show correct-vs-stale behavior; it would just be a compiling call that
// misrepresents what it's showing. Nothing here does that.
package queryexamples
