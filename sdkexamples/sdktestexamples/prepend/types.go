// Package prepend demonstrates Prepend — the whole-bin-value string
// prepend op, the mirror of the already-touched Append — matching the
// real SDK's own AppendTest.java's prepend() (active): two prepends onto
// an empty bin (each new prepend lands in front of what's already
// there), a read-back, then a combined prepend+get in one call.
//
// Source: examples/ and the use-case-cookbook never touch Prepend at
// all — confirmed by direct grep across both, same as every other
// package under sdktestexamples/. The only real usage found anywhere is
// this active test. (`Append` is already touched elsewhere in this repo
// — `sdktestexamples/udf`'s `DemonstrateWriteIfGenerationNotChanged` —
// but only incidentally, as a plain seed-write inside a UDF-generation
// test, not grounded in this file's own `append()` test method.)
package prepend

const binName = "appendbin"
