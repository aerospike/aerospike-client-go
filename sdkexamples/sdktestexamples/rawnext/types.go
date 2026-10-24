// Package rawnext demonstrates the raw ReadStream/WriteStream Next(ctx)
// terminal — the manual, position-by-position pull, as opposed to Iter's
// range-over-func drain and NavigatableStream.Next's pagination form —
// matching the real SDK's own BatchTest.java (active), specifically the
// first half of batchWriteComplex(): a single heterogeneous BatchWrite
// (mixed ops across different keys) whose results the test checks one at
// a time via next(), never via a while(hasNext()) loop, because the
// point is asserting a specific result lands at a specific stream
// position (request order), not "process however many results come
// back" — a distinction Iter's range-over-func doesn't let a caller make
// as directly as a manual pull can.
//
// Source: examples/ and the use-case-cookbook never manually
// single-step a stream at all — every drain there already goes through
// Iter (or, for the single-key-as-stream idiom Java uses in
// ReplaceTest.java/TouchTest.java, Go's Get/Touch return the value
// directly and never produce a stream to walk — a real Go-idiom
// difference, not a gap: confirmed by grep, those are the only two
// active-test files anywhere in the tree that call next() outside a
// hasNext() loop, and both are single-key reads Go's direct calls
// already cover without a stream).
//
// GAP: the real test's second BatchWrite entry deliberately targets a
// different (invalid) namespace and a third uses an ael upsertFrom
// expression, specifically to make each position return a distinct
// result code worth checking individually. sdk/ is a fully opaque stub
// (every DataSet/Key/stream call returns nil unconditionally, per this
// session's standing finding) — there's no way to actually construct a
// key that behaves differently at runtime, and nothing here executes
// against a live cluster anyway. Simplified to two Upserts plus a
// Delete on three ordinary keys: the shape being demonstrated (manual
// Next() per position, in request order) survives the simplification
// intact; the specific result-code values the real test asserts do not,
// and can't, in this stub.
package rawnext

const binName = "bin2"
