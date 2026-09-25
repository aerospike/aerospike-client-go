// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: CommonExample.java inspects a Behavior's resolved settings for a
// specific operation shape:
//
//	ResolvedSettings settings = Behavior.DEFAULT.getSettings(
//	    OpKind.READ, OpShape.QUERY, Mode.CP);
//	System.out.printf("Batch mode maxConcurrentNodes = %d\n",
//	    settings.getMaxConcurrentNodes());
//
// i.e. "what timeout/retry/concurrency settings actually apply to a READ,
// shaped as a QUERY, under CP mode" — one resolved settings object per
// (OpKind, OpShape, Mode) combination. Checked sdk/behavior.go's entire
// catalog: DefaultBehavior, NewBehavior, the three presets
// (ReadFastBehavior/StrictlyConsistentBehavior/FastRackAwareBehavior), and
// Explain() — nothing resembling OpKind, OpShape, Mode, ResolvedSettings,
// or a getSettings-style lookup exists anywhere in the PRD's Behavior
// surface (10.16). Explain() only produces a human-readable description
// of a whole Behavior, not a queryable per-scope settings value. This is
// a real, checked absence, not something to approximate by parsing
// Explain()'s string output.
package commonexamples
