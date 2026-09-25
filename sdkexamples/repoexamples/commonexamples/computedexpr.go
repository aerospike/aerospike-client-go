// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: CommonExample.java has a "Read and write operation example"
// section using computed, expression-derived bins on both sides:
//
//	session.upsert(set.ids(1,2,3))
//	    .bin("readBin").selectFrom("$.age + 12")   // virtual read-only projection
//	    .bin("writeBin").upsertFrom("$.age + 30")  // stored, computed at write time
//	    .execute();
//
//	session.query(set.id(1))
//	    .bin("ageIn20Years").selectFrom("$.age + 20")  // computed read-time projection
//	    .execute();
//
// Checked every method on WriteBinBuilder (sdk/writesegmentbuilder.go) and
// QueryBinBuilder (sdk/query.go): neither has anything resembling
// selectFrom/upsertFrom — SetTo only accepts a literal value, and
// QueryBinBuilder's only method is SelectAs (rename, not compute). AEL
// expressions do exist elsewhere in the PRD (WhereAEL, filters), but there
// is no catalog entry anywhere for evaluating an AEL expression into a new
// bin, on read or on write. This is a real, confirmed absence, not
// something to approximate with WhereAEL or Set — there's nothing here to
// build.
package commonexamples
