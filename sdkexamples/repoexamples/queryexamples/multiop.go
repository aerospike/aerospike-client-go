// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: the source Java QueryExamples.java has a "Multi operation batches"
// section chaining heterogeneous operations across multiple datasets into
// one call/stream:
//
//	session.update(customerDataSet.ids(1000, 1001))
//	    .bin("age").add(1)
//	    .bin("dob").setTo(new Date().getTime())
//	    .expireRecordAfter(Duration.ofMinutes(5))
//	.exists(customerDataSet.ids(1000, 1001))
//	.query(customerDataSet.ids(10, 12))
//	.delete(customerDataSet.id(1003))
//	.defaultExpireRecordAfter(Duration.ofMinutes(20))
//	.execute()
//
// One builder, one .execute(), but update + exists + query + delete are
// four different kinds of operation, potentially against four different
// key sets, combined into a single result stream.
//
// Checked every entry point on *Session (sdk/session.go): Upsert, Insert,
// Update, Replace, ReplaceIfExists, Delete, Touch — each takes exactly one
// *as.Key and returns a *WriteSegmentBuilder scoped to that one write.
// BatchWrite(ctx, []WriteOp) (10.6) is the PRD's actual mechanism for
// combining multiple operations into one round-trip, but WriteOp is
// write-only (UpsertOp/DeleteOp/...) — there's no ExistsOp or QueryOp, so
// a heterogeneous group that includes a read-style exists/query check
// alongside writes can't be expressed there either. Nothing in the PRD
// gives this shape a Go equivalent: not a builder, not a batch entry, not
// documented as intentionally dropped. This is a real, unaddressed gap
// between the Java source and the current sdk/ catalog, not an ambiguity
// to resolve by guessing at a shape — there's nothing here to build.
package queryexamples
