// This file documents, rather than demonstrates, one concept from the
// source Java example with no equivalent anywhere in sdk/. There is no
// function here: there is nothing real to call.
//
// GAP: CommonExample.java writes multiple records in one call repeatedly
// — "Write 1 record", "Write 3 records", "Write 10 records", and async
// variants of each:
//
//	session.upsert(set)
//	    .bins("name", "age")
//	    .id(1).values("Tim", 312)
//	    .id(2).values("Bob", 25)
//	    .id(3).values("Jane", 46)
//	    .execute();
//
// This is the same §10.9 RowWriteBuilder shape already blocked elsewhere
// in this repo (ecommerce/seed.go, and the bulk-typed-insert gap noted
// during the queryexamples build) — Bins(names...) + repeated
// id/values + Execute. That earlier finding was about the typed case
// (UpsertRows(ctx, ds) for a TypedDataSet[T]); this is the plain-bins,
// untyped version of the identical pattern. Same root cause, same
// blocker: RowWriteBuilder doesn't exist anywhere in sdk/ yet, typed or
// not. Not re-documented as a separate defect — this file exists only so
// this package doesn't leave it silently unaddressed with no trace of
// why one of CommonExample.java's most-repeated patterns never shows up
// here.
package commonexamples
