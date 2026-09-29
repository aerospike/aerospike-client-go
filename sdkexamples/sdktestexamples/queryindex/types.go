// Package queryindex demonstrates secondary index creation and filtered
// queries, one index type at a time, matching the real SDK's own
// query/QueryStringTest.java, QueryGeoTest.java, QueryBlobTest.java, and
// QueryCollectionTest.java — each active, each creating a real index of
// its type and querying against it.
//
// Source: examples/ and the use-case-cookbook never touch secondary
// indexes at all — confirmed by direct grep across both, same as every
// other package under sdktestexamples/. The only real coverage found
// anywhere is these active test files.
//
// GAP (package-wide): the real Java SDK creates/drops indexes via flat
// methods — session.createIndex(set, indexName, binName, IndexType,
// IndexCollectionType), session.dropIndex(set, indexName) — not a
// fluent builder. sdk/'s own IndexBuilder (Session.Index(ctx,
// ds).OnBin(...).Named(...).<Type>().Create(ctx)/.Drop(ctx)) is a real,
// deliberate Go-side redesign (D-27: fluent parity is not a goal), so
// every demo here is built against sdk/'s actual fluent chain, not a
// literal translation of Java's flat call — same precedent as
// RunInTransaction vs doInTransaction.
//
// Built one source file at a time. Currently covers QueryStringTest.java
// only (DemonstrateStringIndex, in stringindex.go).
package queryindex
