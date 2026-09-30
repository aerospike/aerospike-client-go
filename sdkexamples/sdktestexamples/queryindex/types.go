// Package queryindex demonstrates secondary index creation and filtered
// queries, one index type at a time, matching the real SDK's own
// query/QueryStringTest.java, QueryBlobTest.java, and
// QueryCollectionTest.java — each active, each creating a real index of
// its type and querying against it. query/QueryGeoTest.java was checked
// too and found entirely disabled (see below) — not part of this
// package's source set.
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
// GAP: QueryGeoTest.java is entirely disabled — both @BeforeAll setup
// and its one @Test are wrapped in comment blocks, with the disabling
// comment reading "TODO Port when geojson is supported in
// BinValuesBuilder." Direct evidence from the Java side (not just PRD
// silence) that GeoJSON isn't wired through the current fluent write
// builder at all — reinforces sdk/FUNCTIONAL_GAPS.md finding #21. No
// geo-index demo exists in this package for that reason, not oversight.
//
// Built one source file at a time. Currently covers QueryStringTest.java
// only (DemonstrateStringIndex, in stringindex.go).
package queryindex
