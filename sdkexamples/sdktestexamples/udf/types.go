// Package udf demonstrates the UDF surface (§10.11): register a Lua
// module, execute several of its functions against a record, list
// registered modules, then remove it.
//
// Source: none of the "example" repositories (aerospike-client-java-sdk's
// own examples/, or the use-case-cookbook) touch UDF at all — confirmed
// by direct grep across both for every real UDF method name
// (registerUdf/registerUdfString/removeUdf/executeUdf/listUdf/UDFModule):
// zero matches anywhere except one commented-out line in QueryExamples.java.
// The only real UDF usage found anywhere is in the SDK's own test suite
// (client/src/test/java/com/aerospike/client/sdk/UdfTest.java) — not a
// curated example, but its embedded Lua module (record_example.lua, see
// scripts/) is real, realistic, reusable source, faithfully copied here
// (trimmed to the functions this package actually calls — the source
// file also has an unrelated busy-wait helper and a commented-out stub
// not needed here).
package udf
