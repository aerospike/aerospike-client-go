//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sdk is the high-level, ergonomics-first Aerospike client for Go.
//
// It sits on top of the core client (the parent package) and is the Go
// counterpart of the Rust `aerospike-sdk` crate, the Python `aerospike_sdk`
// package and the Java `com.aerospike.client.sdk` package: the same
// connect -> session -> fluent-builder model, the same Behavior policy system,
// and the same result and error semantics.
//
// # Object model
//
//	ClusterDefinition --Connect()--> Cluster --CreateSession(b)--> Session
//	                                                                 |
//	                                          Get/Put                | (fast path)
//	                                          Upsert/Insert/...      | -> WriteSegmentBuilder
//	                                          Query(key|keys|ds)     | -> QueryBuilder
//	                                          Transaction()          | -> TransactionalSession
//	                                                                 v
//	                                          Execute()/Stream() -> RecordStream -> RecordResult
//
// # Quick start
//
//	cluster, err := sdk.NewClusterDefinition("localhost", 3000).Connect()
//	defer cluster.Close()
//
//	session, err := cluster.CreateSession(nil)
//	users, _ := sdk.DataSetOf("test", "users")
//	key := users.Key("user-1")
//
//	err = session.Put(key, as.BinMap{"name": "Ada"})
//	rec, err := session.Get(key, sdk.AllBins)
//
// # Go version
//
// This package requires Go 1.27 or newer: it uses parameterized methods to
// express the target-polymorphic verbs (Upsert accepts one key or many through
// one method) that the other Aerospike SDKs express with overloads or
// Into-conversions. The core client keeps its own, lower, Go version floor.
package sdk
