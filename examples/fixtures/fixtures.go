/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Package fixtures holds the Setup/Validate/Cleanup steps that verify the
// documentation examples, plus the helpers they share. Nothing in this
// package appears in documentation.
package fixtures

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// Ambient state shared by all fixtures, assigned once via Init by the
// example runner before any example executes.
var (
	client    *as.Client
	namespace string
	set       string

	// deletePolicy is nil on AP namespaces (the default delete is fine) and
	// carries DurableDelete on strong-consistency namespaces, where the
	// server forbids non-durable deletes.
	deletePolicy *as.WritePolicy
)

// Init hands the fixtures package its connection and target. The example
// runner calls it once, right after connecting and probing the server.
func Init(c *as.Client, ns, setName string, strongConsistency bool) {
	client = c
	namespace = ns
	set = setName

	if strongConsistency {
		deletePolicy = as.NewWritePolicy(0, 0)
		deletePolicy.DurableDelete = true
	}
}

// A SkipError marks an example as skipped rather than failed: the server or
// its configuration cannot support the example. Return it from any lifecycle
// step via Skip.
type SkipError struct {
	Reason string
}

func (e SkipError) Error() string { return e.Reason }

// Skip reports that an example cannot run in this environment.
func Skip(reason string) error { return SkipError{Reason: reason} }

// A Fixture holds the optional lifecycle steps that verify an example:
// Setup seeds required state (cleanup-first, so reruns work), Validate
// asserts the database state the example produced, Cleanup removes
// everything the example created. The zero value means "no steps".
type Fixture struct {
	Setup    func() error
	Validate func() error
	Cleanup  func() error
}
