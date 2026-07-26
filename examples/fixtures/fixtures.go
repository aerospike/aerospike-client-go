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
)

// Init hands the fixtures package its connection and target. The example
// runner calls it once, right after connecting.
func Init(c *as.Client, ns, setName string) {
	client = c
	namespace = ns
	set = setName
}

// A Fixture holds the optional lifecycle steps that verify an example:
// Setup seeds required state (cleanup-first, so reruns work), Validate
// asserts the database state the example produced, Cleanup removes
// everything the example created. The zero value means "no steps".
type Fixture struct {
	Setup    func() error
	Validate func() error
	Cleanup  func() error
}
