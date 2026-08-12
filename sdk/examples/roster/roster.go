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

// Package roster reads and initializes a namespace roster with info commands.
//
// Port of the Java SDK's RosterExample, by way of the Rust SDK's `roster`.
// It reads `roster:namespace=<ns>`, sets the roster on every node from the
// observed-node list, then triggers a recluster. Roster management exists only
// for strong-consistency namespaces, so against an availability-mode namespace
// the example reports what it found and stops.
package roster

import (
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// Run executes the example.
//
// It prefers the strong-consistency namespace when one is configured, since
// that is the only place a roster exists.
func Run(env *exrun.Env) error {
	namespace := env.Namespace
	if env.SCNamespace != "" {
		namespace = env.SCNamespace
	}

	// 1) Read the current roster from one node.
	read := "roster:namespace=" + namespace
	response := ""
	if responses, err := env.Session.Info(read); err != nil {
		env.Printf("Roster read failed: %v", err)
	} else {
		response = firstBody(responses, read)
	}
	env.Printf("Current roster: %s", response)

	observed, ok := parseObservedNodes(response)
	if !ok {
		env.Printf("Skipping: roster commands need a strong-consistency namespace "+
			"reporting observed_nodes; namespace %s reported none.", namespace)
		return nil
	}
	env.Printf("Observed nodes: %s", observed)

	// 2) Set the roster on every node to the observed list.
	set := "roster-set:namespace=" + namespace + ";nodes=" + observed
	if responses, err := env.Session.InfoOnAllNodes(set); err != nil {
		env.Printf("roster-set failed: %v", err)
	} else {
		for node, body := range responses {
			env.Printf("roster-set on %s: %s", node, firstBody(body, set))
		}
	}

	// 3) Apply the change cluster-wide.
	if responses, err := env.Session.InfoOnAllNodes("recluster:"); err != nil {
		env.Printf("recluster failed: %v", err)
	} else {
		for node, body := range responses {
			env.Printf("recluster on %s: %s", node, firstBody(body, "recluster:"))
		}
	}

	// Give the cluster a moment to settle before reporting success.
	time.Sleep(3 * time.Second)
	env.Printf("Roster initialization complete")
	return nil
}

// firstBody reports the value stored under command, or the first value when the
// server echoed the request differently.
func firstBody(responses map[string]string, command string) string {
	if body, ok := responses[command]; ok {
		return body
	}
	for _, body := range responses {
		return body
	}
	return ""
}

// parseObservedNodes pulls `observed_nodes=...` out of a roster response such as
// `roster=null:pending_roster=null:observed_nodes=BB9A469513007B2,BB92D77C4434192`.
//
// It reports absence when the field is missing, empty or null, or when the
// server answered with an error — the availability-mode case.
func parseObservedNodes(response string) (string, bool) {
	if response == "" || strings.Contains(strings.ToLower(response), "error") {
		return "", false
	}
	for _, part := range strings.Split(response, ":") {
		nodes, found := strings.CutPrefix(part, "observed_nodes=")
		if !found {
			continue
		}
		nodes = strings.TrimSpace(nodes)
		if nodes == "" || nodes == "null" {
			return "", false
		}
		return nodes, true
	}
	return "", false
}
