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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/examples/fixtures"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
)

// Cluster configuration flags. Each default falls back to an environment
// variable so CI pipelines can configure runs without arguments.
var (
	hostFlag      = flag.String("h", envStr("AEROSPIKE_HOST", "127.0.0.1"), "Aerospike server hostname or IP address.")
	portFlag      = flag.Int("p", envInt("AEROSPIKE_PORT", 3000), "Aerospike server port number.")
	userFlag      = flag.String("U", envStr("AEROSPIKE_USER", ""), "Aerospike username.")
	passwordFlag  = flag.String("P", envStr("AEROSPIKE_PASSWORD", ""), "Aerospike password.")
	namespaceFlag = flag.String("n", envStr("AEROSPIKE_NAMESPACE", "test"), "Aerospike namespace.")
	setFlag       = flag.String("s", envStr("AEROSPIKE_SET", "testset"), "Aerospike set name.")
)

// Ambient state shared by all examples and fixtures. Assigned exactly once in
// main() before any example runs, so the documentation-ready example files
// stay free of connection and configuration code.
var (
	client *as.Client
	host   string
	port   int
	ns     string
	set    string
)

func envStr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s value %q: %v", name, v, err)
	}
	return n
}

// A stepFunc is one stage in an example's lifecycle.
type stepFunc func() error

// An Example pairs a documentation-ready Run function with its verification
// fixture (from the fixtures package) and the server capabilities it
// requires.
type Example struct {
	Name     string
	Run      stepFunc
	Fixture  fixtures.Fixture
	Requires Requirement // zero value = no requirements
}

// Requirement declares server capabilities an example needs. Requirements are
// checked once against the probed serverFacts; unmet requirements mark the
// example SKIP with a reason instead of failing it.
type Requirement struct {
	enterprise        bool
	strongConsistency bool
	ttl               bool
	tls               bool
	minVersion        *version.Version
}

func EnterpriseEdition() Requirement { return Requirement{enterprise: true} }

func TTLSupported() Requirement { return Requirement{ttl: true} }

func TLSConfigured() Requirement { return Requirement{tls: true} }

func MinServerVersion(major, minor int) Requirement {
	return Requirement{minVersion: &version.Version{Major: major, Minor: minor}}
}

func (r Requirement) AndEnterpriseEdition() Requirement { r.enterprise = true; return r }

func (r Requirement) AndStrongConsistency() Requirement { r.strongConsistency = true; return r }

func (r Requirement) AndTTLSupported() Requirement { r.ttl = true; return r }

func (r Requirement) AndMinServerVersion(major, minor int) Requirement {
	r.minVersion = &version.Version{Major: major, Minor: minor}
	return r
}

// failureReason returns the first reason the server does not satisfy this
// Requirement, or an empty string if all requirements are met.
func (req Requirement) failureReason(facts serverFacts) string {
	switch {
	case req.enterprise && !facts.enterprise:
		return "requires Aerospike Enterprise Edition"
	case req.strongConsistency && !facts.strongConsistency:
		return fmt.Sprintf(
			"requires a strong-consistency namespace (namespace %q is AP)",
			ns,
		)
	case req.ttl && !facts.ttlSupported:
		return fmt.Sprintf(
			"requires TTL support (namespace %q has nsup disabled)",
			ns,
		)
	case req.tls && !facts.tlsConfigured:
		return "requires TLS configuration (AEROSPIKE_TLS_NAME not set)"
	case req.minVersion != nil && !facts.version.IsGreaterOrEqual(req.minVersion):
		return fmt.Sprintf(
			"requires server %d.%d+ (connected to %s)",
			req.minVersion.Major,
			req.minVersion.Minor,
			facts.version.String(),
		)
	}
	return ""
}

// serverFacts is a snapshot of the connected server's capabilities, probed
// once after connecting and checked against each example's requirements.
type serverFacts struct {
	version           version.Version
	enterprise        bool
	strongConsistency bool
	ttlSupported      bool
	tlsConfigured     bool
}

func probeServerFacts() serverFacts {
	node := client.GetNodes()[0]
	facts := serverFacts{
		version:       node.GetServerVersion(),
		tlsConfigured: os.Getenv("AEROSPIKE_TLS_NAME") != "",
	}

	editionCommand := "edition"
	if facts.version.IsGreaterOrEqual(version.ServerVersion_8_1) {
		editionCommand = "release"
	}
	if infoMap, err := node.RequestInfo(as.NewInfoPolicy(), editionCommand); err == nil {
		facts.enterprise = strings.Contains(infoMap[editionCommand], "Enterprise")
	}
	// An example is skipped, not failed, when a fact cannot be confirmed.
	nsConfig := nsInfo(node)
	facts.strongConsistency = nsConfig["strong-consistency"] == "true"
	nsupPeriod := nsConfig["nsup-period"]
	facts.ttlSupported = (nsupPeriod != "" && nsupPeriod != "0") || nsConfig["allow-ttl-without-nsup"] == "true"
	return facts
}

// nsInfo fetches the target namespace's configuration in one info call and
// returns it as a key/value map (empty on error).
func nsInfo(node *as.Node) map[string]string {
	config := make(map[string]string)
	infoMap, err := node.RequestInfo(as.NewInfoPolicy(), "namespace/"+ns)
	if err != nil {
		return config
	}
	for _, pair := range strings.Split(infoMap["namespace/"+ns], ";") {
		if name, value, ok := strings.Cut(pair, "="); ok {
			config[name] = value
		}
	}
	return config
}

// skipError marks an example as skipped instead of failed.
type skipError struct {
	reason string
}

func (e skipError) Error() string {
	return e.reason
}

// skip returns an error that marks the example as skipped.
func skip(reason string) error { return skipError{reason: reason} }

// status is the outcome of one example run.
type status string

const (
	statusPass status = "PASS"
	statusSkip status = "SKIP"
	statusFail status = "FAIL"
)

type result struct {
	name    string
	status  status
	detail  string
	elapsed time.Duration
}

// execute runs one example through its lifecycle: Requirement check, then
// Setup, Run and Validate, with Cleanup deferred so it runs even when an
// earlier step fails.
func execute(ex Example, facts serverFacts) result {
	start := time.Now()
	status, detail := runLifecycle(ex, facts)
	return result{name: ex.Name, status: status, detail: detail, elapsed: time.Since(start)}
}

func runLifecycle(ex Example, facts serverFacts) (st status, detail string) {
	if reason := ex.Requires.failureReason(facts); reason != "" {
		return statusSkip, reason
	}

	if ex.Fixture.Cleanup != nil {
		defer func() {
			if err := call(ex.Fixture.Cleanup); err != nil && st == statusPass {
				st, detail = statusFail, "cleanup: "+err.Error()
			}
		}()
	}

	steps := []struct {
		name string
		fn   stepFunc
	}{
		{"setup", ex.Fixture.Setup},
		{"run", ex.Run},
		{"validate", ex.Fixture.Validate},
	}

	for _, step := range steps {
		if step.fn == nil {
			continue
		}
		err := call(step.fn)
		if err == nil {
			continue
		}
		if s, ok := err.(skipError); ok {
			return statusSkip, s.reason
		}
		return statusFail, step.name + ": " + err.Error()
	}
	return statusPass, ""
}

// call invokes a lifecycle step, converting a panic into an error so a single
// misbehaving example cannot abort the whole run.
func call(fn stepFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}
