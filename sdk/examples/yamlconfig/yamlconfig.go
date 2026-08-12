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

// Package yamlconfig shows the SDK configuration file: the system and
// behaviors sections, connecting with file-supplied configuration, and hot
// reload.
//
// Port of the Java SDK's BehaviorYamlExample, YamlConfigExample,
// CompleteYamlConfigExample and YamlConfigConnectionExample, consolidated the
// way the Rust SDK's `yaml_config` consolidates them: the configuration path
// comes from AEROSPIKE_SDK_CONFIG_URL, which is process-global, so four
// examples each setting it would interfere with one another.
//
// This example connects its own cluster rather than using the one the harness
// supplies, because the configuration file is read once, at connect.
package yamlconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
)

// The behaviors only the file defines, so finding them proves the file was
// read.
var behaviorNames = []string{
	"yaml-fast-operations",
	"yaml-safe-operations",
	"yaml-batch-fast",
}

// initialConfig is the file as first written.
const initialConfig = `
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 1
      maximumConnectionsPerNode: 100
      maximumSocketIdleTime: 30s
    circuitBreaker:
      numTendIntervalsInErrorWindow: 1
      maximumErrorsInErrorWindow: 200
    refresh:
      tendInterval: 1s
    transactions:
      implicitBatchWriteTransactions: true
      numberOfAttempts: 5
      sleepBetweenAttempts: 1s
  docs-cluster:
    connections:
      maximumConnectionsPerNode: 600

behaviors:
  yaml-fast-operations:
    allOperations:
      abandonCallAfter: 2s
      waitForCallToComplete: 200ms
      maximumNumberOfCallAttempts: 3
      sendKey: true
    batchReads:
      maxConcurrentServers: 8
      allowInlineMemoryAccess: true
    query:
      recordQueueSize: 10000
    consistencyModeReads:
      readConsistency: SESSION

  yaml-safe-operations:
    allOperations:
      abandonCallAfter: 10s
      maximumNumberOfCallAttempts: 6
      delayBetweenRetries: 100ms
    nonRetryableWrites:
      useDurableDelete: true
    consistencyModeReads:
      readConsistency: LINEARIZE

  yaml-batch-fast:
    parent: yaml-fast-operations
    batchReads:
      maxConcurrentServers: 16
      allowInlineSsdAccess: true
`

// reloadedConfig is the same file after an operator edit: the fast behavior
// gets a longer call budget.
const reloadedConfig = `
system:
  DEFAULT:
    connections:
      minimumConnectionsPerNode: 1
      maximumConnectionsPerNode: 100
      maximumSocketIdleTime: 30s
    circuitBreaker:
      numTendIntervalsInErrorWindow: 1
      maximumErrorsInErrorWindow: 200
    refresh:
      tendInterval: 1s
    transactions:
      implicitBatchWriteTransactions: true
      numberOfAttempts: 7
      sleepBetweenAttempts: 1s

behaviors:
  yaml-fast-operations:
    allOperations:
      abandonCallAfter: 9s
      waitForCallToComplete: 200ms
      maximumNumberOfCallAttempts: 3
      sendKey: true
    batchReads:
      maxConcurrentServers: 8
      allowInlineMemoryAccess: true
    query:
      recordQueueSize: 10000
    consistencyModeReads:
      readConsistency: SESSION

  yaml-safe-operations:
    allOperations:
      abandonCallAfter: 10s
      maximumNumberOfCallAttempts: 6
      delayBetweenRetries: 100ms
    nonRetryableWrites:
      useDurableDelete: true
    consistencyModeReads:
      readConsistency: LINEARIZE

  yaml-batch-fast:
    parent: yaml-fast-operations
    batchReads:
      maxConcurrentServers: 16
      allowInlineSsdAccess: true
`

// Run executes the example.
func Run(env *exrun.Env) error {
	dir, err := os.MkdirTemp("", "aerospike_yaml_config_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "aerospike-sdk.yaml")
	if err := os.WriteFile(path, []byte(initialConfig), 0o600); err != nil {
		return err
	}
	if err := os.Setenv(sdk.EnvConfigURL, path); err != nil {
		return err
	}
	defer os.Unsetenv(sdk.EnvConfigURL)

	env.Printf("=== SDK configuration file ===")
	env.Printf("  %s", path)
	env.Printf("  %s is read once, when the client connects.", sdk.EnvConfigURL)

	// The file reaches the connection pool through the client policy, and its
	// behaviors are registered so they can be looked up by name — both of which
	// happen at connect, so this example needs its own cluster.
	cluster, err := connect(env)
	if err != nil {
		return err
	}
	defer cluster.Close()

	env.Printf("")
	env.Printf("=== Connected with file-supplied cluster configuration ===")
	showSystemSettings(env, cluster.SystemSettings())
	env.Printf("  (the file's 'docs-cluster' profile would win over DEFAULT for a client " +
		"that validated its cluster name as \"docs-cluster\")")

	session, err := cluster.CreateSession(nil)
	if err != nil {
		return err
	}
	set, err := sdk.DataSetOf(env.Namespace, "yaml_config_demo")
	if err != nil {
		return err
	}
	if err := session.Truncate(set, 0); err != nil {
		return err
	}

	// Behaviors the file defined, which no code in this program declares.
	env.Printf("")
	env.Printf("=== Behaviors loaded from the file ===")
	for _, name := range behaviorNames {
		behavior, ok := sdk.GetBehavior(name)
		if !ok {
			env.Printf("  MISSING behavior: %s", name)
			continue
		}
		env.Printf("  found behavior: %s", name)
		if parent := behavior.Parent(); parent != nil {
			env.Printf("    parent: %s", parent.Name())
		}
		showSettings(env, "    point read:  ",
			behavior.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP))
		showSettings(env, "    batch read:  ",
			behavior.Settings(sdk.OpRead, sdk.ShapeBatch, sdk.ModeAP))
		showSettings(env, "    query read:  ",
			behavior.Settings(sdk.OpRead, sdk.ShapeQuery, sdk.ModeAP))
	}

	// A session bound to a file-defined behavior does ordinary work.
	env.Printf("")
	env.Printf("=== Using a file-defined behavior ===")
	fast := sdk.GetBehaviorOrDefault("yaml-fast-operations")
	fastSession, err := cluster.CreateSession(fast)
	if err != nil {
		return err
	}
	key := set.Key("config-1")
	if err := fastSession.Put(key, as.BinMap{"name": "Ada", "age": 36}); err != nil {
		return err
	}
	rec, err := fastSession.Get(key, sdk.AllBins)
	if err != nil {
		return err
	}
	env.Printf("  wrote and read back with %s: name=%v", fast.Name(), rec.Bins["name"])

	keys := set.Keys([]int64{1, 2, 3})
	batchSession, err := cluster.CreateSession(sdk.GetBehaviorOrDefault("yaml-batch-fast"))
	if err != nil {
		return err
	}
	stream, err := batchSession.Upsert(keys).SetTo("v", 1).Execute()
	if err != nil {
		return err
	}
	rows, err := stream.Collect()
	if err != nil {
		return err
	}
	env.Printf("  batch wrote %d more records with yaml-batch-fast", len(rows))

	// The derived behavior inherits its parent and overrides one scope.
	if derived, ok := sdk.GetBehavior("yaml-batch-fast"); ok {
		batch := derived.Settings(sdk.OpRead, sdk.ShapeBatch, sdk.ModeAP)
		if batch.MaxConcurrentNodes != nil {
			env.Printf("  yaml-batch-fast batch reads use %d concurrent servers, "+
				"overriding its parent's 8", *batch.MaxConcurrentNodes)
		}
	}

	// Hot reload: rewrite the file and watch the behavior change underneath.
	env.Printf("")
	env.Printf("=== Hot reload ===")
	before := totalTimeout(fast)
	env.Printf("  yaml-fast-operations abandonCallAfter before: %v", before)

	if err := os.WriteFile(path, []byte(reloadedConfig), 0o600); err != nil {
		return err
	}

	// The monitor polls about once a second and reloads on a content change.
	var after time.Duration
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		if b, ok := sdk.GetBehavior("yaml-fast-operations"); ok {
			if got := totalTimeout(b); got != before {
				after = got
				break
			}
		}
	}
	if after != 0 {
		env.Printf("  reloaded from disk, abandonCallAfter now: %v", after)
	} else {
		env.Printf("  reload did not land within 10s (nothing was reapplied)")
	}
	env.Printf("  the behaviors section is live; connection sizing and tend timing are " +
		"applied at connect only, so changing those means reconnecting.")

	// The session bound to the reloaded behavior keeps working, with its cached
	// policies rebuilt underneath it.
	rec, err = fastSession.Get(key, sdk.AllBins)
	if err != nil {
		return err
	}
	env.Printf("  read after reload: name=%v", rec.Bins["name"])

	if err := session.Truncate(set, 0); err != nil {
		return err
	}
	env.Printf("")
	env.Printf("Removed the config file and %s.", sdk.EnvConfigURL)
	return nil
}

// connect opens a cluster that will read the configuration file, honoring the
// same environment the harness does.
func connect(env *exrun.Env) (*sdk.Cluster, error) {
	hosts := os.Getenv("AEROSPIKE_HOSTS")
	if hosts == "" {
		hosts = "127.0.0.1:3000"
	}
	var seeds []*as.Host
	for _, part := range strings.Split(hosts, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, port, found := strings.Cut(part, ":")
		if !found {
			seeds = append(seeds, as.NewHost(part, 3000))
			continue
		}
		var p int
		if _, err := fmt.Sscanf(port, "%d", &p); err != nil || p == 0 {
			p = 3000
		}
		seeds = append(seeds, as.NewHost(host, p))
	}

	def := sdk.WithHosts(seeds...)
	if v := strings.ToLower(os.Getenv("AEROSPIKE_USE_SERVICES_ALTERNATE")); v == "true" || v == "1" {
		def = def.UsingServicesAlternate()
	}
	return def.Connect()
}

// showSystemSettings prints the cluster-wide settings the file supplied.
func showSystemSettings(env *exrun.Env, s sdk.SystemSettings) {
	if s.MinConnectionsPerNode != nil {
		env.Printf("  minimum connections per node: %d", *s.MinConnectionsPerNode)
	}
	if s.MaxConnectionsPerNode != nil {
		env.Printf("  maximum connections per node: %d", *s.MaxConnectionsPerNode)
	}
	if s.MaxSocketIdleTime != nil {
		env.Printf("  maximum socket idle time:     %v", *s.MaxSocketIdleTime)
	}
	if s.TendInterval != nil {
		env.Printf("  tend interval:                %v", *s.TendInterval)
	}
	if s.MaxErrorsInErrorWindow != nil {
		env.Printf("  maximum errors in window:     %d", *s.MaxErrorsInErrorWindow)
	}
	if s.Transactions.NumberOfAttempts != nil {
		env.Printf("  transaction attempts:         %d", *s.Transactions.NumberOfAttempts)
	}
	if s.Transactions.ImplicitBatchWriteTransactions != nil {
		env.Printf("  implicit batch-write txns:    %t", *s.Transactions.ImplicitBatchWriteTransactions)
	}
}

// showSettings prints the fields a file block is likely to have set.
func showSettings(env *exrun.Env, prefix string, s sdk.Settings) {
	var parts []string
	if s.TotalTimeout != nil {
		parts = append(parts, fmt.Sprintf("total=%v", *s.TotalTimeout))
	}
	if s.SocketTimeout != nil {
		parts = append(parts, fmt.Sprintf("socket=%v", *s.SocketTimeout))
	}
	if s.MaxRetries != nil {
		parts = append(parts, fmt.Sprintf("retries=%d", *s.MaxRetries))
	}
	if s.MaxConcurrentNodes != nil {
		parts = append(parts, fmt.Sprintf("concurrent=%d", *s.MaxConcurrentNodes))
	}
	if s.RecordQueueSize != nil {
		parts = append(parts, fmt.Sprintf("queue=%d", *s.RecordQueueSize))
	}
	if s.SendKey != nil {
		parts = append(parts, fmt.Sprintf("sendKey=%t", *s.SendKey))
	}
	if len(parts) == 0 {
		parts = append(parts, "(nothing set)")
	}
	env.Printf("%s%s", prefix, strings.Join(parts, " "))
}

// totalTimeout reads a behavior's point-read total timeout, the field the
// reload changes.
func totalTimeout(b *sdk.Behavior) time.Duration {
	s := b.Settings(sdk.OpRead, sdk.ShapePoint, sdk.ModeAP)
	if s.TotalTimeout == nil {
		return 0
	}
	return *s.TotalTimeout
}
