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

// Command awsrealprobe is the Go probe for the aws-tags real AWS harness
// (aerospike-client-conformance aws-real/). It prints one JSON object.
//
//	latency  cold refresh, then AWS_REAL_WARM warm refreshes
//	denied   one refresh with the credentials in the environment
//	join     client seeded only by the provider, then AWS_REAL_REFRESHES counted
//
// Required: AWS_REAL_REGION, AWS_REAL_TAG (key=value).
// Optional: AWS_REAL_WARM (20), AWS_REAL_EXPECTED_NODES (join),
// AWS_REAL_NAMESPACE (test), AWS_REAL_REFRESHES (30).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const partitionCount = 4096

// Join counters match seed_refresher.go, not Java.
const (
	credentialsPrefix = "AWS EC2 seed provider credentials from "
	refreshPrefix     = "AWS EC2 seed provider "
	refreshReturned   = " returned "
	overrunText       = "exceeding the tend refresh deadline"
	trippedText       = "exceeded the tend refresh deadline"
	failurePrefix     = "Seed refresh by provider"
	failureMark       = "failed;"
)

var logs logCapture

func main() {
	if len(os.Args) != 2 {
		fail("usage: awsrealprobe latency|denied|join")
	}

	tag := env("AWS_REAL_TAG", "")
	eq := strings.IndexByte(tag, '=')
	if eq <= 0 || eq == len(tag)-1 {
		fail("AWS_REAL_TAG must be key=value")
	}
	region, key, value := env("AWS_REAL_REGION", ""), tag[:eq], tag[eq+1:]
	logger.Logger.SetLogger(&logs)

	switch os.Args[1] {
	case "latency":
		logger.Logger.SetLevel(logger.INFO)
		printJSON(latency(newProvider(region, key, value), envInt("AWS_REAL_WARM", "20")))
	case "denied":
		logger.Logger.SetLevel(logger.INFO)
		printJSON(denied(newProvider(region, key, value)))
	case "join":
		logger.Logger.SetLevel(logger.DEBUG)
		printJSON(join(region, key, value, envInt("AWS_REAL_EXPECTED_NODES", ""),
			env("AWS_REAL_NAMESPACE", "test"), envInt("AWS_REAL_REFRESHES", "30")))
	default:
		fail("unknown mode " + os.Args[1])
	}
}

func newProvider(region, key, value string) as.SeedCandidateProvider {
	p, err := as.NewAwsEc2TagSeedCandidateProvider(as.AwsEc2TagSeedCandidateProviderConfig{
		Region:  region,
		Filters: []as.AwsFilter{as.AwsTagFilter(key, value)},
	})
	if err != nil {
		fail(flatten(err))
	}
	return p
}

func latency(p as.SeedCandidateProvider, warm int) map[string]any {
	hosts, cold, err := timedRefresh(p, 0)
	if err != nil {
		return credJSON("latency", nil, err)
	}
	times := make([]int64, 0, warm)
	failures := make([]string, 0)
	for i := 1; i <= warm; i++ {
		_, us, err := timedRefresh(p, int64(i))
		if err != nil {
			failures = append(failures, flatten(err))
		}
		times = append(times, us)
	}
	out := credJSON("latency", hosts, nil)
	out["cold_us"] = cold
	out["warm_us"] = times
	out["warm_failures"] = failures
	return out
}

func denied(p as.SeedCandidateProvider) map[string]any {
	hosts, err := p.RefreshSeedCandidates(&as.SeedCandidateContext{})
	return credJSON("denied", hosts, err)
}

func credJSON(mode string, hosts []*as.Host, err error) map[string]any {
	out := map[string]any{"mode": mode, "credentials_source": logs.source()}
	if err != nil {
		out["error"] = flatten(err)
		return out
	}
	out["hosts"] = hostStrings(hosts)
	return out
}

func timedRefresh(p as.SeedCandidateProvider, n int64) ([]*as.Host, int64, error) {
	start := time.Now()
	hosts, err := p.RefreshSeedCandidates(&as.SeedCandidateContext{TendIteration: n})
	return hosts, time.Since(start).Microseconds(), err
}

func join(region, key, value string, expected int, namespace string, want int) map[string]any {
	policy := as.NewClientPolicy()
	policy.DiscoveryExecutionMode = as.DiscoveryExecutionThread
	policy.DiscoveryRefreshInterval = policy.TendInterval
	policy.SeedProvider = newProvider(region, key, value)

	client, err := as.NewClientWithPolicyAndHost(policy)
	if err != nil {
		if client != nil {
			client.Close()
		}
		return map[string]any{"mode": "join", "error": flatten(err)}
	}
	defer client.Close()

	if !until(60*time.Second, func() bool {
		return len(client.Cluster().GetNodes()) >= expected && ownedPartitions(client, namespace) >= partitionCount
	}) {
		out := joinJSON(client, namespace)
		out["error"] = fmt.Sprintf("nodes=%d owned=%d after 60s", out["nodes"], out["owned_partitions"])
		return out
	}

	logs.clear()
	wait := time.Duration(want)*policy.DiscoveryRefreshInterval + 60*time.Second
	if !until(wait, func() bool {
		s := logs.stats()
		return s.refreshes >= want || s.tripped
	}) {
		out := joinJSON(client, namespace)
		out["error"] = fmt.Sprintf("only %d refreshes in %dms", out["refreshes"], wait.Milliseconds())
		return out
	}
	return joinJSON(client, namespace)
}

func joinJSON(client *as.Client, namespace string) map[string]any {
	s := logs.stats()
	return map[string]any{
		"mode":             "join",
		"nodes":            len(client.Cluster().GetNodes()),
		"owned_partitions": ownedPartitions(client, namespace),
		"partitions":       partitionCount,
		"refreshes":        s.refreshes,
		"overruns":         s.overruns,
		"failures":         s.failures,
		"tripped":          s.tripped,
	}
}

func ownedPartitions(client *as.Client, namespace string) int {
	cluster := client.Cluster()
	policy := as.NewPolicy()
	policy.ReplicaPolicy = as.MASTER
	digest := make([]byte, 20)
	owned := 0
	for i := range partitionCount {
		digest[0] = byte(i)
		digest[1] = byte(i >> 8)
		key, err := as.NewKeyWithDigest(namespace, "", nil, digest)
		if err != nil {
			continue
		}
		part, err := as.PartitionForWrite(cluster, policy, key)
		if err != nil {
			continue
		}
		node, err := part.GetNodeWrite(cluster)
		if err == nil && node != nil && node.IsActive() {
			owned++
		}
	}
	return owned
}

func until(d time.Duration, done func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if done() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func hostStrings(hosts []*as.Host) []string {
	out := make([]string, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, fmt.Sprintf("%s:%d", h.Name, h.Port))
	}
	return out
}

func flatten(err error) string {
	return strings.ReplaceAll(err.Error(), "\n", " ")
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		fail("json encode: " + err.Error())
	}
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	if fallback != "" {
		return fallback
	}
	fail(name + " is not set")
	return ""
}

func envInt(name, fallback string) int {
	n, err := strconv.Atoi(env(name, fallback))
	if err != nil {
		fail(name + " must be an integer")
	}
	return n
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (c *logCapture) Printf(format string, v ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, fmt.Sprintf(format, v...))
}

func (c *logCapture) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = nil
}

func (c *logCapture) source() any {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if strings.HasPrefix(m, credentialsPrefix) {
			return strings.TrimSpace(m[len(credentialsPrefix):])
		}
	}
	return nil
}

type logStats struct {
	refreshes int
	overruns  int
	failures  []string
	tripped   bool
}

func (c *logCapture) stats() logStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := logStats{failures: make([]string, 0)}
	for _, m := range c.msgs {
		if strings.HasPrefix(m, refreshPrefix) && strings.Contains(m, refreshReturned) {
			s.refreshes++
		}
		if strings.Contains(m, overrunText) {
			s.overruns++
		}
		if strings.Contains(m, trippedText) {
			s.tripped = true
		}
		if strings.Contains(m, failurePrefix) && strings.Contains(m, failureMark) {
			s.failures = append(s.failures, m)
		}
	}
	return s
}
