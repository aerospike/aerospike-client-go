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

package aerospike_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	l3KubectlTimeout = 120 * time.Second
	l3ScaleTimeout   = 900 * time.Second
	l3MinPods        = 2
)

// l3Pod is one Aerospike pod as the API server reports it.
type l3Pod struct {
	name  string
	node  string
	ip    string
	phase string
}

func (pod l3Pod) String() string {
	return pod.name + " node=" + pod.node + " ip=" + pod.ip + " phase=" + pod.phase
}

// l3Cluster is the single AerospikeCluster in the kind cluster that
// setup-kind-ako (or an equivalent local run) left running.
type l3Cluster struct {
	namespace string
	name      string
}

func l3FindCluster(t *testing.T) l3Cluster {
	t.Helper()
	lines := l3Lines(l3Kubectl(t, "get", "aerospikecluster", "--all-namespaces", "-o",
		`jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}`))
	if len(lines) != 1 {
		t.Fatalf("AerospikeClusters %v, want 1", lines)
	}
	parts := strings.Fields(lines[0])
	if len(parts) != 2 {
		t.Fatalf("AerospikeCluster line %q", lines[0])
	}
	return l3Cluster{namespace: parts[0], name: parts[1]}
}

func (cluster l3Cluster) get(t *testing.T, jsonpath string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "aerospikecluster", cluster.name,
		"-o", "jsonpath={"+jsonpath+"}"))
}

func (cluster l3Cluster) size(t *testing.T) int {
	t.Helper()
	return l3Atoi(t, "spec.size", cluster.get(t, ".spec.size"))
}

func (cluster l3Cluster) multiPodPerHost(t *testing.T) bool {
	t.Helper()
	return cluster.get(t, ".spec.podSpec.multiPodPerHost") == "true"
}

func (cluster l3Cluster) servicePort(t *testing.T) int {
	t.Helper()
	return l3Atoi(t, "service.port", cluster.get(t, ".spec.aerospikeConfig.network.service.port"))
}

func (cluster l3Cluster) pods(t *testing.T) []l3Pod {
	t.Helper()
	var pods []l3Pod
	for _, line := range l3Lines(l3Kubectl(t, "-n", cluster.namespace, "get", "pods",
		"-l", "aerospike.com/cr="+cluster.name, "-o",
		`jsonpath={range .items[*]}{.metadata.name} {.spec.nodeName} {.status.podIP} {.status.phase}{"\n"}{end}`)) {
		parts := strings.SplitN(line, " ", 4)
		if len(parts) != 4 {
			t.Fatalf("pod line %q", line)
		}
		pods = append(pods, l3Pod{name: parts[0], node: parts[1], ip: parts[2], phase: parts[3]})
	}
	slices.SortFunc(pods, func(a, b l3Pod) int { return strings.Compare(a.name, b.name) })
	return pods
}

func (cluster l3Cluster) nodePort(t *testing.T, pod string, port int) int {
	t.Helper()
	value := strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "service", pod, "--ignore-not-found",
		"-o", fmt.Sprintf("jsonpath={.spec.ports[?(@.port==%d)].nodePort}", port)))
	if value == "" {
		return 0
	}
	return l3Atoi(t, "nodePort", value)
}

// hostPort is the hostPort the pod's containers publish port on, or 0 when none does.
func (cluster l3Cluster) hostPort(t *testing.T, pod string, port int) int {
	t.Helper()
	value := strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "pod", pod,
		"-o", fmt.Sprintf("jsonpath={.spec.containers[*].ports[?(@.containerPort==%d)].hostPort}", port)))
	if value == "" {
		return 0
	}
	return l3Atoi(t, "hostPort", strings.Fields(value)[0])
}

func (cluster l3Cluster) nodeLabel(t *testing.T, node string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "get", "node", node, "-o",
		`jsonpath={.metadata.labels.aerospike\.com/configured-alternate-access-address}`))
}

// ensureSize scales the cluster up when it has fewer than want pods, and
// restores the original size when the test ends.
func (cluster l3Cluster) ensureSize(t *testing.T, want int) {
	t.Helper()
	size := cluster.size(t)
	if size >= want {
		return
	}
	t.Cleanup(func() { cluster.scale(t, size) })
	cluster.scale(t, want)
}

func (cluster l3Cluster) scale(t *testing.T, size int) {
	t.Helper()
	l3Kubectl(t, "-n", cluster.namespace, "patch", "aerospikecluster", cluster.name, "--type", "merge",
		"-p", fmt.Sprintf(`{"spec":{"size":%d}}`, size))
	var state string
	if !waitFor(l3ScaleTimeout, 2*time.Second, func() bool {
		state = cluster.get(t, ".status.size") + " " + cluster.get(t, ".status.phase")
		if cluster.get(t, ".status.size") != strconv.Itoa(size) || cluster.get(t, ".status.phase") != "Completed" {
			return false
		}
		pods := cluster.pods(t)
		if len(pods) != size {
			return false
		}
		for _, pod := range pods {
			if pod.phase != "Running" {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("AerospikeCluster %s not Completed at size %d within %s: %s", cluster.name, size, l3ScaleTimeout, state)
	}
}

func l3Kubectl(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), l3KubectlTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--request-timeout=30s"}, args...)...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+os.Getenv("DISCOVERY_KUBECONFIG"))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String()
}

func l3CSVPorts(t *testing.T, name string) []int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		t.Fatalf("%s is not set", name)
	}
	var ports []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		port, err := strconv.Atoi(part)
		if err != nil || port <= 0 {
			t.Fatalf("%s has invalid port %q", name, part)
		}
		ports = append(ports, port)
	}
	if len(ports) == 0 {
		t.Fatalf("%s is empty", name)
	}
	return ports
}

func l3Lines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func l3Atoi(t *testing.T, what, raw string) int {
	t.Helper()
	n, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s %q: %v", what, raw, err)
	}
	return n
}

func waitFor(timeout, poll time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(poll)
	}
	return true
}
