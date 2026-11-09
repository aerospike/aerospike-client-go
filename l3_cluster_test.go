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
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
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

// Pod as the API server reports it.
type l3Pod struct {
	name  string
	node  string
	ip    string
	phase string
}

func (pod l3Pod) String() string {
	return pod.name + " node=" + pod.node + " ip=" + pod.ip + " phase=" + pod.phase
}

// The one AerospikeCluster in the kind cluster.
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

// Container hostPort for port, or 0 if none.
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

func (cluster l3Cluster) uid(t *testing.T, kind, object string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", kind, object, "--ignore-not-found",
		"-o", "jsonpath={.metadata.uid}"))
}

func (cluster l3Cluster) podReadyAt(t *testing.T, pod string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "pod", pod, "-o",
		`jsonpath={.status.conditions[?(@.type=="Ready")].lastTransitionTime}`))
}

func (cluster l3Cluster) readinessProbe(t *testing.T, pod string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "pod", pod, "-o",
		`jsonpath={.spec.containers[?(@.name=="aerospike-server")].readinessProbe}`))
}

func (cluster l3Cluster) publishNotReadyAddresses(t *testing.T, service string) string {
	t.Helper()
	return strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "service", service, "-o",
		"jsonpath={.spec.publishNotReadyAddresses}"))
}

func (cluster l3Cluster) readyEndpoints(t *testing.T, service string) int {
	t.Helper()
	ready := l3Kubectl(t, "-n", cluster.namespace, "get", "endpointslices", "-l",
		"kubernetes.io/service-name="+service, "-o",
		`jsonpath={range .items[*].endpoints[*]}{.conditions.ready} {end}`)
	count := 0
	for _, value := range strings.Fields(ready) {
		if value == "true" {
			count++
		}
	}
	return count
}

func (cluster l3Cluster) deletePod(t *testing.T, pod string) {
	t.Helper()
	l3Kubectl(t, "-n", cluster.namespace, "delete", "pod", pod, "--wait=true")
}

func (cluster l3Cluster) createPodNodePortService(t *testing.T, service, pod string, port, nodePort int) {
	t.Helper()
	l3Kubectl(t, "-n", cluster.namespace, "create", "service", "nodeport", service,
		fmt.Sprintf("--tcp=%d:%d", port, port), fmt.Sprintf("--node-port=%d", nodePort))
	l3Kubectl(t, "-n", cluster.namespace, "set", "selector", "service", service,
		"statefulset.kubernetes.io/pod-name="+pod)
}

func (cluster l3Cluster) deleteService(t *testing.T, service string) {
	t.Helper()
	l3Kubectl(t, "-n", cluster.namespace, "delete", "service", service, "--ignore-not-found", "--wait=true")
}

func (cluster l3Cluster) secretData(t *testing.T, secret, key string) []byte {
	t.Helper()
	raw := strings.TrimSpace(l3Kubectl(t, "-n", cluster.namespace, "get", "secret", secret, "-o",
		"jsonpath={.data."+strings.ReplaceAll(key, ".", "\\.")+"}"))
	if raw == "" {
		t.Fatalf("secret %s has no key %s", secret, key)
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("secret %s key %s: %v", secret, key, err)
	}
	return data
}

// API server NodePort range, from a rejected Service create.
func (cluster l3Cluster) nodePortRange(t *testing.T) (int, int) {
	t.Helper()
	probe := cluster.name + "-node-port-range"
	if cluster.uid(t, "service", probe) != "" {
		t.Fatalf("Service %s exists", probe)
	}
	defer cluster.deleteService(t, probe)
	errOut := l3KubectlError(t, "-n", cluster.namespace, "create", "service", "nodeport", probe,
		"--tcp=1:1", "--node-port=1")
	match := regexp.MustCompile(`range of valid ports is (\d+)-(\d+)`).FindStringSubmatch(errOut)
	if len(match) != 3 {
		t.Fatalf("No NodePort range in: %s", errOut)
	}
	return l3Atoi(t, "nodePort range start", match[1]), l3Atoi(t, "nodePort range end", match[2])
}

func l3AllocatedNodePorts(t *testing.T) map[int]struct{} {
	t.Helper()
	allocated := map[int]struct{}{}
	for _, part := range strings.Split(l3Kubectl(t, "get", "service", "--all-namespaces", "-o",
		`jsonpath={range .items[*].spec.ports[*]}{.nodePort},{end}`), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		port, err := strconv.Atoi(part)
		if err != nil || port <= 0 {
			continue
		}
		allocated[port] = struct{}{}
	}
	return allocated
}

func l3Ordinal(t *testing.T, pod string) int {
	t.Helper()
	i := strings.LastIndex(pod, "-")
	if i < 0 {
		t.Fatalf("pod %q has no ordinal", pod)
	}
	return l3Atoi(t, "pod ordinal", pod[i+1:])
}

// Scale up to want pods if needed, restore original size on cleanup.
func (cluster l3Cluster) ensureSize(t *testing.T, want int) {
	t.Helper()
	size := cluster.size(t)
	if size >= want {
		return
	}
	t.Cleanup(func() { cluster.scale(t, size) })
	cluster.scale(t, want)
}

// Patch spec.size without waiting for the operator.
func (cluster l3Cluster) resize(t *testing.T, size int) {
	t.Helper()
	l3Kubectl(t, "-n", cluster.namespace, "patch", "aerospikecluster", cluster.name, "--type", "merge",
		"-p", fmt.Sprintf(`{"spec":{"size":%d}}`, size))
}

func (cluster l3Cluster) scale(t *testing.T, size int) {
	t.Helper()
	cluster.resize(t, size)
	var state string
	if !waitFor(l3ScaleTimeout, 2*time.Second, func() bool {
		pods := cluster.pods(t)
		state = cluster.get(t, ".status.size") + " " + cluster.get(t, ".status.phase") + " " + fmt.Sprint(pods)
		if cluster.get(t, ".status.size") != strconv.Itoa(size) || cluster.get(t, ".status.phase") != "Completed" {
			return false
		}
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
	t.Logf("L3-SCALED cluster=%s/%s size=%d state=%s", cluster.namespace, cluster.name, size, state)
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

func l3KubectlError(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), l3KubectlTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--request-timeout=30s"}, args...)...)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+os.Getenv("DISCOVERY_KUBECONFIG"))
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("kubectl %s succeeded", strings.Join(args, " "))
	}
	if ctx.Err() != nil {
		t.Fatalf("kubectl %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stderr.String())
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
