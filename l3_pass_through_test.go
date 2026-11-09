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
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	l3Set         = "l3-pass-through"
	l3RoutingWait = 120 * time.Second
	l3Skip        = "Kubernetes pass-through needs a kind cluster running the Aerospike Kubernetes Operator: run the discovery-k8s workflow (DISCOVERY_KUBECONFIG is not set)"
	l3SharedSkip  = "shared-endpoint pass-through needs multiPodPerHost: true with every node labelled with one endpoint hostname (the discovery-k8s workflow)"
	l3PerNodeSkip = "per-node pass-through needs multiPodPerHost: false with each kind worker labelled with its own endpoint hostname. The discovery-k8s workflow builds the shared-endpoint cluster"
)

// TestL3PassThroughSharedEndpoint is the operator's default topology: every
// worker has the same configured-alternate-access-address, so each pod is
// advertised as that hostname plus its own Service NodePort.
//
// The client seeds at the ingress with pass-through and UseServicesAlternate
// (no map). It must:
//   - hold exactly one node per pod
//   - keep non-seed nodes at hostname:NodePort, where NodePort is the pod
//     Service the API server reports (fails if the operator stops advertising it)
//   - own every partition of the namespace on those nodes, each node a master
//   - serve a MASTER write and read on each node
//   - reach the seeded node again at the address it advertises
func TestL3PassThroughSharedEndpoint(t *testing.T) {
	skipUnlessL3(t)
	cluster := l3FindCluster(t)
	if !cluster.multiPodPerHost(t) {
		t.Skip(l3SharedSkip)
	}
	hostname := strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
	if hostname == "" {
		t.Fatal("DISCOVERY_ENDPOINT_HOSTNAME is not set")
	}
	if got := cluster.get(t, ".spec.aerospikeNetworkPolicy.alternateAccess"); got != "configuredIP" {
		t.Fatalf("alternateAccess %q, want configuredIP", got)
	}

	cluster.ensureSize(t, l3MinPods)
	pods := cluster.pods(t)
	if len(pods) < l3MinPods {
		t.Fatalf("%d pods, want at least %d", len(pods), l3MinPods)
	}
	expected := l3SharedAdvertised(t, cluster, hostname, pods)
	l3AssertRouted(t, pods, expected)
}

// TestL3PassThroughPerNode is the per-node topology: each kind worker has its
// own configured-alternate-access-address and runs one pod, so each pod is
// advertised as its worker's hostname plus the pod's hostPort.
//
// It runs only on that cluster shape and skips on the shared-endpoint cluster
// the discovery-k8s workflow builds. The client must meet the same routing
// contract as the shared-endpoint test, at worker-hostname:hostPort.
func TestL3PassThroughPerNode(t *testing.T) {
	skipUnlessL3(t)
	cluster := l3FindCluster(t)
	if cluster.multiPodPerHost(t) {
		t.Skip(l3PerNodeSkip)
	}
	if got := cluster.get(t, ".spec.aerospikeNetworkPolicy.alternateAccess"); got != "configuredIP" {
		t.Fatalf("alternateAccess %q, want configuredIP", got)
	}
	pods := cluster.pods(t)
	if len(pods) < l3MinPods {
		t.Fatalf("%d pods, want at least %d", len(pods), l3MinPods)
	}
	expected := l3PerNodeAdvertised(t, cluster, pods)
	l3AssertRouted(t, pods, expected)
}

func skipUnlessL3(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_KUBECONFIG")) == "" {
		t.Skip(l3Skip)
	}
}

// l3SharedAdvertised is hostname:NodePort for each pod, taken from the API
// server. Every worker label must be the shared hostname, NodePorts must be
// unique, and the ports the workflow published must still be in that set.
func l3SharedAdvertised(t *testing.T, cluster l3Cluster, hostname string, pods []l3Pod) map[string]string {
	t.Helper()
	port := cluster.servicePort(t)
	expected := make(map[string]string, len(pods))
	nodePorts := make(map[int]string, len(pods))
	for _, pod := range pods {
		label := cluster.nodeLabel(t, pod.node)
		nodePort := cluster.nodePort(t, pod.name, port)
		t.Logf("pod %s label=%s serviceNodePort=%d", pod, label, nodePort)
		if label != hostname {
			t.Fatalf("worker %s label %q, want shared hostname %s", pod.node, label, hostname)
		}
		if nodePort <= 0 {
			t.Fatalf("pod %s has no Service NodePort for %d", pod.name, port)
		}
		if other, dup := nodePorts[nodePort]; dup {
			t.Fatalf("NodePort %d is on both %s and %s", nodePort, other, pod.name)
		}
		nodePorts[nodePort] = pod.name
		expected[pod.name] = net.JoinHostPort(hostname, strconv.Itoa(nodePort))
	}
	listed := l3CSVPorts(t, "DISCOVERY_SERVICE_NODE_PORTS")
	for _, published := range listed {
		if _, ok := nodePorts[published]; !ok {
			t.Fatalf("workflow NodePort %d is not a pod Service port %v", published, nodePorts)
		}
	}
	return expected
}

// l3PerNodeAdvertised is worker-label:hostPort for each pod. Every pod must sit
// on its own worker, and every worker must carry its own hostname label.
func l3PerNodeAdvertised(t *testing.T, cluster l3Cluster, pods []l3Pod) map[string]string {
	t.Helper()
	port := cluster.servicePort(t)
	expected := make(map[string]string, len(pods))
	workers := make(map[string]string, len(pods))
	labels := make(map[string]string, len(pods))
	for _, pod := range pods {
		label := cluster.nodeLabel(t, pod.node)
		hostPort := cluster.hostPort(t, pod.name, port)
		t.Logf("pod %s label=%s hostPort=%d", pod, label, hostPort)
		if label == "" {
			t.Fatalf("worker %s has no configured-alternate-access-address label", pod.node)
		}
		if hostPort <= 0 {
			t.Fatalf("pod %s has no hostPort for %d", pod.name, port)
		}
		if other, dup := workers[pod.node]; dup {
			t.Fatalf("worker %s runs both %s and %s, want one pod per worker", pod.node, other, pod.name)
		}
		if other, dup := labels[label]; dup {
			t.Fatalf("label %s is on both %s and %s, want one hostname per worker", label, other, pod.node)
		}
		workers[pod.node] = pod.name
		labels[label] = pod.node
		expected[pod.name] = net.JoinHostPort(label, strconv.Itoa(hostPort))
	}
	return expected
}

func l3AssertRouted(t *testing.T, pods []l3Pod, expected map[string]string) {
	t.Helper()
	logs := captureL2Logs(t)
	policy := l3Policy()
	seeds := l3Seeds(t)
	client := l3Client(t, policy, seeds...)

	var masters []*as.Node
	if !waitFor(l3RoutingWait, 500*time.Millisecond, func() bool {
		masters = as.PartitionMasters(client, l3Namespace())
		return len(client.GetNodes()) == len(pods) && l3Unowned(client, masters) == 0 && len(l3MasterNames(masters)) == len(pods)
	}) {
		masters = as.PartitionMasters(client, l3Namespace())
		t.Fatalf("after %s: nodes=%d want %d, unowned=%d, owners=%v",
			l3RoutingWait, len(client.GetNodes()), len(pods), l3Unowned(client, masters), l3MasterNames(masters))
	}

	l3AssertNodesMatchPods(t, client, seeds, pods, expected)
	l3AssertEachNodeServes(t, client, masters)
	l3AssertNoCollisions(t, logs)
}

func l3AssertNodesMatchPods(t *testing.T, client *as.Client, seeds []*as.Host, pods []l3Pod, expected map[string]string) {
	t.Helper()
	podsByIP := make(map[string]l3Pod, len(pods))
	for _, pod := range pods {
		podsByIP[pod.ip] = pod
	}
	matched := make(map[string]*as.Node, len(pods))
	seededAt := map[string]string{}
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "service-clear-std", "service-clear-alt")
		if err != nil {
			t.Fatalf("info %s: %v", node.GetName(), err)
		}
		pod, ok := l3PodByAccess(t, podsByIP, info["service-clear-std"])
		if !ok {
			t.Fatalf("node %s access %q matches no pod IP %v", node.GetName(), info["service-clear-std"], l3PodIPs(pods))
		}
		if other, dup := matched[pod.name]; dup {
			t.Fatalf("pod %s matched both %s and %s", pod.name, other.GetName(), node.GetName())
		}
		matched[pod.name] = node
		advertised := info["service-clear-alt"]
		if advertised != expected[pod.name] {
			t.Fatalf("node %s (%s) service-clear-alt %q, want %s",
				node.GetName(), pod.name, advertised, expected[pod.name])
		}
		host := node.GetHost()
		t.Logf("node %s pod=%s access=%s alternate=%s client=%s:%d",
			node.GetName(), pod.name, info["service-clear-std"], advertised, host.Name, host.Port)
		if l3HostIn(t, seeds, host) {
			seededAt[node.GetName()] = advertised
			continue
		}
		l3AssertHostIs(t, advertised, host)
	}
	for _, pod := range pods {
		if _, ok := matched[pod.name]; !ok {
			t.Fatalf("no client node for pod %s", pod.name)
		}
	}
	if len(client.GetNodes()) != len(pods) {
		t.Fatalf("client holds %d nodes, want %d pods", len(client.GetNodes()), len(pods))
	}
	for name, advertised := range seededAt {
		l3AssertReachableAt(t, name, advertised)
	}
}

func l3AssertEachNodeServes(t *testing.T, client *as.Client, masters []*as.Node) {
	t.Helper()
	cluster := client.Cluster()
	write := as.NewWritePolicy(0, 0)
	write.ReplicaPolicy = as.MASTER
	read := as.NewPolicy()
	read.ReplicaPolicy = as.MASTER
	ns := l3Namespace()
	for _, node := range client.GetNodes() {
		partition := -1
		for id, master := range masters {
			if master == node {
				partition = id
				break
			}
		}
		if partition < 0 {
			t.Fatalf("node %s is master of no partition", node.GetName())
		}
		key := l3KeyForPartition(t, ns, fmt.Sprintf("%s-%d", node.GetName(), partition), partition)
		l3AssertRoutedTo(t, cluster, write, read, key, node)
		if err := client.Put(write, key, as.BinMap{"v": node.GetName()}); err != nil {
			t.Fatalf("put %s partition %d: %v", node.GetName(), partition, err)
		}
		record, err := client.Get(read, key)
		if err != nil {
			t.Fatalf("get %s partition %d: %v", node.GetName(), partition, err)
		}
		if record.Bins["v"] != node.GetName() {
			t.Fatalf("get %s partition %d returned %v, want the node name", node.GetName(), partition, record.Bins["v"])
		}
	}
}

func l3AssertRoutedTo(t *testing.T, cluster *as.Cluster, write *as.WritePolicy, read *as.BasePolicy, key *as.Key, want *as.Node) {
	t.Helper()
	wpart, err := as.PartitionForWrite(cluster, &write.BasePolicy, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := wpart.GetNodeWrite(cluster)
	if err != nil || got != want {
		t.Fatalf("write of %v routes to %v (%v), want %s", key, got, err, want.GetName())
	}
	rpart, err := as.PartitionForRead(cluster, read, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err = rpart.GetNodeRead(cluster)
	if err != nil || got != want {
		t.Fatalf("read of %v routes to %v (%v), want %s", key, got, err, want.GetName())
	}
}

func l3AssertReachableAt(t *testing.T, name, advertised string) {
	t.Helper()
	client := l3Client(t, l3Policy(), l3ParseHost(t, advertised))
	node, err := client.Cluster().GetNodeByName(name)
	if err != nil {
		t.Fatalf("second client seeded at %s did not find %s: %v", advertised, name, err)
	}
	l3AssertHostIs(t, advertised, node.GetHost())
	if !node.IsActive() {
		t.Fatalf("%s at %s is not active", name, advertised)
	}
}

// l3AssertNoCollisions fails when two pods were translated to one address,
// which would mean the operator advertised the same hostname:port twice.
func l3AssertNoCollisions(t *testing.T, logs *l2LogCapture) {
	t.Helper()
	if got := logs.containing("to the same address"); len(got) > 0 {
		t.Fatalf("pass-through collided: %v", got)
	}
}

func l3Unowned(client *as.Client, masters []*as.Node) int {
	unowned := 0
	held := client.Cluster()
	for _, node := range masters {
		if node == nil || !node.IsActive() {
			unowned++
			continue
		}
		same, err := held.GetNodeByName(node.GetName())
		if err != nil || same != node {
			unowned++
		}
	}
	return unowned
}

func l3MasterNames(masters []*as.Node) map[string]int {
	owners := map[string]int{}
	for _, node := range masters {
		if node != nil && node.IsActive() {
			owners[node.GetName()]++
		}
	}
	return owners
}

func l3PodByAccess(t *testing.T, podsByIP map[string]l3Pod, access string) (l3Pod, bool) {
	t.Helper()
	for _, host := range l3ServiceHosts(t, access) {
		if pod, ok := podsByIP[host.Name]; ok {
			return pod, true
		}
	}
	return l3Pod{}, false
}

func l3ServiceHosts(t *testing.T, access string) []*as.Host {
	t.Helper()
	if strings.TrimSpace(access) == "" {
		return nil
	}
	var hosts []*as.Host
	for _, part := range strings.Split(access, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parsed, err := as.NewHosts(part)
		if err != nil {
			t.Fatalf("service address %q: %v", part, err)
		}
		hosts = append(hosts, parsed...)
	}
	return hosts
}

func l3AssertHostIs(t *testing.T, advertised string, got *as.Host) {
	t.Helper()
	want := l3ParseHost(t, advertised)
	if !l3HostIn(t, []*as.Host{want}, got) {
		t.Fatalf("client host %s:%d is not advertised %s (resolved %v)",
			got.Name, got.Port, advertised, l3Lookup(t, want.Name))
	}
}

func l3HostIn(t *testing.T, hosts []*as.Host, got *as.Host) bool {
	t.Helper()
	for _, host := range hosts {
		if host.Port == got.Port && (host.Name == got.Name || slices.Contains(l3Lookup(t, host.Name), got.Name)) {
			return true
		}
	}
	return false
}

func l3Lookup(t *testing.T, name string) []string {
	t.Helper()
	addrs, err := net.LookupHost(name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return addrs
}

func l3ParseHost(t *testing.T, advertised string) *as.Host {
	t.Helper()
	hosts, err := as.NewHosts(advertised)
	if err != nil || len(hosts) != 1 {
		t.Fatalf("advertised %q: %v %v", advertised, hosts, err)
	}
	return hosts[0]
}

func l3KeyForPartition(t *testing.T, ns, user string, partition int) *as.Key {
	t.Helper()
	digest := make([]byte, 20)
	digest[0] = byte(partition)
	digest[1] = byte(partition >> 8)
	key, err := as.NewKeyWithDigest(ns, l3Set, user, digest)
	if err != nil {
		t.Fatal(err)
	}
	if key.PartitionId() != partition {
		t.Fatalf("key digest maps to partition %d, want %d", key.PartitionId(), partition)
	}
	return key
}

func l3Policy() *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.User = "admin"
	policy.Password = os.Getenv("AEROSPIKE_ADMIN_PASSWORD")
	policy.UseServicesAlternate = true
	policy.AddressTranslator = as.NewPassThroughAddressTranslator()
	return policy
}

func l3Seeds(t *testing.T) []*as.Host {
	t.Helper()
	hostname := strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
	if hostname == "" {
		t.Fatal("DISCOVERY_ENDPOINT_HOSTNAME is not set")
	}
	ports := l3CSVPorts(t, "DISCOVERY_INGRESS_PORTS")
	return []*as.Host{as.NewHost(hostname, ports[0])}
}

func l3Client(t *testing.T, policy *as.ClientPolicy, seeds ...*as.Host) *as.Client {
	t.Helper()
	client, err := as.NewClientWithPolicyAndHost(policy, seeds...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func l3Namespace() string {
	if namespace != nil && *namespace != "" {
		return *namespace
	}
	return "test"
}

func l3PodIPs(pods []l3Pod) []string {
	ips := make([]string, 0, len(pods))
	for _, pod := range pods {
		ips = append(ips, pod.ip)
	}
	return ips
}
