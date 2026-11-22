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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	l3ScaleSet            = "l3-scale"
	l3LoadThreads         = 4
	l3LoadKeys            = 1000
	l3QuietTendIntervals  = 5
	l3ScaleNoKubeconfig   = "Kubernetes scale needs a kind cluster running the Aerospike Kubernetes Operator: run test/discovery/run_l3.sh or the discovery-k8s workflow (DISCOVERY_KUBECONFIG is not set)"
	l3ScaleSharedEndpoint = "Kubernetes scale needs every pod advertising one endpoint hostname: configuredIP with multiPodPerHost: true, or networkLoadBalancer (run_l3.sh default profile, or the discovery-k8s workflow)"
	l3ScaleNLB            = "needs alternateAccess networkLoadBalancer (the l3-nlb or l3-nlb-per-pod cluster of the discovery-k8s workflow)"
	l3NodeMark            = "l3-discovery-mark"
)

// Shared-endpoint cluster plus extra Services, restored on cleanup.
type l3Scale struct {
	cluster       l3Cluster
	foundSize     int
	hostname      string
	pinnedService string
	heldService   string
}

func l3RequireScaleCluster(t *testing.T) *l3Scale {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_KUBECONFIG")) == "" {
		t.Skip(l3ScaleNoKubeconfig)
	}
	cluster := l3FindCluster(t)
	if !cluster.networkLoadBalancer(t) && !cluster.multiPodPerHost(t) {
		t.Fatal(l3ScaleSharedEndpoint)
	}
	hostname := l3EndpointHostname(t)
	l3RequireSharedEndpoint(t, cluster, hostname)
	state := &l3Scale{cluster: cluster, foundSize: cluster.size(t), hostname: hostname}
	t.Cleanup(func() { state.restore(t) })
	return state
}

func (state *l3Scale) restore(t *testing.T) {
	t.Helper()
	if state.heldService != "" {
		state.cluster.deleteService(t, state.heldService)
		state.heldService = ""
	}
	if state.pinnedService != "" {
		state.cluster.deleteService(t, state.pinnedService)
		t.Logf("L3-PINNED-DELETED service=%s", state.pinnedService)
		state.pinnedService = ""
	}
	if state.cluster.size(t) != state.foundSize || len(state.cluster.pods(t)) != state.foundSize {
		state.cluster.scale(t, state.foundSize)
	}
}

// Scales to at least want pods, restored on cleanup, and returns the size.
func (state *l3Scale) sizeAtLeast(t *testing.T, want int) int {
	t.Helper()
	if state.foundSize < want {
		state.cluster.scale(t, want)
	}
	return state.cluster.size(t)
}

// Under load, the removed pod leaves the node list and partition map.
// Errors and retries stay flat for 5 tend intervals while commands succeed.
func TestL3ScaleDownRemovesNodeWithoutRetryStorm(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	size := state.sizeAtLeast(t, l3MinPods)
	pods := cluster.pods(t)
	removed := l3HighestOrdinal(t, pods)
	policy := l3Policy()
	client := l3Client(t, policy, l3Seeds(t)...)
	l3AwaitRouting(t, "scale-down-before", client, size)

	victim := l3NodeOf(t, client, removed)
	if victim == nil {
		t.Fatalf("No node for %s", removed)
	}
	victimName := victim.GetName()
	t.Logf("L3-SCALE-DOWN-VICTIM node=%s pod=%s", victimName, removed)

	load := newL3Load(client)
	t.Cleanup(func() { load.close(t) })
	if !waitFor(l3RoutingWait, 10*time.Millisecond, func() bool { return load.succeeded.Load() > 0 }) {
		t.Fatal("Load did not start")
	}

	failedBefore := load.failed.Load()
	start := time.Now()
	cluster.scale(t, size-1)

	ns := l3Namespace()
	var masters []*as.Node
	gone := waitFor(l3RoutingWait, 50*time.Millisecond, func() bool {
		masters = as.PartitionMasters(client, ns)
		return !l3HasNodeName(client, victimName) && !as.PartitionContains(client, ns, victimName) &&
			len(client.GetNodes()) == size-1 && l3Unowned(client, masters) == 0
	})
	transition := time.Since(start)
	masters = as.PartitionMasters(client, ns)
	t.Logf("L3-SCALE-DOWN-TRANSITION node=%s pod=%s transitionMs=%d failedCommands=%d succeededCommands=%d lastError=%s nodes=%v owners=%v",
		victimName, removed.name, transition.Milliseconds(), load.failed.Load()-failedBefore, load.succeeded.Load(),
		load.lastError(), client.GetNodeNames(), l3MasterNames(masters))
	if !gone {
		t.Fatalf("%s still in the node list or partition map, or a partition unowned, %s after the scale down: nodes=%v",
			victimName, l3RoutingWait, client.GetNodeNames())
	}

	remaining := l3PodNameSet(cluster.pods(t))
	for _, node := range client.GetNodes() {
		if !node.IsActive() {
			t.Fatalf("%s is not active", node.GetName())
		}
		pod, ok := l3PodOf(t, cluster.pods(t), node)
		if !ok {
			t.Fatalf("%s is no remaining pod", node.GetName())
		}
		if _, ok := remaining[pod.name]; !ok {
			t.Fatalf("%s not in %v", pod.name, remaining)
		}
	}
	if _, ok := l3MasterNames(masters)[victimName]; ok {
		t.Fatalf("owners still include %s", victimName)
	}

	errors, retries := as.ClusterCommandCounts(client)
	failed := load.failed.Load()
	succeeded := load.succeeded.Load()
	window := time.Duration(l3QuietTendIntervals) * policy.TendInterval
	grew := waitFor(window, 50*time.Millisecond, func() bool {
		gotErrors, gotRetries := as.ClusterCommandCounts(client)
		return gotErrors != errors || gotRetries != retries || load.failed.Load() != failed
	})
	gotErrors, gotRetries := as.ClusterCommandCounts(client)
	t.Logf("L3-SCALE-DOWN-QUIET windowMs=%d errors=%d->%d retries=%d->%d failed=%d->%d succeeded=%d->%d lastError=%s",
		window.Milliseconds(), errors, gotErrors, retries, gotRetries, failed, load.failed.Load(),
		succeeded, load.succeeded.Load(), load.lastError())
	if grew {
		t.Fatalf("Errors, retries or failed commands grew after %s left", victimName)
	}
	if load.succeeded.Load() <= succeeded {
		t.Fatalf("No command succeeded after %s left", victimName)
	}
	load.close(t)
	remainingPods := cluster.pods(t)
	l3AssertNodesMatchPods(t, client, l3Seeds(t), remainingPods, l3HostnameNodePorts(t, cluster, state.hostname, remainingPods))
	l3AssertEachNodeServes(t, client, as.PartitionMasters(client, ns))
}

// Pod Service NodePort survives reschedule, a new one is allocated on recreate,
// and a pinned seed Service keeps its port and reaches every node.
func TestL3NodePortsStableAcrossPodRescheduleAndRecreate(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	size := state.sizeAtLeast(t, l3MinPods)
	port := cluster.servicePort(t)
	pods := cluster.pods(t)
	first := pods[0]
	for _, pod := range pods {
		if l3Ordinal(t, pod.name) < l3Ordinal(t, first.name) {
			first = pod
		}
	}
	if l3Ordinal(t, first.name) != 0 {
		t.Fatalf("%s ordinal %d, want 0", first.name, l3Ordinal(t, first.name))
	}

	lo, hi := cluster.nodePortRange(t)
	allocated := l3AllocatedNodePorts(t)
	pinned := 0
	for p := hi; p >= lo && pinned == 0; p-- {
		if _, used := allocated[p]; !used {
			pinned = p
		}
	}
	if pinned <= 0 {
		t.Fatalf("No free NodePort in %d-%d", lo, hi)
	}

	service := cluster.name + "-pinned-seed"
	state.pinnedService = service
	cluster.createPodNodePortService(t, service, first.name, port, pinned)
	t.Logf("L3-PINNED service=%s pod=%s nodePort=%d range=%d-%d allocated=%d",
		service, first.name, pinned, lo, hi, len(allocated))
	if got := cluster.nodePort(t, service, port); got != pinned {
		t.Fatalf("pinned Service NodePort %d, want %d", got, pinned)
	}
	l3AssertPinnedSeed(t, state, "created", service, port, pinned, size)

	podUID := cluster.uid(t, "pod", first.name)
	serviceUID := cluster.uid(t, "service", first.name)
	nodePort := cluster.nodePort(t, first.name, port)
	cluster.deletePod(t, first.name)
	name := first.name
	rescheduled := waitFor(l3ScaleTimeout, 2*time.Second, func() bool {
		uid := cluster.uid(t, "pod", name)
		if uid == "" || uid == podUID {
			return false
		}
		for _, pod := range cluster.pods(t) {
			if pod.name == name {
				return pod.phase == "Running"
			}
		}
		return false
	})
	if !rescheduled {
		t.Fatalf("%s not Running again within %s", name, l3ScaleTimeout)
	}
	cluster.scale(t, size)
	t.Logf("L3-POD-RESCHEDULED pod=%s podUid=%s->%s serviceUid=%s->%s nodePort=%d->%d",
		name, podUID, cluster.uid(t, "pod", name), serviceUID, cluster.uid(t, "service", name),
		nodePort, cluster.nodePort(t, name, port))
	if got := cluster.uid(t, "service", name); got != serviceUID {
		t.Fatalf("%s Service UID %s, want %s", name, got, serviceUID)
	}
	if got := cluster.nodePort(t, name, port); got != nodePort {
		t.Fatalf("%s Service NodePort %d, want %d", name, got, nodePort)
	}
	if got := cluster.nodePort(t, service, port); got != pinned {
		t.Fatalf("pinned Service NodePort %d, want %d", got, pinned)
	}
	l3AssertPinnedSeed(t, state, "rescheduled", service, port, pinned, size)

	last := l3HighestOrdinal(t, cluster.pods(t))
	lastUID := cluster.uid(t, "service", last.name)
	lastNodePort := cluster.nodePort(t, last.name, port)
	cluster.scale(t, size-1)
	lastName := last.name
	if !waitFor(l3ScaleTimeout, time.Second, func() bool { return cluster.uid(t, "service", lastName) == "" }) {
		t.Fatalf("%s Service not deleted within %s", lastName, l3ScaleTimeout)
	}

	held := cluster.name + "-held-node-port"
	state.heldService = held
	cluster.createPodNodePortService(t, held, last.name, port, lastNodePort)
	cluster.scale(t, size)
	recreatedUID := cluster.uid(t, "service", last.name)
	recreatedNodePort := cluster.nodePort(t, last.name, port)
	cluster.deleteService(t, held)
	state.heldService = ""
	t.Logf("L3-POD-RECREATED pod=%s serviceUid=%s->(deleted)->%s oldNodePort=%d newNodePort=%d",
		last.name, lastUID, recreatedUID, lastNodePort, recreatedNodePort)
	if recreatedUID == "" {
		t.Fatalf("%s has no Service after it was recreated", last.name)
	}
	if recreatedUID == lastUID {
		t.Fatalf("%s Service not recreated", last.name)
	}
	if recreatedNodePort <= 0 {
		t.Fatalf("%s has no Service NodePort for %d", last.name, port)
	}
	if recreatedNodePort == lastNodePort {
		t.Fatalf("%s recreated Service kept NodePort %d", last.name, lastNodePort)
	}
	if got := cluster.nodePort(t, service, port); got != pinned {
		t.Fatalf("pinned Service NodePort %d, want %d", got, pinned)
	}
	l3AssertPinnedSeed(t, state, "recreated", service, port, pinned, size)
}

// networkLoadBalancer: the pod its StatefulSet recreates advertises the same
// address and Service NodePort, and the client seeded once routes every
// partition again.
func TestL3AdvertisedAddressSurvivesPodReschedule(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	if !cluster.networkLoadBalancer(t) {
		t.Skip("Pod reschedule " + l3ScaleNLB)
	}
	size := state.sizeAtLeast(t, l3MinPods)
	port := cluster.servicePort(t)
	victim := l3HighestOrdinal(t, cluster.pods(t))
	podUID := cluster.uid(t, "pod", victim.name)
	nodePort := cluster.nodePort(t, victim.name, port)
	if nodePort <= 0 {
		t.Fatalf("%s has no Service NodePort for %d", victim.name, port)
	}
	want := net.JoinHostPort(state.hostname, strconv.Itoa(nodePort))
	client := l3Client(t, l3Policy(), l3Seeds(t)...)
	l3AwaitRouting(t, "reschedule-before", client, size)
	before := l3NodeOf(t, client, victim)
	if before == nil {
		t.Fatalf("No node for %s", victim)
	}
	if got := l3Info(t, before, "service-clear-alt"); got != want {
		t.Fatalf("%s advertised alternate address %q before the reschedule, want %s", victim.name, got, want)
	}

	cluster.deletePod(t, victim.name)
	pod := cluster.awaitReplaced(t, victim.name, podUID)
	node := l3AwaitNodeOf(t, client, pod)
	l3AwaitRouting(t, "reschedule-after", client, size)

	advertised := l3Info(t, node, "service-clear-alt")
	after := cluster.nodePort(t, pod.name, port)
	host := node.GetHost()
	t.Logf("L3-RESCHEDULED pod=%s podUid=%s->%s worker=%s->%s ip=%s->%s node=%s->%s alternate=%s serviceNodePort=%d->%d clientHost=%s",
		pod.name, podUID, cluster.uid(t, "pod", pod.name), victim.node, pod.node, victim.ip, pod.ip,
		before.GetName(), node.GetName(), advertised, nodePort, after, net.JoinHostPort(host.Name, strconv.Itoa(host.Port)))
	if after != nodePort {
		t.Fatalf("%s Service NodePort %d after the reschedule, want %d", pod.name, after, nodePort)
	}
	if advertised != want {
		t.Fatalf("%s (%s) advertised alternate address %q after the reschedule, want %s",
			node.GetName(), pod.name, advertised, want)
	}
	l3AssertHostIs(t, want, host)
	l3AssertEachNodeServes(t, client, as.PartitionMasters(client, l3Namespace()))
}

// networkLoadBalancer: a pod on a Node deleted and registered again, so without
// the labels set through the API, advertises the AerospikeCluster address with
// its Service NodePort, and the client seeded once routes every partition.
// kind does not register a deleted Node again; reregister-kind-nodes.sh does.
func TestL3PodOnRecreatedNodeAdvertisesClusterAddress(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	if !cluster.networkLoadBalancer(t) {
		t.Skip("Recreated Node " + l3ScaleNLB)
	}
	size := state.sizeAtLeast(t, l3MinPods)
	port := cluster.servicePort(t)
	// Deleting the Node the endpoint resolves to takes the seed and every pod
	// with their work directories, which the operator does not recover from.
	victim, ok := l3AwayFromEndpoint(t, state.hostname, cluster.pods(t))
	if !ok {
		t.Skip("Recreated Node needs a pod on a worker " + state.hostname +
			" does not resolve to (the l3-nlb-per-pod cluster of the discovery-k8s workflow)")
	}
	worker := victim.node
	podUID := cluster.uid(t, "pod", victim.name)
	l3LabelNode(t, worker, l3NodeMark, "set-through-api")
	if got := l3NodeLabel(t, worker, l3NodeMark); got != "set-through-api" {
		t.Fatalf("%s %s %q, want set-through-api", worker, l3NodeMark, got)
	}
	client := l3Client(t, l3Policy(), l3Seeds(t)...)
	l3AwaitRouting(t, "node-before", client, size)

	cluster.recreateNode(t, worker, victim.name)
	pod := cluster.awaitReplaced(t, victim.name, podUID)
	if !waitFor(l3ScaleTimeout, 2*time.Second, func() bool { return len(cluster.readyPods(t)) == size }) {
		t.Fatalf("Not every pod Ready within %s: %v", l3ScaleTimeout, cluster.pods(t))
	}
	node := l3AwaitNodeOf(t, client, pod)
	l3AwaitRouting(t, "node-after", client, size)

	nodePort := cluster.nodePort(t, pod.name, port)
	advertised := l3Info(t, node, "service-clear-alt")
	mark := l3NodeLabel(t, worker, l3NodeMark)
	label := l3NodeLabel(t, worker, l3AlternateAccessLabel)
	t.Logf("L3-NODE-RECREATED worker=%s mark=%s label=%s pod=%s podUid=%s->%s podWorker=%s node=%s alternate=%s serviceNodePort=%d",
		worker, mark, label, pod.name, podUID, cluster.uid(t, "pod", pod.name), pod.node, node.GetName(), advertised, nodePort)
	if pod.node != worker {
		t.Fatalf("%s runs on %s, want the recreated %s", pod.name, pod.node, worker)
	}
	if mark != "" {
		t.Fatalf("%s %s %q after it registered again, want none", worker, l3NodeMark, mark)
	}
	if label != "" {
		t.Fatalf("%s %s %q, want none", worker, l3AlternateAccessLabel, label)
	}
	if nodePort <= 0 {
		t.Fatalf("%s has no Service NodePort for %d", pod.name, port)
	}
	want := net.JoinHostPort(state.hostname, strconv.Itoa(nodePort))
	if advertised != want {
		t.Fatalf("%s (%s) advertised alternate address %q, want %s", node.GetName(), pod.name, advertised, want)
	}
	l3AssertHostIs(t, want, node.GetHost())
	l3AssertEachNodeServes(t, client, as.PartitionMasters(client, l3Namespace()))
}

// Highest-ordinal pod on a worker that is not the control plane and that
// hostname does not resolve to.
func l3AwayFromEndpoint(t *testing.T, hostname string, pods []l3Pod) (l3Pod, bool) {
	t.Helper()
	endpoint := l3Lookup(t, hostname)
	var away []l3Pod
	for _, pod := range pods {
		if !slices.Contains(endpoint, l3NodeAddress(t, pod.node)) && !l3ControlPlane(t, pod.node) {
			away = append(away, pod)
		}
	}
	if len(away) == 0 {
		return l3Pod{}, false
	}
	return l3HighestOrdinal(t, away), true
}

// The node held before the pod was replaced stays for a few tends while its
// connections fail, so info errors are expected until the new one answers.
func l3AwaitNodeOf(t *testing.T, client *as.Client, pod l3Pod) *as.Node {
	t.Helper()
	var found *as.Node
	if !waitFor(l3RoutingWait, 500*time.Millisecond, func() bool {
		for _, node := range client.GetNodes() {
			info, err := node.RequestInfo(as.NewInfoPolicy(), "service-clear-std")
			if err == nil && l3HasAddress(t, info["service-clear-std"], pod.ip) {
				found = node
				return true
			}
		}
		return false
	}) {
		t.Fatalf("No node answers from %s within %s: %v", pod, l3RoutingWait, client.GetNodeNames())
	}
	return found
}

func l3Info(t *testing.T, node *as.Node, name string) string {
	t.Helper()
	info, err := node.RequestInfo(as.NewInfoPolicy(), name)
	if err != nil {
		t.Fatalf("info %s %s: %v", node.GetName(), name, err)
	}
	return info[name]
}

func l3AssertPinnedSeed(t *testing.T, state *l3Scale, step, service string, port, pinned, size int) {
	t.Helper()
	seed := as.NewHost(state.hostname, pinned)
	if !waitFor(l3RoutingWait, 500*time.Millisecond, func() bool {
		return state.cluster.readyEndpoints(t, service) > 0
	}) {
		t.Fatalf("%s has no ready endpoint within %s", service, l3RoutingWait)
	}
	client := l3Connect(t, l3Policy(), seed)
	l3AwaitRouting(t, "pinned-"+step, client, size)
	pods := state.cluster.pods(t)
	for _, node := range client.GetNodes() {
		pod, ok := l3PodOf(t, pods, node)
		if !ok {
			t.Fatalf("%s is no pod in %v", node.GetName(), pods)
		}
		advertised := l3Info(t, node, "service-clear-alt")
		nodePort := state.cluster.nodePort(t, pod.name, port)
		host := node.GetHost()
		t.Logf("L3-PINNED-NODE step=%s node=%s pod=%s alternate=%s serviceNodePort=%d pinned=%d clientHost=%s",
			step, node.GetName(), pod.name, advertised, nodePort, pinned, net.JoinHostPort(host.Name, strconv.Itoa(host.Port)))
		want := net.JoinHostPort(state.hostname, strconv.Itoa(nodePort))
		if advertised != want {
			t.Fatalf("%s (%s) advertised alternate address %q, want %s", node.GetName(), pod.name, advertised, want)
		}
		if nodePort == pinned {
			t.Fatalf("%s advertises the pinned NodePort of %s", pod.name, service)
		}
		if !l3HostIn(t, []*as.Host{seed}, host) {
			l3AssertHostIs(t, advertised, host)
		}
	}
	l3AssertEachNodeServes(t, client, as.PartitionMasters(client, l3Namespace()))
}

func l3Connect(t *testing.T, policy *as.ClientPolicy, seed *as.Host) *as.Client {
	t.Helper()
	var last as.Error
	var client *as.Client
	ok := waitFor(l3RoutingWait, time.Second, func() bool {
		c, err := as.NewClientWithPolicyAndHost(policy, seed)
		if err == nil {
			client = c
			return true
		}
		if err.Matches(types.TIMEOUT, types.NETWORK_ERROR, types.SERVER_NOT_AVAILABLE, types.INVALID_NODE_ERROR, types.MAX_RETRIES_EXCEEDED) {
			last = err
			return false
		}
		t.Fatalf("connect %s:%d: %v", seed.Name, seed.Port, err)
		return false
	})
	if !ok || client == nil {
		t.Fatalf("seed %s:%d: %v", seed.Name, seed.Port, last)
	}
	t.Cleanup(client.Close)
	return client
}

func l3AwaitRouting(t *testing.T, step string, client *as.Client, size int) {
	t.Helper()
	ns := l3Namespace()
	var masters []*as.Node
	routed := waitFor(l3RoutingWait, 50*time.Millisecond, func() bool {
		masters = as.PartitionMasters(client, ns)
		return len(client.GetNodes()) == size && l3Unowned(client, masters) == 0 && len(l3MasterNames(masters)) == size
	})
	masters = as.PartitionMasters(client, ns)
	t.Logf("L3-ROUTING step=%s nodes=%v unowned=%d owners=%v",
		step, client.GetNodeNames(), l3Unowned(client, masters), l3MasterNames(masters))
	if !routed {
		t.Fatalf("Client did not hold %d nodes owning every partition within %s: %v",
			size, l3RoutingWait, client.GetNodeNames())
	}
}

func l3HasAddress(t *testing.T, access, ip string) bool {
	t.Helper()
	for _, host := range l3ServiceHosts(t, access) {
		if host.Name == ip {
			return true
		}
	}
	return false
}

func l3PodOf(t *testing.T, pods []l3Pod, node *as.Node) (l3Pod, bool) {
	t.Helper()
	access := l3Info(t, node, "service-clear-std")
	for _, pod := range pods {
		if l3HasAddress(t, access, pod.ip) {
			return pod, true
		}
	}
	return l3Pod{}, false
}

func l3NodeOf(t *testing.T, client *as.Client, pod l3Pod) *as.Node {
	t.Helper()
	for _, node := range client.GetNodes() {
		if l3HasAddress(t, l3Info(t, node, "service-clear-std"), pod.ip) {
			return node
		}
	}
	return nil
}

func l3HasNodeName(client *as.Client, name string) bool {
	for _, n := range client.GetNodeNames() {
		if n == name {
			return true
		}
	}
	return false
}

func l3PodNameSet(pods []l3Pod) map[string]struct{} {
	names := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		names[pod.name] = struct{}{}
	}
	return names
}

func l3HighestOrdinal(t *testing.T, pods []l3Pod) l3Pod {
	t.Helper()
	last := pods[0]
	for _, pod := range pods {
		if l3Ordinal(t, pod.name) > l3Ordinal(t, last.name) {
			last = pod
		}
	}
	return last
}

type l3Load struct {
	succeeded atomic.Int64
	failed    atomic.Int64
	err       atomic.Value
	stop      atomic.Bool
	wg        sync.WaitGroup
}

func newL3Load(client *as.Client) *l3Load {
	load := &l3Load{}
	ns := l3Namespace()
	for thread := 0; thread < l3LoadThreads; thread++ {
		load.wg.Add(1)
		go func(thread int) {
			defer load.wg.Done()
			for i := 0; !load.stop.Load(); i++ {
				// Get reads the key the previous Put wrote: the client counts a miss as a transaction error.
				key, err := as.NewKey(ns, l3ScaleSet, fmt.Sprintf("load-%d", (thread+(i/2)*l3LoadThreads)%l3LoadKeys))
				if err != nil {
					load.failed.Add(1)
					load.err.Store(err.Error())
					continue
				}
				var cmdErr as.Error
				if i%2 == 0 {
					cmdErr = client.Put(nil, key, as.BinMap{"v": i})
				} else {
					_, cmdErr = client.Get(nil, key)
					if cmdErr != nil && cmdErr.Matches(types.KEY_NOT_FOUND_ERROR) {
						cmdErr = nil
					}
				}
				if cmdErr != nil {
					load.failed.Add(1)
					load.err.Store(cmdErr.Error())
					continue
				}
				load.succeeded.Add(1)
			}
		}(thread)
	}
	return load
}

func (load *l3Load) lastError() string {
	if v, ok := load.err.Load().(string); ok {
		return v
	}
	return ""
}

func (load *l3Load) close(t *testing.T) {
	t.Helper()
	if load.stop.Swap(true) {
		return
	}
	done := make(chan struct{})
	go func() {
		load.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(l3RoutingWait):
		t.Errorf("load threads did not stop within %s", l3RoutingWait)
	}
}
