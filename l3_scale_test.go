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
	l3ScaleSharedEndpoint = "Kubernetes scale needs multiPodPerHost: true with every node labelled with one endpoint hostname, the cluster setup-kind-ako builds (run_l3.sh default profile, or the discovery-k8s workflow)"
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
		t.Fatal(l3ScaleNoKubeconfig)
	}
	cluster := l3FindCluster(t)
	if !cluster.multiPodPerHost(t) {
		t.Fatal(l3ScaleSharedEndpoint)
	}
	hostname := strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
	if hostname == "" {
		t.Fatal("DISCOVERY_ENDPOINT_HOSTNAME is not set")
	}
	if got := cluster.get(t, ".spec.aerospikeNetworkPolicy.alternateAccess"); got != "configuredIP" {
		t.Fatalf("alternateAccess %q, want configuredIP", got)
	}
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

// Live client: the pod the operator adds is active within 2*tendInterval of
// the peers-generation bump, at hostname:NodePort, with every partition owned.
func TestL3ScaleUpNewNodeActiveWithinTwoTendIntervals(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	size := state.foundSize
	port := cluster.servicePort(t)
	before := l3PodNameSet(cluster.pods(t))
	policy := l3Policy()
	if policy.SeedProvider != nil {
		t.Fatal("SeedProvider is set")
	}
	client := l3Client(t, policy, l3Seeds(t)...)
	l3AwaitRouting(t, "scale-up-before", client, size)

	existing := map[string]*as.Node{}
	for _, node := range client.GetNodes() {
		existing[node.GetName()] = node
	}
	observer := client.GetNodes()[0]
	generation := l3PeersGeneration(t, observer)
	var bump, active time.Time
	var added *as.Node
	cluster.resize(t, size+1)
	seen := waitFor(l3ScaleTimeout, 5*time.Millisecond, func() bool {
		now := time.Now()
		if bump.IsZero() && l3PeersGeneration(t, observer) != generation {
			bump = now
		}
		if added == nil {
			for _, node := range client.GetNodes() {
				if node.IsActive() && existing[node.GetName()] == nil {
					active = now
					added = node
				}
			}
		}
		return !bump.IsZero() && added != nil
	})
	if !seen {
		t.Fatalf("No peers-generation bump on %s or no new active node", observer.GetName())
	}
	if active.Before(bump) {
		t.Fatalf("%s active before the peers-generation bump", added.GetName())
	}
	elapsed := active.Sub(bump)
	budget := 2 * policy.TendInterval
	cluster.scale(t, size+1)

	var pod *l3Pod
	for _, p := range cluster.pods(t) {
		if _, ok := before[p.name]; !ok {
			found := p
			pod = &found
		}
	}
	if pod == nil {
		t.Fatalf("No new pod in %v", cluster.pods(t))
	}
	nodePort := cluster.nodePort(t, pod.name, port)
	info, err := added.RequestInfo(as.NewInfoPolicy(), "service-clear-std", "service-clear-alt")
	if err != nil {
		t.Fatalf("info %s: %v", added.GetName(), err)
	}
	host := added.GetHost()
	t.Logf("L3-SCALE-UP node=%s pod=%s access=%s alternate=%s serviceNodePort=%d clientHost=%s generation=%d->%d bumpToActiveMs=%d tendInterval=%s budgetMs=%d bumpAt=%s podReadyAt=%s readinessProbe=%s publishNotReadyAddresses=%s",
		added.GetName(), pod.name, info["service-clear-std"], info["service-clear-alt"], nodePort,
		net.JoinHostPort(host.Name, strconv.Itoa(host.Port)), generation, l3PeersGeneration(t, observer),
		elapsed.Milliseconds(), policy.TendInterval, budget.Milliseconds(), bump.UTC().Format(time.RFC3339Nano),
		cluster.podReadyAt(t, pod.name), cluster.readinessProbe(t, pod.name), cluster.publishNotReadyAddresses(t, pod.name))
	if nodePort <= 0 {
		t.Fatalf("%s has no Service NodePort for %d", pod.name, port)
	}
	if !l3HasAddress(t, info["service-clear-std"], pod.ip) {
		t.Fatalf("%s access %s is not %s", added.GetName(), info["service-clear-std"], pod)
	}
	wantAlt := net.JoinHostPort(state.hostname, strconv.Itoa(nodePort))
	if info["service-clear-alt"] != wantAlt {
		t.Fatalf("%s (%s) advertised alternate address %q, want %s",
			added.GetName(), pod.name, info["service-clear-alt"], wantAlt)
	}
	l3AssertHostIs(t, wantAlt, host)
	if elapsed > budget {
		t.Fatalf("%s active %s after the bump, budget %s", added.GetName(), elapsed, budget)
	}

	l3AwaitRouting(t, "scale-up-after", client, size+1)
	for name, node := range existing {
		got, err := client.Cluster().GetNodeByName(name)
		if err != nil || got != node {
			t.Fatalf("%s replaced: %v %v", name, got, err)
		}
	}
	pods := cluster.pods(t)
	l3AssertNodesMatchPods(t, client, l3Seeds(t), pods, l3SharedAdvertised(t, cluster, state.hostname, pods))
	l3AssertEachNodeServes(t, client, as.PartitionMasters(client, l3Namespace()))
}

// Under load, the removed pod leaves the node list and partition map.
// Errors and retries stay flat for 5 tend intervals while commands succeed.
func TestL3ScaleDownRemovesNodeWithoutRetryStorm(t *testing.T) {
	state := l3RequireScaleCluster(t)
	cluster := state.cluster
	if state.foundSize < l3MinPods {
		cluster.scale(t, l3MinPods)
	}
	size := cluster.size(t)
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
	if state.foundSize < l3MinPods {
		cluster.scale(t, l3MinPods)
	}
	size := cluster.size(t)
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
		info, err := node.RequestInfo(as.NewInfoPolicy(), "service-clear-alt")
		if err != nil {
			t.Fatalf("info %s: %v", node.GetName(), err)
		}
		advertised := info["service-clear-alt"]
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

func l3PeersGeneration(t *testing.T, node *as.Node) int64 {
	t.Helper()
	info, err := node.RequestInfo(as.NewInfoPolicy(), "peers-generation")
	if err != nil {
		t.Fatalf("peers-generation %s: %v", node.GetName(), err)
	}
	n, parseErr := strconv.ParseInt(info["peers-generation"], 10, 64)
	if parseErr != nil {
		t.Fatalf("peers-generation %s %q: %v", node.GetName(), info["peers-generation"], parseErr)
	}
	return n
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
	info, err := node.RequestInfo(as.NewInfoPolicy(), "service-clear-std")
	if err != nil {
		t.Fatalf("info %s: %v", node.GetName(), err)
	}
	for _, pod := range pods {
		if l3HasAddress(t, info["service-clear-std"], pod.ip) {
			return pod, true
		}
	}
	return l3Pod{}, false
}

func l3NodeOf(t *testing.T, client *as.Client, pod l3Pod) *as.Node {
	t.Helper()
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "service-clear-std")
		if err != nil {
			t.Fatalf("info %s: %v", node.GetName(), err)
		}
		if l3HasAddress(t, info["service-clear-std"], pod.ip) {
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
				key, err := as.NewKey(ns, l3ScaleSet, fmt.Sprintf("load-%d", (thread+i*l3LoadThreads)%l3LoadKeys))
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
