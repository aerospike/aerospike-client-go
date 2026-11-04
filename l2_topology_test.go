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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	asl "github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	l2TendInterval = 250 * time.Millisecond
	l2Wait         = 30 * time.Second
	l2Poll         = 100 * time.Millisecond
	l2DialTimeout  = time.Second
	l2Set          = "l2"
	l2KeyCount     = 300
	l2MissKeyCount = 200
	l2MissRetries  = 3
	// Longer than the test, so no periodic refresh can run during it.
	l2ReseedRefreshInterval = 10 * time.Minute
	l2ReseedPoll            = 5 * time.Millisecond
	l2TLSProxySuffix        = "-tls"
	l2TLSKeyCount           = 20
)

var l2PeerPattern = regexp.MustCompile(`\[([0-9A-Fa-f]+),([^,\[\]]*),\[`)

type l2Node struct {
	proxy      string
	port       int
	name       string
	advertised []string

	// Set only when the topology has TLS listeners.
	tlsPort       int
	tlsName       string
	tlsAdvertised []string
}

// l2EmptyTranslator maps no address.
type l2EmptyTranslator struct{}

func (l2EmptyTranslator) Translate(as.Endpoint) (as.Host, bool) {
	return as.Host{}, false
}

// l2CountingSeedProvider returns fixed seeds and counts its calls.
type l2CountingSeedProvider struct {
	seeds []*as.Host
	calls atomic.Int32
}

func (provider *l2CountingSeedProvider) RefreshSeedCandidates(*as.SeedCandidateContext) ([]*as.Host, error) {
	provider.calls.Add(1)
	return provider.seeds, nil
}

func (provider *l2CountingSeedProvider) NeedsPeriodicRefresh() bool { return true }

func (provider *l2CountingSeedProvider) SupportsDeadline() bool { return true }

func (provider *l2CountingSeedProvider) Close() {}

func (provider *l2CountingSeedProvider) callCount() int {
	return int(provider.calls.Load())
}

// l2LogCapture records every client log line and forwards it to the logger it replaced.
type l2LogCapture struct {
	forward interface{ Printf(string, ...any) }

	mu    sync.Mutex
	lines []string
}

func (capture *l2LogCapture) Printf(format string, v ...any) {
	capture.mu.Lock()
	capture.lines = append(capture.lines, fmt.Sprintf(format, v...))
	capture.mu.Unlock()
	capture.forward.Printf(format, v...)
}

// misses returns the strict translation miss warnings for node.
func (capture *l2LogCapture) misses(node string) []string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var misses []string
	for _, line := range capture.lines {
		if strings.Contains(line, "has no mapping for node "+node+" ") {
			misses = append(misses, line)
		}
	}
	return misses
}

var (
	l2Once  sync.Once
	l2Host  string
	l2Nodes []l2Node
	l2Err   error
)

func TestL2PortMapByHostPortJoinsAllNodes(t *testing.T) {
	assertL2PortMapJoins(t, l2PortMapByEndpoint)
}

func TestL2PortMapByNodeNameJoinsAllNodes(t *testing.T) {
	assertL2PortMapJoins(t, l2PortMapByNodeName)
}

// Startup must settle within ClientPolicy.Timeout, and a dial to an advertised
// address the network drops takes that whole timeout. Java fails init with
// "Peers not reachable" here instead.
func TestL2WithoutTranslatorFailsInitWithTimeout(t *testing.T) {
	host, nodes := l2Topology(t)
	client, err := as.NewClientWithPolicyAndHost(l2Policy(), as.NewHost(host, nodes[0].port))
	if err == nil {
		client.Close()
		t.Fatalf("client without a translator started with nodes %v, want TIMEOUT", client.GetNodeNames())
	}
	if !err.Matches(types.TIMEOUT) {
		t.Fatalf("client without a translator failed with %v, want TIMEOUT", err)
	}
}

// The peer with no mapping gets no node, so its partitions have no master,
// and the miss is logged again on every tend.
func TestL2StrictMissCreatesNoNode(t *testing.T) {
	host, nodes := l2Topology(t)
	victim := nodes[len(nodes)-1]
	mapped := nodes[:len(nodes)-1]
	logs := captureL2Logs(t)

	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, mapped))
	policy.TranslationOnMiss = as.TranslationMissStrict
	client := newL2Client(t, policy, as.NewHost(host, nodes[0].port))

	want := l2NodeNames(mapped)
	waitL2Nodes(t, client, want)
	waitL2Misses(t, client, logs, want, victim)
	assertL2MissMessage(t, logs.misses(victim.name)[0], victim, "port-map")
	assertL2MappedKeysWork(t, client, want)
}

func TestL2StrictEmptyIpMapDropsPeers(t *testing.T) {
	policy := l2Policy()
	policy.IpMap = map[string]string{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2StrictMissDropsPeers(t, policy, "static-map")
}

func TestL2StrictEmptyCustomTranslatorDropsPeers(t *testing.T) {
	policy := l2Policy()
	policy.AddressTranslator = l2EmptyTranslator{}
	policy.TranslationOnMiss = as.TranslationMissStrict
	assertL2StrictMissDropsPeers(t, policy, fmt.Sprintf("%T", l2EmptyTranslator{}))
}

// In tend mode the provider runs only on its refresh interval, so the client
// re-seeds from the seeds it holds with no provider call after startup.
func TestL2ReseedOnZeroActiveNodes(t *testing.T) {
	provider := assertL2Reseeds(t, as.DiscoveryExecutionTend, nil)
	if calls := provider.callCount(); calls != 1 {
		t.Fatalf("%d seed provider calls, want only the startup call", calls)
	}
}

// In thread mode a client with no nodes also wakes the refresh goroutine. The
// listeners stay down until that refresh has run, since the first tend that
// finds no nodes requests it. The re-seed still uses the seeds in hand.
func TestL2ReseedOnZeroActiveNodesInThreadMode(t *testing.T) {
	assertL2Reseeds(t, as.DiscoveryExecutionThread, func(provider *l2CountingSeedProvider) {
		if !waitL2(l2Poll, func() bool { return provider.callCount() > 1 }) {
			t.Fatal("no seed provider refresh while the client had no nodes, want the urgent refresh")
		}
	})
}

// assertL2Reseeds disables every listener until the client has no nodes, runs
// whileListenersDown if set, enables them again and checks the client
// re-seeds within two tends plus the connect timeout and recovers the full
// partition map. It returns the seed provider the client used.
func assertL2Reseeds(t *testing.T, mode as.DiscoveryExecutionMode, whileListenersDown func(*l2CountingSeedProvider)) *l2CountingSeedProvider {
	t.Helper()
	host, nodes := l2Topology(t)
	provider := &l2CountingSeedProvider{seeds: []*as.Host{as.NewHost(host, nodes[0].port)}}
	policy := l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes))
	policy.SeedProvider = provider
	policy.DiscoveryExecutionMode = mode
	policy.DiscoveryRefreshInterval = l2ReseedRefreshInterval
	client := newL2Client(t, policy)

	want := l2NodeNames(nodes)
	waitL2Cluster(t, client, want)

	for _, node := range nodes {
		disableL2Listener(t, node.proxy)
	}
	if !waitL2(l2Poll, func() bool { return len(client.GetNodes()) == 0 }) {
		t.Fatalf("client kept nodes %v with every listener disabled", client.GetNodeNames())
	}
	if whileListenersDown != nil {
		whileListenersDown(provider)
	}

	reenabled := time.Now()
	for _, node := range nodes {
		enableL2Listener(t, node.proxy)
	}
	if !waitL2(l2ReseedPoll, func() bool { return len(client.GetNodes()) > 0 }) {
		t.Fatal("client did not re-seed")
	}
	if elapsed, budget := time.Since(reenabled), 2*policy.TendInterval+policy.Timeout; elapsed > budget {
		t.Fatalf("re-seeded %s after the listeners came back, budget %s", elapsed, budget)
	}
	waitL2Cluster(t, client, want)
	return provider
}

// Writes with no retry show which keys the client still serves while one
// node's listener is down.
func TestL2NodeRemovedFromEndpointDegradesAndReturns(t *testing.T) {
	host, nodes := l2Topology(t)
	victim := nodes[len(nodes)-1]
	client := newL2Client(t, l2PortMapPolicy(t, l2PortMapByEndpoint(host, nodes)), as.NewHost(host, nodes[0].port))

	want := l2NodeNames(nodes)
	waitL2Cluster(t, client, want)

	masters := as.PartitionMasters(client, *namespace)
	var victimKeys, otherKeys []*as.Key
	for i := range l2KeyCount {
		key := l2Key(t, "n7-", i)
		master := masters[key.PartitionId()]
		if master == nil {
			t.Fatalf("partition %d of key %v has no master", key.PartitionId(), key)
		}
		if master.GetName() == victim.name {
			victimKeys = append(victimKeys, key)
		} else {
			otherKeys = append(otherKeys, key)
		}
	}
	if len(victimKeys) == 0 || len(otherKeys) == 0 {
		t.Fatalf("%d keys on %s and %d on the other nodes, want both", len(victimKeys), victim.name, len(otherKeys))
	}

	policy := as.NewWritePolicy(0, 0)
	policy.MaxRetries = 0
	policy.TotalTimeout = 2 * time.Second

	disableL2Listener(t, victim.proxy)
	if !waitL2(l2Poll, func() bool { return !l2Writes(client, policy, victimKeys[0]) }) {
		t.Fatalf("writes to %s still succeed with its listener disabled", victim.name)
	}
	for _, key := range victimKeys {
		if l2Writes(client, policy, key) {
			t.Fatalf("write of %v to %s succeeded with its listener disabled", key, victim.name)
		}
	}
	for _, key := range otherKeys {
		assertL2KeyWrites(t, client, policy, key, 1)
	}
	others := slices.DeleteFunc(slices.Clone(want), func(name string) bool { return name == victim.name })
	if active := l2ActiveNodeNames(client, victim.name); !slices.Equal(active, others) {
		t.Fatalf("active nodes %v besides %s, want %v", active, victim.name, others)
	}

	enableL2Listener(t, victim.proxy)
	if !waitL2(l2Poll, func() bool {
		return !slices.ContainsFunc(victimKeys, func(key *as.Key) bool { return !l2Writes(client, policy, key) })
	}) {
		t.Fatalf("writes to %s did not recover", victim.name)
	}
	waitL2Cluster(t, client, want)
	for _, key := range append(victimKeys, otherKeys...) {
		assertL2KeyWrites(t, client, policy, key, 2)
	}
}

// The handshake checks the certificate against the node's tls-name, not the
// endpoint hostname the client dials, and a seed named by that hostname fails.
func TestL2TLSValidatesAgainstNodeTLSName(t *testing.T) {
	host, nodes := l2Topology(t)
	if subset := os.Getenv("DISCOVERY_SUBSET"); subset != "ee" {
		t.Skip("N-6 needs DISCOVERY_SUBSET=ee, subset is " + subset)
	}
	first := nodes[0]
	if first.tlsPort == 0 {
		t.Skip("N-6 needs an ee topology with TLS listeners, DISCOVERY_TLS_ENDPOINT_PORTS is empty")
	}
	if first.tlsName == host {
		t.Fatalf("tls-name %s is the endpoint hostname", first.tlsName)
	}

	policy := l2PortMapPolicy(t, l2TLSPortMapByEndpoint(host, nodes))
	policy.TlsConfig = l2TLSConfig(t)
	client := newL2Client(t, policy, &as.Host{Name: host, TLSName: first.tlsName, Port: first.tlsPort})

	waitL2Cluster(t, client, l2NodeNames(nodes))
	endpointAddresses, err := net.LookupHost(host)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]l2Node, len(nodes))
	for _, node := range nodes {
		byName[node.name] = node
	}
	for _, node := range client.GetNodes() {
		got, expected := node.GetHost(), byName[node.GetName()]
		if got.Name != host && !slices.Contains(endpointAddresses, got.Name) {
			t.Fatalf("node %s host %s is not %s %v", node.GetName(), got.Name, host, endpointAddresses)
		}
		if got.Port != expected.tlsPort || got.TLSName != expected.tlsName {
			t.Fatalf("node %s host %s, want port %d and tls-name %s", node.GetName(), got, expected.tlsPort, expected.tlsName)
		}
	}
	for i := range l2TLSKeyCount {
		assertL2KeyWrites(t, client, nil, l2Key(t, "n6-", i), i)
	}

	wrongName, err := as.NewClientWithPolicyAndHost(policy, &as.Host{Name: host, TLSName: host, Port: first.tlsPort})
	if err == nil {
		wrongName.Close()
		t.Fatalf("TLS seed validated against the endpoint hostname %s", host)
	}
	var mismatch x509.HostnameError
	if !errors.As(err, &mismatch) || mismatch.Host != host {
		t.Fatalf("TLS seed with tls-name %s failed with %v, want a certificate name mismatch", host, err)
	}
}

func skipUnlessL2(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME")) == "" {
		t.Skip("set DISCOVERY_ENDPOINT_HOSTNAME to run L2 topology tests")
	}
	if subset := os.Getenv("DISCOVERY_SUBSET"); subset != "" && subset != "ce" && subset != "ee" {
		t.Skip("L2 default-profile smoke, DISCOVERY_SUBSET is " + subset)
	}
	// cn-only is the N-17 profile, not the default smoke.
	if shape := os.Getenv("DISCOVERY_TLS_CERT_SHAPE"); shape != "" && shape != "san-only" {
		t.Skip("L2 default-profile smoke, DISCOVERY_TLS_CERT_SHAPE is " + shape)
	}
	for _, name := range []string{
		"DISCOVERY_REACHABLE_NODE",
		"DISCOVERY_PASS_THROUGH_BRANCH",
		"DISCOVERY_PORT_MAP_RANGE",
		"DISCOVERY_CLIENT_NETWORK_MTU",
		"DISCOVERY_PROXY_PORT_RANGE",
		"DISCOVERY_IDLE_RESET",
	} {
		if os.Getenv(name) != "" {
			t.Skip("L2 default-profile smoke, " + name + " is set")
		}
	}
}

func l2Topology(t *testing.T) (string, []l2Node) {
	t.Helper()
	skipUnlessL2(t)
	l2Once.Do(func() {
		l2Host = strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
		l2Nodes, l2Err = discoverL2Nodes(l2Host)
	})
	if l2Err != nil {
		t.Fatal(l2Err)
	}
	return l2Host, l2Nodes
}

func discoverL2Nodes(host string) ([]l2Node, error) {
	ports, err := l2Ports("DISCOVERY_ENDPOINT_PORTS")
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, errors.New("DISCOVERY_ENDPOINT_PORTS is not set")
	}
	tlsPorts, err := l2Ports("DISCOVERY_TLS_ENDPOINT_PORTS")
	if err != nil {
		return nil, err
	}
	rawContainers := os.Getenv("AEROSPIKE_CONTAINER_NAMES")
	if rawContainers == "" {
		return nil, errors.New("AEROSPIKE_CONTAINER_NAMES is not set")
	}
	if containers := strings.Split(rawContainers, ","); len(containers) != len(ports) {
		return nil, fmt.Errorf("DISCOVERY_ENDPOINT_PORTS %v vs AEROSPIKE_CONTAINER_NAMES %v", ports, containers)
	}
	proxyPorts, err := l2ProxyPorts()
	if err != nil {
		return nil, err
	}
	proxies := make(map[int]string, len(proxyPorts))
	for proxy, port := range proxyPorts {
		proxies[port] = proxy
	}
	withTLS := len(tlsPorts) > 0
	commands := []string{"node", "service-clear-std"}
	if withTLS {
		commands = append(commands, "service-tls-std", "peers-tls-std")
	}
	nodes := make([]l2Node, 0, len(ports))
	tlsNames := make(map[string]string)
	for _, port := range ports {
		proxy, found := proxies[port]
		if !found {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP has no proxy for listener %d", port)
		}
		info, err := l2RequestInfo(host, port, commands...)
		if err != nil {
			return nil, fmt.Errorf("info %s:%d: %w", host, port, err)
		}
		name := strings.TrimSpace(info["node"])
		if name == "" {
			return nil, fmt.Errorf("no node name from listener %d", port)
		}
		advertised, err := l2Advertised(info["service-clear-std"])
		if err != nil {
			return nil, fmt.Errorf("node %s service-clear-std: %w", name, err)
		}
		if len(advertised) == 0 {
			return nil, fmt.Errorf("node %s advertises no clear address", name)
		}
		for _, endpoint := range advertised {
			if l2Reachable(endpoint) {
				return nil, fmt.Errorf("node %s advertised address %s is reachable from the client", name, endpoint)
			}
		}
		node := l2Node{proxy: proxy, port: port, name: name, advertised: advertised}
		if withTLS {
			if node.tlsPort, found = proxyPorts[proxy+l2TLSProxySuffix]; !found {
				return nil, fmt.Errorf("DISCOVERY_PROXY_MAP has no TLS proxy %s%s", proxy, l2TLSProxySuffix)
			}
			if !slices.Contains(tlsPorts, node.tlsPort) {
				return nil, fmt.Errorf("TLS proxy %s%s listens on %d, not in DISCOVERY_TLS_ENDPOINT_PORTS %v", proxy, l2TLSProxySuffix, node.tlsPort, tlsPorts)
			}
			if node.tlsAdvertised, err = l2Advertised(info["service-tls-std"]); err != nil {
				return nil, fmt.Errorf("node %s service-tls-std: %w", name, err)
			}
			if len(node.tlsAdvertised) == 0 {
				return nil, fmt.Errorf("node %s advertises no TLS address", name)
			}
			maps.Copy(tlsNames, l2PeerTLSNames(info["peers-tls-std"]))
		}
		nodes = append(nodes, node)
	}
	if withTLS {
		for i := range nodes {
			if nodes[i].tlsName = tlsNames[nodes[i].name]; nodes[i].tlsName == "" {
				return nil, fmt.Errorf("no peer lists a tls-name for node %s", nodes[i].name)
			}
		}
	}
	return nodes, nil
}

// l2PeerTLSNames reads the tls-name of each peer in a peers-tls-std reply,
// gen,port,[[node,tls-name,[address,...]],...]. A node does not list itself.
func l2PeerTLSNames(reply string) map[string]string {
	names := make(map[string]string)
	for _, match := range l2PeerPattern.FindAllStringSubmatch(reply, -1) {
		if match[2] != "" {
			names[match[1]] = match[2]
		}
	}
	return names
}

// l2ProxyPorts parses DISCOVERY_PROXY_MAP, a list of Toxiproxy proxy
// name:listener port pairs, into listener ports by proxy name.
func l2ProxyPorts() (map[string]int, error) {
	proxies := make(map[string]int)
	for _, entry := range strings.Split(os.Getenv("DISCOVERY_PROXY_MAP"), ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		index := strings.LastIndexByte(entry, ':')
		if index <= 0 {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP entry %q is not name:port", entry)
		}
		port, err := strconv.Atoi(entry[index+1:])
		if err != nil {
			return nil, fmt.Errorf("DISCOVERY_PROXY_MAP entry %q: %w", entry, err)
		}
		proxies[entry[:index]] = port
	}
	if len(proxies) == 0 {
		return nil, errors.New("DISCOVERY_PROXY_MAP is not set")
	}
	return proxies, nil
}

// l2Ports parses a comma-separated port list from the named variable. Unset is empty.
func l2Ports(name string) ([]int, error) {
	var ports []int
	for _, part := range strings.Split(os.Getenv(name), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		port, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ports = append(ports, port)
	}
	return ports, nil
}

func l2RequestInfo(host string, port int, commands ...string) (map[string]string, error) {
	policy := as.NewClientPolicy()
	policy.Timeout = time.Second
	conn, err := as.NewConnection(policy, as.NewHost(host, port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.RequestInfo(commands...)
}

func l2Advertised(reply string) ([]string, error) {
	var advertised []string
	for _, part := range strings.Split(reply, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hosts, err := as.NewHosts(part)
		if err != nil {
			return nil, err
		}
		for _, host := range hosts {
			advertised = append(advertised, host.String())
		}
	}
	return advertised, nil
}

func l2PortMapByEndpoint(host string, nodes []l2Node) map[string]string {
	return l2ListenerPortMap(host, nodes, func(node l2Node) (int, []string) { return node.port, node.advertised })
}

func l2TLSPortMapByEndpoint(host string, nodes []l2Node) map[string]string {
	return l2ListenerPortMap(host, nodes, func(node l2Node) (int, []string) { return node.tlsPort, node.tlsAdvertised })
}

// l2ListenerPortMap maps each advertised address of a node to its listener on host.
func l2ListenerPortMap(host string, nodes []l2Node, listenerOf func(l2Node) (int, []string)) map[string]string {
	portMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		port, advertised := listenerOf(node)
		listener := net.JoinHostPort(host, strconv.Itoa(port))
		for _, address := range advertised {
			portMap[address] = listener
		}
	}
	return portMap
}

func l2PortMapByNodeName(host string, nodes []l2Node) map[string]string {
	portMap := make(map[string]string, len(nodes))
	for _, node := range nodes {
		portMap[node.name] = net.JoinHostPort(host, strconv.Itoa(node.port))
	}
	return portMap
}

// l2TLSConfig trusts only the server CA in AEROSPIKE_CA_CERT.
func l2TLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	path := os.Getenv("AEROSPIKE_CA_CERT")
	if path == "" {
		t.Fatal("AEROSPIKE_CA_CERT is not set")
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatalf("no certificate in %s", path)
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

func l2NodeNames(nodes []l2Node) []string {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.name)
	}
	slices.Sort(names)
	return names
}

func l2Policy() *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.TendInterval = l2TendInterval
	policy.Timeout = time.Second
	return policy
}

func l2PortMapPolicy(t *testing.T, portMap map[string]string) *as.ClientPolicy {
	t.Helper()
	translator, err := as.NewPortMapAddressTranslator(portMap)
	if err != nil {
		t.Fatal(err)
	}
	policy := l2Policy()
	policy.AddressTranslator = translator
	return policy
}

// newL2Client closes the client when the test ends.
func newL2Client(t *testing.T, policy *as.ClientPolicy, seeds ...*as.Host) *as.Client {
	t.Helper()
	client, err := as.NewClientWithPolicyAndHost(policy, seeds...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// captureL2Logs raises the client log level to warnings for the test.
// Call it before creating the client, so the client is closed before the
// logger is restored.
func captureL2Logs(t *testing.T) *l2LogCapture {
	t.Helper()
	capture := &l2LogCapture{forward: asl.Logger.Logger}
	asl.Logger.SetLogger(capture)
	asl.Logger.SetLevel(asl.WARNING)
	t.Cleanup(func() {
		asl.Logger.SetLogger(capture.forward)
		if *debug {
			asl.Logger.SetLevel(asl.DEBUG)
		} else {
			asl.Logger.SetLevel(asl.OFF)
		}
	})
	return capture
}

// disableL2Listener enables the listener again when the test ends.
func disableL2Listener(t *testing.T, proxy string) {
	t.Helper()
	if err := setL2Listener(proxy, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := setL2Listener(proxy, true); err != nil {
			t.Error(err)
		}
	})
}

func enableL2Listener(t *testing.T, proxy string) {
	t.Helper()
	if err := setL2Listener(proxy, true); err != nil {
		t.Fatal(err)
	}
}

func setL2Listener(proxy string, enabled bool) error {
	toxiproxy := strings.TrimRight(os.Getenv("TOXIPROXY_URL"), "/")
	if toxiproxy == "" {
		return errors.New("TOXIPROXY_URL is not set")
	}
	httpClient := http.Client{Timeout: 10 * time.Second}
	response, err := httpClient.Post(toxiproxy+"/proxies/"+proxy, "application/json", strings.NewReader(fmt.Sprintf(`{"enabled":%t}`, enabled)))
	if err != nil {
		return fmt.Errorf("toxiproxy %s enabled=%t: %w", proxy, enabled, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("toxiproxy %s enabled=%t: status %d %s", proxy, enabled, response.StatusCode, body)
	}
	return nil
}

func assertL2PortMapJoins(t *testing.T, portMapOf func(host string, nodes []l2Node) map[string]string) {
	t.Helper()
	host, nodes := l2Topology(t)
	client := newL2Client(t, l2PortMapPolicy(t, portMapOf(host, nodes)), as.NewHost(host, nodes[0].port))
	waitL2Cluster(t, client, l2NodeNames(nodes))
}

func waitL2Nodes(t *testing.T, client *as.Client, want []string) {
	t.Helper()
	var got []string
	if !waitL2(l2Poll, func() bool {
		got = slices.Sorted(slices.Values(client.GetNodeNames()))
		return slices.Equal(got, want)
	}) {
		t.Fatalf("nodes %v, want %v", got, want)
	}
}

// waitL2Cluster waits until the client holds the want nodes and they master
// every partition of the test namespace.
func waitL2Cluster(t *testing.T, client *as.Client, want []string) {
	t.Helper()
	waitL2Nodes(t, client, want)
	var owners []string
	var unowned int
	if !waitL2(l2Poll, func() bool {
		owners, unowned = l2PartitionOwners(client)
		return unowned == 0
	}) {
		t.Fatalf("%d partitions of namespace %s have no active master, owners %v", unowned, *namespace, owners)
	}
	if !slices.Equal(owners, want) {
		t.Fatalf("partition owners %v, want %v", owners, want)
	}
}

func l2PartitionOwners(client *as.Client) ([]string, int) {
	owners := make(map[string]struct{})
	unowned := 0
	for _, node := range as.PartitionMasters(client, *namespace) {
		if node == nil || !node.IsActive() {
			unowned++
			continue
		}
		owners[node.GetName()] = struct{}{}
	}
	return slices.Sorted(maps.Keys(owners)), unowned
}

// waitL2 polls condition until it holds or l2Wait passes.
func waitL2(poll time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(l2Wait)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(poll)
	}
	return true
}

// waitL2Misses waits until each peer's miss has been logged on several tends,
// with the client holding exactly the want nodes throughout.
func waitL2Misses(t *testing.T, client *as.Client, logs *l2LogCapture, want []string, peers ...l2Node) {
	t.Helper()
	underRetried := func(peer l2Node) bool { return len(logs.misses(peer.name)) < l2MissRetries }
	if waitL2(l2Poll, func() bool {
		if got := slices.Sorted(slices.Values(client.GetNodeNames())); !slices.Equal(got, want) {
			t.Fatalf("nodes %v, want %v", got, want)
		}
		return !slices.ContainsFunc(peers, underRetried)
	}) {
		return
	}
	for _, peer := range peers {
		if underRetried(peer) {
			t.Fatalf("missed peer %s logged %d times, want at least %d", peer.name, len(logs.misses(peer.name)), l2MissRetries)
		}
	}
}

func assertL2MissMessage(t *testing.T, message string, peer l2Node, translator string) {
	t.Helper()
	for _, part := range []string{peer.name, peer.advertised[0], "standard", translator, "pass_through"} {
		if !strings.Contains(message, part) {
			t.Fatalf("%q not in miss warning: %s", part, message)
		}
	}
}

func assertL2StrictMissDropsPeers(t *testing.T, policy *as.ClientPolicy, translator string) {
	t.Helper()
	host, nodes := l2Topology(t)
	seed, peers := nodes[0], nodes[1:]
	logs := captureL2Logs(t)
	client := newL2Client(t, policy, as.NewHost(host, seed.port))

	want := []string{seed.name}
	waitL2Misses(t, client, logs, want, peers...)
	for _, peer := range peers {
		assertL2MissMessage(t, logs.misses(peer.name)[0], peer, translator)
	}
	assertL2SeedKeysWork(t, client, seed)
}

// assertL2MappedKeysWork checks that only mapped nodes are masters, that some
// partitions have none, and that every key, owned or not, can be written.
func assertL2MappedKeysWork(t *testing.T, client *as.Client, mapped []string) {
	t.Helper()
	masters := as.PartitionMasters(client, *namespace)
	noMaster := 0
	for _, node := range masters {
		if node == nil {
			noMaster++
		} else if !slices.Contains(mapped, node.GetName()) {
			t.Fatalf("node %s is a master, want only %v", node.GetName(), mapped)
		}
	}
	owned, missed := 0, 0
	for i := range l2KeyCount {
		key := l2Key(t, "n8-", i)
		if masters[key.PartitionId()] == nil {
			missed++
		} else {
			owned++
		}
		assertL2KeyWrites(t, client, nil, key, i)
	}
	if noMaster == 0 || owned == 0 || missed == 0 {
		t.Fatalf("%d partitions without a master, %d keys on mapped masters, %d keys on missed partitions, want all above zero", noMaster, owned, missed)
	}
}

// assertL2SeedKeysWork checks that the seed serves every key routed to it and
// that the keys of partitions no node holds are not routed at all.
func assertL2SeedKeysWork(t *testing.T, client *as.Client, seed l2Node) {
	t.Helper()
	cluster := client.Cluster()
	policy := client.GetDefaultWritePolicy()
	onSeed, notOwned := 0, 0
	for i := range l2MissKeyCount {
		key := l2Key(t, "miss-", i)
		partition, err := as.PartitionForWrite(cluster, &policy.BasePolicy, key)
		if err != nil {
			t.Fatal(err)
		}
		node, err := partition.GetNodeWrite(cluster)
		if err != nil {
			if !err.Matches(types.INVALID_NODE_ERROR) {
				t.Fatal(err)
			}
			notOwned++
			continue
		}
		if node.GetName() != seed.name {
			t.Fatalf("key %v routed to %s, want the seed %s", key, node.GetName(), seed.name)
		}
		assertL2KeyWrites(t, client, policy, key, i)
		onSeed++
	}
	if onSeed == 0 || notOwned == 0 {
		t.Fatalf("%d keys on the seed and %d keys with no node, want both above zero", onSeed, notOwned)
	}
}

func l2Key(t *testing.T, prefix string, i int) *as.Key {
	t.Helper()
	key, err := as.NewKey(*namespace, l2Set, prefix+strconv.Itoa(i))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func l2Writes(client *as.Client, policy *as.WritePolicy, key *as.Key) bool {
	return client.Put(policy, key, as.BinMap{"v": 0}) == nil
}

func assertL2KeyWrites(t *testing.T, client *as.Client, policy *as.WritePolicy, key *as.Key, value int) {
	t.Helper()
	if err := client.Put(policy, key, as.BinMap{"v": value}); err != nil {
		t.Fatalf("put %v: %v", key, err)
	}
	record, err := client.Get(nil, key)
	if err != nil {
		t.Fatalf("get %v: %v", key, err)
	}
	if got := record.Bins["v"]; got != value {
		t.Fatalf("key %v has v=%v, want %d", key, got, value)
	}
}

// l2ActiveNodeNames returns the sorted names of the active nodes other than except.
func l2ActiveNodeNames(client *as.Client, except string) []string {
	var names []string
	for _, node := range client.GetNodes() {
		if node.IsActive() && node.GetName() != except {
			names = append(names, node.GetName())
		}
	}
	slices.Sort(names)
	return names
}

func l2Reachable(endpoint string) bool {
	conn, err := net.DialTimeout("tcp", endpoint, l2DialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
