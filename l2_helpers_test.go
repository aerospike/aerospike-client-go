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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	l2TendInterval     = 250 * time.Millisecond
	l2Wait             = 30 * time.Second
	l2Poll             = 100 * time.Millisecond
	l2DialTimeout      = 2 * time.Second
	l2Set              = "l2"
	l2KeyCount         = 300
	l2MissKeyCount     = 200
	l2MissRetries      = 3
	l2CollisionRetries = 3
	l2TLSProxySuffix   = "-tls"
	l2HoldToxic        = "hold"
)

var (
	l2PeerPattern     = regexp.MustCompile(`\[([0-9A-Fa-f]+),([^,\[\]]*),\[`)
	l2PeerFullPattern = regexp.MustCompile(`\[([0-9A-Fa-f]+),([^,\[\]]*),\[((?:[^\[\]]|\[[^\]]*\])*)\]\]`)
)

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

// containing returns captured lines that include every part.
func (capture *l2LogCapture) containing(parts ...string) []string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	var matched []string
	for _, line := range capture.lines {
		ok := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				ok = false
				break
			}
		}
		if ok {
			matched = append(matched, line)
		}
	}
	return matched
}

// misses returns the strict translation miss warnings for node.
func (capture *l2LogCapture) misses(node string) []string {
	return capture.containing("has no mapping for node " + node + " ")
}

// collisions returns translation-collision warnings that name node.
func (capture *l2LogCapture) collisions(node string) []string {
	return capture.containing("to the same address", node)
}

// l2Failure is one command that failed under l2Load.
type l2Failure struct {
	start   time.Time
	elapsed time.Duration
	err     as.Error
}

// codes returns the result codes down the error chain, outermost first.
func (failure l2Failure) codes() string {
	var codes []string
	for err := error(failure.err); err != nil; err = errors.Unwrap(err) {
		if aerospikeErr, ok := err.(*as.AerospikeError); ok {
			codes = append(codes, aerospikeErr.ResultCode.String())
		}
	}
	return strings.Join(codes, ">")
}

func (failure l2Failure) end() time.Time {
	return failure.start.Add(failure.elapsed)
}

// l2Load runs a command in a loop on several goroutines until stopped.
type l2Load struct {
	stop      chan struct{}
	stopOnce  sync.Once
	done      sync.WaitGroup
	successes atomic.Int64

	mu     sync.Mutex
	failed []l2Failure
}

func startL2Load(threads int, command func(thread, i int) as.Error) *l2Load {
	load := &l2Load{stop: make(chan struct{})}
	for thread := range threads {
		load.done.Add(1)
		go func() {
			defer load.done.Done()
			for i := 0; ; i++ {
				select {
				case <-load.stop:
					return
				default:
				}
				start := time.Now()
				err := command(thread, i)
				if err == nil {
					load.successes.Add(1)
					continue
				}
				failure := l2Failure{start: start, elapsed: time.Since(start), err: err}
				load.mu.Lock()
				load.failed = append(load.failed, failure)
				load.mu.Unlock()
			}
		}()
	}
	return load
}

// stopWithin stops the load and reports whether every goroutine returned within bound.
func (load *l2Load) stopWithin(bound time.Duration) bool {
	load.stopOnce.Do(func() { close(load.stop) })
	finished := make(chan struct{})
	go func() {
		load.done.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return true
	case <-time.After(bound):
		return false
	}
}

func (load *l2Load) String() string {
	chains := map[string]int{}
	for _, failure := range load.failures() {
		chains[failure.codes()]++
	}
	return fmt.Sprintf("%d successes, failures by error chain %v", load.successes.Load(), chains)
}

// failures returns the failures with any of codes down their error chain, or
// all of them if none is given.
func (load *l2Load) failures(codes ...types.ResultCode) []l2Failure {
	load.mu.Lock()
	defer load.mu.Unlock()
	if len(codes) == 0 {
		return slices.Clone(load.failed)
	}
	var failures []l2Failure
	for _, failure := range load.failed {
		if failure.err.Matches(codes...) {
			failures = append(failures, failure)
		}
	}
	return failures
}

var (
	l2Once  sync.Once
	l2Host  string
	l2Nodes []l2Node
	l2Err   error
)

func skipUnlessL2(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME")) == "" {
		t.Skip("set DISCOVERY_ENDPOINT_HOSTNAME to run L2 topology tests")
	}
	if subset := os.Getenv("DISCOVERY_SUBSET"); subset != "" && subset != "ce" && subset != "ee" {
		t.Skip("L2 tests need DISCOVERY_SUBSET=ce or ee, got " + subset)
	}
}

func skipUnlessL2Default(t *testing.T) {
	t.Helper()
	skipUnlessL2(t)
	if shape := os.Getenv("DISCOVERY_TLS_CERT_SHAPE"); shape != "" && shape != "san-only" {
		t.Skip("L2 default-profile tests, DISCOVERY_TLS_CERT_SHAPE is " + shape)
	}
	for _, name := range []string{
		"DISCOVERY_REACHABLE_NODE",
		"DISCOVERY_PASS_THROUGH_BRANCH",
		"DISCOVERY_PORT_MAP_RANGE",
		"DISCOVERY_CLIENT_NETWORK_MTU",
		"DISCOVERY_PROXY_PORT_RANGE",
		"DISCOVERY_IDLE_RESET",
		"DISCOVERY_UNRESOLVED_HOSTNAME",
	} {
		if os.Getenv(name) != "" {
			t.Skip("L2 default-profile tests, " + name + " is set")
		}
	}
}

func l2Topology(t *testing.T) (string, []l2Node) {
	t.Helper()
	skipUnlessL2Default(t)
	return l2Discover(t)
}

// l2Profile discovers the live topology when every required DISCOVERY_* variable is set.
func l2Profile(t *testing.T, reason string, required ...string) (string, []l2Node) {
	t.Helper()
	skipUnlessL2(t)
	for _, name := range required {
		if os.Getenv(name) == "" {
			t.Skip(reason)
		}
	}
	return l2Discover(t)
}

func l2Discover(t *testing.T) (string, []l2Node) {
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
		reachable := os.Getenv("DISCOVERY_REACHABLE_NODE")
		if proxy != reachable {
			for _, endpoint := range advertised {
				if l2Reachable(endpoint) {
					return nil, fmt.Errorf("node %s advertised address %s is reachable from the client", name, endpoint)
				}
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
	return l2Toxiproxy(http.MethodPost, "/proxies/"+proxy, fmt.Sprintf(`{"enabled":%t}`, enabled), http.StatusOK)
}

// holdL2Listener stops the node's replies without closing its connections, and
// returns the release, which also runs when the test ends.
func holdL2Listener(t *testing.T, proxy string) (release func()) {
	t.Helper()
	toxic := fmt.Sprintf(`{"name":%q,"type":"timeout","stream":"upstream","toxicity":1.0,"attributes":{"timeout":0}}`, l2HoldToxic)
	if err := l2Toxiproxy(http.MethodPost, "/proxies/"+proxy+"/toxics", toxic, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() {
		once.Do(func() {
			if err := l2Toxiproxy(http.MethodDelete, "/proxies/"+proxy+"/toxics/"+l2HoldToxic, "", http.StatusNoContent); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

// nextL2ListenerPort returns the first free port above every Toxiproxy listener.
func nextL2ListenerPort(t *testing.T) int {
	t.Helper()
	port := 0
	for _, listener := range l2ToxiproxyProxies(t) {
		if _, listenPort, err := net.SplitHostPort(listener.Listen); err == nil {
			if used, err := strconv.Atoi(listenPort); err == nil {
				port = max(port, used)
			}
		}
	}
	return port + 1
}

// addL2ListenerOn creates a Toxiproxy listener named proxy on port, removes it
// when the test ends and returns its port.
func addL2ListenerOn(t *testing.T, proxy string, port int, upstream string) int {
	t.Helper()
	listener := fmt.Sprintf(`{"name":%q,"listen":"0.0.0.0:%d","upstream":%q,"enabled":true}`, proxy, port, upstream)
	if err := l2Toxiproxy(http.MethodPost, "/proxies", listener, http.StatusCreated); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l2Toxiproxy(http.MethodDelete, "/proxies/"+proxy, "", http.StatusNoContent); err != nil {
			t.Error(err)
		}
	})
	return port
}

func addL2Toxic(t *testing.T, proxy, name, kind, stream string, toxicity float64, attributes string) {
	t.Helper()
	body := fmt.Sprintf(`{"name":%q,"type":%q,"stream":%q,"toxicity":%g,"attributes":%s}`, name, kind, stream, toxicity, attributes)
	if err := l2Toxiproxy(http.MethodPost, "/proxies/"+proxy+"/toxics", body, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l2Toxiproxy(http.MethodDelete, "/proxies/"+proxy+"/toxics/"+name, "", http.StatusNoContent); err != nil {
			t.Error(err)
		}
	})
}

// l2UpstreamPort returns the node port a Toxiproxy listener forwards to.
func l2UpstreamPort(t *testing.T, proxy string) int {
	t.Helper()
	listener, ok := l2ToxiproxyProxies(t)[proxy]
	if !ok {
		t.Fatalf("toxiproxy has no listener %s", proxy)
	}
	_, upstreamPort, err := net.SplitHostPort(listener.Upstream)
	if err != nil {
		t.Fatalf("listener %s upstream %q: %v", proxy, listener.Upstream, err)
	}
	port, err := strconv.Atoi(upstreamPort)
	if err != nil {
		t.Fatalf("listener %s upstream %q: %v", proxy, listener.Upstream, err)
	}
	return port
}

type l2ToxiproxyListener struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
}

func l2ToxiproxyProxies(t *testing.T) map[string]l2ToxiproxyListener {
	t.Helper()
	reply, err := l2ToxiproxyReply(http.MethodGet, "/proxies", "", http.StatusOK)
	if err != nil {
		t.Fatal(err)
	}
	var proxies map[string]l2ToxiproxyListener
	if err := json.Unmarshal(reply, &proxies); err != nil {
		t.Fatalf("toxiproxy proxies %s: %v", reply, err)
	}
	return proxies
}

func l2Toxiproxy(method, path, body string, want int) error {
	_, err := l2ToxiproxyReply(method, path, body, want)
	return err
}

func l2ToxiproxyReply(method, path, body string, want int) ([]byte, error) {
	toxiproxy := strings.TrimRight(os.Getenv("TOXIPROXY_URL"), "/")
	if toxiproxy == "" {
		return nil, errors.New("TOXIPROXY_URL is not set")
	}
	request, err := http.NewRequest(method, toxiproxy+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	httpClient := http.Client{Timeout: 10 * time.Second}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	reply, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	if response.StatusCode != want {
		return nil, fmt.Errorf("toxiproxy %s %s: status %d %s", method, path, response.StatusCode, reply)
	}
	return reply, nil
}

type l2StartedNode struct {
	container string
	address   string
}

// startL2NodeArgs asks the host to run command, which starts a node, and stops
// the node when the test ends, waiting until the cluster is back to nodes.
func startL2NodeArgs(t *testing.T, controlDir, host string, nodes []l2Node, command string) l2StartedNode {
	t.Helper()
	started := startL2Extra(t, controlDir, command)
	t.Cleanup(func() {
		l2Control(t, controlDir, "stop "+started.container)
		waitL2ClusterStable(t, host, nodes)
	})
	return started
}

// startL2Extra asks the host to start a node. The caller stops it. Address is
// empty when the node exits at startup.
func startL2Extra(t *testing.T, controlDir, command string) l2StartedNode {
	t.Helper()
	ack := l2Control(t, controlDir, command)
	started := l2StartedNode{container: ack["container"], address: ack["address"]}
	if started.container == "" {
		t.Fatalf("start acknowledgement %v has no container", ack)
	}
	return started
}

func stopL2Extras(t *testing.T, controlDir, host string, nodes []l2Node, extras []l2StartedNode) {
	t.Helper()
	t.Cleanup(func() {
		for i := len(extras) - 1; i >= 0; i-- {
			l2Control(t, controlDir, "stop "+extras[i].container)
		}
		if len(extras) > 0 {
			waitL2ClusterStable(t, host, nodes)
		}
	})
}

// l2Control writes command to controlDir as <id>.request and returns the host's
// <id>.ack, one key=value per line.
func l2Control(t *testing.T, controlDir, command string) map[string]string {
	t.Helper()
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	pending := filepath.Join(controlDir, id+".tmp")
	if err := os.WriteFile(pending, []byte(command+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(pending, filepath.Join(controlDir, id+".request")); err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(controlDir, id+".ack")
	var reply []byte
	if !waitL2(l2Poll, func() bool {
		var err error
		reply, err = os.ReadFile(ackPath)
		return err == nil
	}) {
		t.Fatalf("no acknowledgement for %q in %s", command, controlDir)
	}
	if err := os.Remove(ackPath); err != nil {
		t.Error(err)
	}
	ack := make(map[string]string)
	for _, line := range strings.Split(string(reply), "\n") {
		if key, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok && key != "" {
			ack[key] = value
		}
	}
	if failure, ok := ack["error"]; ok {
		t.Fatalf("%q failed: %s", command, failure)
	}
	return ack
}

// waitL2ClusterStable waits until every node reports the same stable cluster of len(nodes).
func waitL2ClusterStable(t *testing.T, host string, nodes []l2Node) {
	t.Helper()
	waitL2ClusterSize(t, host, nodes, len(nodes))
}

func waitL2ClusterSize(t *testing.T, host string, nodes []l2Node, size int) {
	t.Helper()
	command := fmt.Sprintf("cluster-stable:size=%d;ignore-migrations=false", size)
	var keys []string
	if !waitL2(l2Poll, func() bool {
		keys = keys[:0]
		for _, node := range nodes {
			info, err := l2RequestInfo(host, node.port, command)
			if err != nil {
				keys = append(keys, err.Error())
				continue
			}
			keys = append(keys, info[command])
		}
		return len(slices.Compact(slices.Clone(keys))) == 1 && !strings.HasPrefix(keys[0], "ERROR")
	}) {
		t.Fatalf("cluster not stable at size %d: %v", size, keys)
	}
}

// l2NamespaceStats returns the client command counters of the test namespace on node.
func l2NamespaceStats(t *testing.T, host string, node l2Node) map[string]int64 {
	t.Helper()
	command := "namespace/" + *namespace
	info, err := l2RequestInfo(host, node.port, command)
	if err != nil {
		t.Fatal(err)
	}
	wanted := []string{"client_write_success", "client_read_success", "client_proxy_complete", "client_proxy_error", "client_proxy_timeout"}
	stats := make(map[string]int64, len(wanted))
	for _, stat := range strings.Split(info[command], ";") {
		if name, value, ok := strings.Cut(stat, "="); ok && slices.Contains(wanted, name) {
			count, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatalf("node %s %s=%q: %v", node.name, name, value, err)
			}
			stats[name] = count
		}
	}
	for _, name := range wanted {
		if _, ok := stats[name]; !ok {
			t.Fatalf("node %s has no namespace statistic %s", node.name, name)
		}
	}
	return stats
}

// assertL2RoutedTo checks the client sends writes and reads of key to owner.
func assertL2RoutedTo(t *testing.T, cluster *as.Cluster, write, read *as.BasePolicy, key *as.Key, owner *as.Node) {
	t.Helper()
	writePartition, err := as.PartitionForWrite(cluster, write, key)
	if err != nil {
		t.Fatal(err)
	}
	writeNode, err := writePartition.GetNodeWrite(cluster)
	if err != nil {
		t.Fatal(err)
	}
	readPartition, err := as.PartitionForRead(cluster, read, key)
	if err != nil {
		t.Fatal(err)
	}
	readNode, err := readPartition.GetNodeRead(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if writeNode != owner || readNode != owner {
		t.Fatalf("key %v writes to %v and reads from %v, want its master %v", key, writeNode, readNode, owner)
	}
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

func l2ControlDir(t *testing.T, reason string) string {
	t.Helper()
	dir := os.Getenv("DISCOVERY_CONTROL_DIR")
	if dir == "" {
		t.Skip(reason)
	}
	return dir
}

type l2Peer struct {
	name      string
	tlsName   string
	endpoints []string
}

func l2ParsePeers(reply string) []l2Peer {
	first := strings.IndexByte(reply, ',')
	if first < 0 {
		return nil
	}
	secondRel := strings.IndexByte(reply[first+1:], ',')
	if secondRel < 0 {
		return nil
	}
	second := first + 1 + secondRel
	defaultPort := 0
	if portString := reply[first+1 : second]; portString != "" {
		if port, err := strconv.Atoi(portString); err == nil {
			defaultPort = port
		}
	}
	var peers []l2Peer
	for _, match := range l2PeerFullPattern.FindAllStringSubmatch(reply[second+1:], -1) {
		var endpoints []string
		for _, address := range strings.Split(match[3], ",") {
			if address == "" {
				continue
			}
			hasPort := strings.Contains(address, "]:") || (!strings.HasPrefix(address, "[") && strings.Contains(address, ":"))
			if !hasPort && defaultPort > 0 {
				address = net.JoinHostPort(address, strconv.Itoa(defaultPort))
			}
			endpoints = append(endpoints, address)
		}
		peers = append(peers, l2Peer{name: match[1], tlsName: match[2], endpoints: endpoints})
	}
	return peers
}

func l2PeerNames(t *testing.T, host string, observer l2Node, command, endpoint string) []string {
	t.Helper()
	info, err := l2RequestInfo(host, observer.port, command)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, peer := range l2ParsePeers(info[command]) {
		if slices.Contains(peer.endpoints, endpoint) {
			names = append(names, peer.name)
		}
	}
	slices.Sort(names)
	return names
}

func l2PeerName(t *testing.T, host string, observer l2Node, command, endpoint string) string {
	t.Helper()
	names := l2PeerNames(t, host, observer, command, endpoint)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func l2Alternates(t *testing.T, host string, node l2Node) []string {
	t.Helper()
	info, err := l2RequestInfo(host, node.port, "service-clear-alt")
	if err != nil {
		t.Fatal(err)
	}
	advertised, err := l2Advertised(info["service-clear-alt"])
	if err != nil {
		t.Fatal(err)
	}
	return advertised
}

func l2Alternate(t *testing.T, host string, node l2Node) string {
	t.Helper()
	list := l2Alternates(t, host, node)
	if len(list) != 1 {
		t.Fatalf("node %s alternate list %v, want 1", node.name, list)
	}
	return list[0]
}

func l2HostPort(t *testing.T, endpoint string) (string, int) {
	t.Helper()
	host, portString, err := net.SplitHostPort(endpoint)
	if err != nil {
		t.Fatalf("endpoint %q: %v", endpoint, err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("endpoint %q: %v", endpoint, err)
	}
	return host, port
}

func assertL2Address(t *testing.T, advertised string, host *as.Host) {
	t.Helper()
	name, port := l2HostPort(t, advertised)
	addresses, err := net.LookupHost(name)
	if err != nil {
		t.Fatal(err)
	}
	if host.Name != name && !slices.Contains(addresses, host.Name) {
		t.Fatalf("node host %s is not %s %v", host.Name, name, addresses)
	}
	if host.Port != port {
		t.Fatalf("node port %d, want %d", host.Port, port)
	}
}

func l2ListenerPorts(t *testing.T) map[int]struct{} {
	t.Helper()
	ports := make(map[int]struct{})
	for _, listener := range l2ToxiproxyProxies(t) {
		_, listenPort, err := net.SplitHostPort(listener.Listen)
		if err != nil {
			continue
		}
		if port, err := strconv.Atoi(listenPort); err == nil {
			ports[port] = struct{}{}
		}
	}
	return ports
}

func l2NodeOf(t *testing.T, nodes []l2Node, name string) l2Node {
	t.Helper()
	for _, node := range nodes {
		if node.name == name {
			return node
		}
	}
	t.Fatalf("unknown node %s", name)
	return l2Node{}
}

func assertL2Collision(t *testing.T, message, nodeA, nodeB, address, translator string) {
	t.Helper()
	for _, part := range []string{nodeA, nodeB, address, translator} {
		if !strings.Contains(message, part) {
			t.Fatalf("%q not in collision warning: %s", part, message)
		}
	}
}

func assertL2ServedByOwners(t *testing.T, client *as.Client, host string, nodes []l2Node, prefix string) {
	t.Helper()
	masters := as.PartitionMasters(client, *namespace)
	before := make(map[string]map[string]int64, len(nodes))
	for _, node := range nodes {
		before[node.name] = l2NamespaceStats(t, host, node)
	}
	write := as.NewWritePolicy(0, 0)
	write.ReplicaPolicy = as.MASTER
	read := as.NewPolicy()
	read.ReplicaPolicy = as.MASTER
	served := make(map[string]int64, len(nodes))
	cluster := client.Cluster()
	for i := range l2KeyCount {
		key := l2Key(t, prefix, i)
		owner := masters[key.PartitionId()]
		assertL2RoutedTo(t, cluster, &write.BasePolicy, read, key, owner)
		if err := client.Put(write, key, as.BinMap{"v": i}); err != nil {
			t.Fatalf("put %v: %v", key, err)
		}
		record, err := client.Get(read, key)
		if err != nil {
			t.Fatalf("get %v: %v", key, err)
		}
		if record.Bins["v"] != i {
			t.Fatalf("key %v holds %v, want %d", key, record.Bins["v"], i)
		}
		served[owner.GetName()]++
	}
	for _, node := range nodes {
		after := l2NamespaceStats(t, host, node)
		delta := func(stat string) int64 { return after[stat] - before[node.name][stat] }
		want := served[node.name]
		if want == 0 {
			t.Fatalf("node %s served no keys", node.name)
		}
		if writes := delta("client_write_success"); writes != want {
			t.Fatalf("node %s counted %d writes, want %d", node.name, writes, want)
		}
		if reads := delta("client_read_success"); reads != want {
			t.Fatalf("node %s counted %d reads, want %d", node.name, reads, want)
		}
		if proxied := delta("client_proxy_complete") + delta("client_proxy_error") + delta("client_proxy_timeout"); proxied != 0 {
			t.Fatalf("node %s proxied %d commands, want none", node.name, proxied)
		}
	}
}

func l2StatInt(value any) int64 {
	switch n := value.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

type l2ConnStats struct {
	closed int64
	errors int64
}

func l2ConnectionStats(t *testing.T, client *as.Client) map[string]l2ConnStats {
	t.Helper()
	raw, err := client.Stats()
	if err != nil {
		t.Fatal(err)
	}
	stats := make(map[string]l2ConnStats)
	for name, value := range raw {
		node, ok := value.(map[string]any)
		if !ok || name == "cluster-aggregated-stats" {
			continue
		}
		if _, ok := node["closed-connections"]; !ok {
			continue
		}
		stats[name] = l2ConnStats{
			closed: l2StatInt(node["closed-connections"]),
			errors: l2StatInt(node["transaction-error-count"]),
		}
	}
	return stats
}
