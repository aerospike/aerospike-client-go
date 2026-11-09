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
	"encoding/pem"
	"errors"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const (
	l3TlsNoKubeconfig = "Kubernetes TLS name needs a kind cluster running the Aerospike Kubernetes Operator: run test/discovery/run_l3.sh or the discovery-k8s workflow (DISCOVERY_KUBECONFIG is not set)"
	l3TlsProfile      = "Kubernetes TLS name needs DISCOVERY_TLS_CA_FILE and DISCOVERY_TLS_NAME, set once .github/discovery/enable-l3-tls.sh serves TLS on the AerospikeCluster (run_l3.sh L3_PROFILE=tls, or the discovery-k8s workflow)"
)

var l3TlsPeer = regexp.MustCompile(`\[([0-9A-Fa-f]+),([^,\[\]]*),\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\]`)

// Shared cert SAN is the cluster tls-name, not the endpoint hostname.
// Seed with that tls-name succeeds. Hostname as tls-name fails the handshake.
func TestL3TlsSharedCertificateCarriesTlsNameAndClientValidatesAgainstIt(t *testing.T) {
	l3RequireTlsCluster(t)
	cluster := l3FindCluster(t)
	endpoint := strings.TrimSpace(os.Getenv("DISCOVERY_ENDPOINT_HOSTNAME"))
	tlsName := strings.TrimSpace(os.Getenv("DISCOVERY_TLS_NAME"))
	if endpoint == "" {
		t.Fatal("DISCOVERY_ENDPOINT_HOSTNAME is not set")
	}
	if got := cluster.get(t, ".spec.aerospikeConfig.network.service['tls-name']"); got != tlsName {
		t.Fatalf("AerospikeCluster tls-name %q, want %s", got, tlsName)
	}
	if cluster.get(t, ".spec.aerospikeNetworkPolicy.alternateAccess") !=
		cluster.get(t, ".spec.aerospikeNetworkPolicy.tlsAlternateAccess") {
		t.Fatalf("tlsAlternateAccess %q, want alternateAccess %q",
			cluster.get(t, ".spec.aerospikeNetworkPolicy.tlsAlternateAccess"),
			cluster.get(t, ".spec.aerospikeNetworkPolicy.alternateAccess"))
	}

	cluster.ensureSize(t, l3MinPods)
	l3AssertTlsCertificate(t, cluster, tlsName, endpoint)

	tlsPort := l3Atoi(t, "tls-port", cluster.get(t, ".spec.aerospikeConfig.network.service['tls-port']"))
	pods := cluster.pods(t)
	if len(pods) < l3MinPods {
		t.Fatalf("%d pods, want at least %d", len(pods), l3MinPods)
	}
	podsByIP := make(map[string]l3Pod, len(pods))
	expected := make(map[string]string, len(pods))
	for _, pod := range pods {
		nodePort := cluster.nodePort(t, pod.name, tlsPort)
		t.Logf("L3-TLS-POD %s tlsServiceNodePort=%d", pod, nodePort)
		if nodePort <= 0 {
			t.Fatalf("pod %s has no Service NodePort for %d", pod.name, tlsPort)
		}
		podsByIP[pod.ip] = pod
		expected[pod.name] = net.JoinHostPort(endpoint, strconv.Itoa(nodePort))
	}

	policy := l3TlsPolicy(t)
	seedPort := cluster.nodePort(t, pods[0].name, tlsPort)
	seed := &as.Host{Name: endpoint, TLSName: tlsName, Port: seedPort}
	captureL2Logs(t)
	client := l3Client(t, policy, seed)

	var masters []*as.Node
	if !waitFor(l3RoutingWait, 500*time.Millisecond, func() bool {
		masters = as.PartitionMasters(client, l3Namespace())
		return len(client.GetNodes()) == len(pods) && l3Unowned(client, masters) == 0 && len(l3MasterNames(masters)) == len(pods)
	}) {
		masters = as.PartitionMasters(client, l3Namespace())
		t.Fatalf("Client seeded at %s:%d did not hold %d nodes owning every partition within %s: nodes=%d unowned=%d owners=%v",
			seed.Name, seed.Port, len(pods), l3RoutingWait, len(client.GetNodes()), l3Unowned(client, masters), l3MasterNames(masters))
	}

	nodeNames := map[string]struct{}{}
	advertisedPeers := map[string]struct{}{}
	matched := map[string]struct{}{}
	for _, node := range client.GetNodes() {
		nodeNames[node.GetName()] = struct{}{}
	}
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "service-tls-std", "service-tls-alt", "peers-tls-alt")
		if err != nil {
			t.Fatalf("info %s: %v", node.GetName(), err)
		}
		pod, ok := l3PodByAccess(t, podsByIP, info["service-tls-std"])
		if !ok {
			t.Fatalf("node %s TLS access %q matches no pod IP %v", node.GetName(), info["service-tls-std"], l3PodIPs(pods))
		}
		if _, dup := matched[pod.name]; dup {
			t.Fatalf("pod %s matched twice", pod.name)
		}
		matched[pod.name] = struct{}{}
		peers := l3ParseTlsPeers(info["peers-tls-alt"])
		t.Logf("L3-TLS-NAME pod=%s node=%s tlsAlternate=%s expected=%s clientTlsName=%s peersTlsAlt=%s",
			pod.name, node.GetName(), info["service-tls-alt"], expected[pod.name], node.GetHost().TLSName, info["peers-tls-alt"])
		if info["service-tls-alt"] != expected[pod.name] {
			t.Fatalf("pod %s TLS alternate address %q, want %s", pod.name, info["service-tls-alt"], expected[pod.name])
		}
		if node.GetHost().TLSName != tlsName {
			t.Fatalf("node %s client tls-name %q, want %s", node.GetName(), node.GetHost().TLSName, tlsName)
		}
		if len(peers) != len(pods)-1 {
			t.Fatalf("pod %s peers %s count %d, want %d", pod.name, info["peers-tls-alt"], len(peers), len(pods)-1)
		}
		for _, peer := range peers {
			if peer.tlsName != tlsName {
				t.Fatalf("pod %s tls-name for peer %s is %q, want %s", pod.name, peer.node, peer.tlsName, tlsName)
			}
			if _, ok := nodeNames[peer.node]; !ok {
				t.Fatalf("pod %s advertises unknown peer %s, nodes %v", pod.name, peer.node, l3NameSet(nodeNames))
			}
			advertisedPeers[peer.node] = struct{}{}
		}
	}
	if len(matched) != len(expected) {
		t.Fatalf("matched pods %v, want %v", l3NameSet(matched), l3NameSet(expectedKeys(expected)))
	}
	if !l3SameNames(nodeNames, advertisedPeers) {
		t.Fatalf("Nodes advertised as peers %v, want %v", l3NameSet(advertisedPeers), l3NameSet(nodeNames))
	}
	l3AssertEachNodeServes(t, client, masters)

	wrong := &as.Host{Name: endpoint, TLSName: endpoint, Port: seedPort}
	bad, err := as.NewClientWithPolicyAndHost(policy, wrong)
	if err == nil {
		bad.Close()
		t.Fatalf("TLS handshake with %s tls-name %s accepted a certificate that names only %s; nodes=%v",
			wrong.Name, wrong.TLSName, tlsName, bad.GetNodeNames())
	}
	var mismatch x509.HostnameError
	if !errors.As(err, &mismatch) || mismatch.Host != endpoint {
		t.Fatalf("TLS seed with tls-name %s failed with %v, want a certificate name mismatch", endpoint, err)
	}
	t.Logf("L3-TLS-FQDN-REJECTED seed=%s:%d tls-name=%s error=%v", wrong.Name, wrong.Port, wrong.TLSName, err)
}

func l3RequireTlsCluster(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("DISCOVERY_KUBECONFIG")) == "" {
		t.Fatal(l3TlsNoKubeconfig)
	}
	if strings.TrimSpace(os.Getenv("DISCOVERY_TLS_CA_FILE")) == "" || strings.TrimSpace(os.Getenv("DISCOVERY_TLS_NAME")) == "" {
		t.Fatal(l3TlsProfile)
	}
}

func l3TlsPolicy(t *testing.T) *as.ClientPolicy {
	t.Helper()
	policy := l3Policy()
	policy.TlsConfig = l3TlsConfig(t)
	return policy
}

func l3TlsConfig(t *testing.T) *tls.Config {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("DISCOVERY_TLS_CA_FILE"))
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		t.Fatalf("no certificate in %s", path)
	}
	return &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
}

func l3AssertTlsCertificate(t *testing.T, cluster l3Cluster, tlsName, endpoint string) {
	t.Helper()
	certFile := cluster.get(t, `.spec.aerospikeConfig.network.tls[?(@.name=="`+tlsName+`")]['cert-file']`)
	index := strings.LastIndex(certFile, "/")
	if index <= 0 {
		t.Fatalf("cert-file %q", certFile)
	}
	dir := certFile[:index]
	secret := cluster.get(t, `.spec.storage.volumes[?(@.aerospike.path=="`+dir+`")].source.secret.secretName`)
	if secret == "" {
		t.Fatalf("No secret volume at %s", dir)
	}
	block, rest := pem.Decode(cluster.secretData(t, secret, certFile[index+1:]))
	if block == nil {
		t.Fatalf("secret %s key %s is not PEM", secret, certFile[index+1:])
	}
	_ = rest
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	sans := make([]string, 0, len(cert.DNSNames)+len(cert.IPAddresses))
	for _, name := range cert.DNSNames {
		sans = append(sans, "dns:"+name)
	}
	for _, ip := range cert.IPAddresses {
		sans = append(sans, "ip:"+ip.String())
	}
	t.Logf("L3-TLS-CERT secret=%s/%s key=%s subject=%s sans=%v dnsNames=%v endpoint=%s",
		cluster.namespace, secret, certFile[index+1:], cert.Subject, sans, cert.DNSNames, endpoint)
	if !slices.Equal(cert.DNSNames, []string{tlsName}) {
		t.Fatalf("dNSName SANs %v, want [%s]", cert.DNSNames, tlsName)
	}
	for _, name := range cert.DNSNames {
		if strings.EqualFold(name, endpoint) {
			t.Fatalf("SAN dns:%s names the endpoint %s", name, endpoint)
		}
	}
	for _, ip := range cert.IPAddresses {
		if strings.EqualFold(ip.String(), endpoint) {
			t.Fatalf("SAN ip:%s names the endpoint %s", ip, endpoint)
		}
	}
}

type l3TlsPeerName struct {
	node    string
	tlsName string
}

func l3ParseTlsPeers(reply string) []l3TlsPeerName {
	var peers []l3TlsPeerName
	for _, match := range l3TlsPeer.FindAllStringSubmatch(reply, -1) {
		peers = append(peers, l3TlsPeerName{node: match[1], tlsName: match[2]})
	}
	return peers
}

func l3NameSet(names map[string]struct{}) []string {
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

func expectedKeys(expected map[string]string) map[string]struct{} {
	names := make(map[string]struct{}, len(expected))
	for name := range expected {
		names[name] = struct{}{}
	}
	return names
}

func l3SameNames(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for name := range a {
		if _, ok := b[name]; !ok {
			return false
		}
	}
	return true
}
