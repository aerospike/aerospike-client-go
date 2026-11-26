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

package aerospike

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const tlsFixtureFile = "tls/name-matching.yaml"

func tlsEnvRoots(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(payload) {
		t.Fatalf("no certificate in %s", path)
	}
	return roots
}

// Cases of the tls-matching environment whose expected outcome follows Java's
// legacy CN fallback. They take the outcome of the fixture row they test.
var tlsEnvCasesGradedByFixture = map[string]string{
	"legacy_cn_only":            "cn-only-no-san",
	"legacy_cn_nonmatching_dns": "cn-equal-dns-name-differs",
	"legacy_ip_as_dns":          "ip-id-only-as-dns-name",
	"ci_setup_aerospike_server": "ci-server-certificate-shape",
}

var tlsEnvCasesUnsupported = map[string]string{
	"revoked_serial": "the client has no list of revoked certificate serials",
}

const tlsEnvProxyRecords = 50

// TestTLSEnvMatching runs every case of the aerospike-client-conformance
// tls-matching environment: it starts the case's server, connects with the
// case's tlsName and compares the outcome with the expected one. Skips unless
// TLS_CASES is set, as it is by the environment's values file.
func TestTLSEnvMatching(t *testing.T) {
	cases := strings.TrimSpace(os.Getenv("TLS_CASES"))
	if cases == "" {
		t.Skip("set TLS_CASES by running in the aerospike-client-conformance tls-matching environment")
	}
	host := requiredTLSEnvValue(t, "ENV_HOST")
	namespace := requiredTLSEnvValue(t, "ENV_NAMESPACE")
	control := requiredTLSEnvValue(t, "ENV_CTL")
	fixtureOutcomes := tlsFixtureOutcomes(t)

	for _, caseName := range strings.Split(cases, ",") {
		t.Run(caseName, func(t *testing.T) {
			if reason, unsupported := tlsEnvCasesUnsupported[caseName]; unsupported {
				t.Skip(reason)
			}
			prefix := "TLS_" + strings.ToUpper(caseName) + "_"
			expected := expectedTLSEnvOutcome(t, caseName, prefix, fixtureOutcomes)
			tlsName := requiredTLSEnvValue(t, prefix+"NAME")
			port := tlsEnvPort(t, prefix+"PORT")
			roots := tlsEnvRoots(t, requiredTLSEnvValue(t, prefix+"CA"))

			if output, err := exec.Command(control, "serve", caseName).CombinedOutput(); err != nil {
				t.Fatalf("serve %s: %v\n%s", caseName, err, output)
			}
			t.Logf("case=%s tlsName=%s expected=%s", caseName, tlsName, expected)
			client, err := NewClientWithPolicyAndHost(tlsEnvClientPolicy(roots), &Host{Name: host, TLSName: tlsName, Port: port})
			if expected == "reject" {
				if err == nil {
					client.Close()
					t.Fatal("expected reject, got accept")
				}
				requireTLSNameMismatch(t, err, tlsName)
				return
			}
			if err != nil {
				t.Fatalf("expected accept, got %v", err)
			}
			defer client.Close()
			if err := putAndGetRecord(client, namespace, caseName); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestTLSEnvProxy seeds the two node proxy cluster of the tls-matching
// environment through node A's forwarder and learns node B from peers. The
// translator maps each node to its forwarder and also offers the forwarder
// host as TLS name, which the client must ignore: the certificate carries the
// cluster tls-name only as a dNSName. Skips unless PROXY_TLS_NAME is set.
func TestTLSEnvProxy(t *testing.T) {
	tlsName := strings.TrimSpace(os.Getenv("PROXY_TLS_NAME"))
	if tlsName == "" {
		t.Skip("set PROXY_TLS_NAME by running in the aerospike-client-conformance tls-matching environment")
	}
	namespace := requiredTLSEnvValue(t, "ENV_NAMESPACE")
	roots := tlsEnvRoots(t, requiredTLSEnvValue(t, "PROXY_CA"))
	forwarders := forwarderTranslator{}
	for _, node := range []string{"PROXY_NODE_A", "PROXY_NODE_B"} {
		forwarders[requiredTLSEnvValue(t, node+"_NAME")] = Host{
			Name: requiredTLSEnvValue(t, node+"_HOST"),
			Port: tlsEnvPort(t, node+"_PORT"),
		}
	}
	seed := forwarders[requiredTLSEnvValue(t, "PROXY_NODE_A_NAME")]

	policy := tlsEnvClientPolicy(roots)
	policy.AddressTranslator = forwarders
	client, err := NewClientWithPolicyAndHost(policy, &Host{Name: seed.Name, TLSName: tlsName, Port: seed.Port})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for deadline := time.Now().Add(policy.Timeout); len(client.GetNodes()) < len(forwarders) && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
	}
	nodes := client.GetNodes()
	if len(nodes) != len(forwarders) {
		t.Fatalf("held %d nodes, want %d", len(nodes), len(forwarders))
	}
	for _, node := range nodes {
		forwarder, known := forwarders[node.GetName()]
		host := node.GetHost()
		t.Logf("node=%s host=%s tlsName=%s", node.GetName(), host, host.TLSName)
		if !known {
			t.Fatalf("unexpected node %s", node.GetName())
		}
		if host.Name != forwarder.Name || host.Port != forwarder.Port || host.TLSName != tlsName {
			t.Errorf("node %s at %s tlsName %s, want %s:%d tlsName %s", node.GetName(), host, host.TLSName, forwarder.Name, forwarder.Port, tlsName)
		}
		info, err := node.RequestInfo(NewInfoPolicy(), "node")
		if err != nil {
			t.Fatalf("node %s info: %v", node.GetName(), err)
		}
		if info["node"] != node.GetName() {
			t.Errorf("node %s answered info node with %q", node.GetName(), info["node"])
		}
	}
	for record := 0; record < tlsEnvProxyRecords; record++ {
		if err := putAndGetRecord(client, namespace, "proxy-"+strconv.Itoa(record)); err != nil {
			t.Fatal(err)
		}
	}
}

// forwarderTranslator maps each node name to the forwarder in front of it.
type forwarderTranslator map[string]Host

func (forwarders forwarderTranslator) Translate(advertised Endpoint) (Host, bool) {
	forwarder, known := forwarders[advertised.NodeName]
	return Host{Name: forwarder.Name, Port: forwarder.Port, TLSName: forwarder.Name}, known
}

// tlsEnvClientPolicy validates server certificates against roots.
func tlsEnvClientPolicy(roots *x509.CertPool) *ClientPolicy {
	policy := NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.TlsConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	return policy
}

func requireTLSNameMismatch(t *testing.T, err error, tlsName string) {
	t.Helper()
	var mismatch x509.HostnameError
	if !errors.As(err, &mismatch) || mismatch.Host != tlsName {
		t.Fatalf("want a certificate name mismatch for %s, got %v", tlsName, err)
	}
}

// putAndGetRecord writes a record named name and reads the value back.
func putAndGetRecord(client *Client, namespace, name string) error {
	key, err := NewKey(namespace, "tls", name)
	if err != nil {
		return err
	}
	if err := client.Put(nil, key, BinMap{"v": name}); err != nil {
		return err
	}
	record, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	if record == nil || fmt.Sprint(record.Bins["v"]) != name {
		return fmt.Errorf("get of %s did not return the value put", name)
	}
	return nil
}

// expectedTLSEnvOutcome is the environment's expected outcome, or the fixture
// row's for a case graded by the fixture.
func expectedTLSEnvOutcome(t *testing.T, caseName, prefix string, fixtureOutcomes map[string]string) string {
	t.Helper()
	if fixtureCase, gradedByFixture := tlsEnvCasesGradedByFixture[caseName]; gradedByFixture {
		outcome, found := fixtureOutcomes[fixtureCase]
		if !found {
			t.Fatalf("%s: no row %s in %s", caseName, fixtureCase, tlsFixtureFile)
		}
		return outcome
	}
	outcome := requiredTLSEnvValue(t, prefix+"EXPECT")
	if outcome != "accept" && outcome != "reject" {
		t.Fatalf("%sEXPECT is %q, want accept or reject", prefix, outcome)
	}
	return outcome
}

// tlsFixtureOutcomes maps each live tls/name-matching.yaml row to its expected outcome.
func tlsFixtureOutcomes(t *testing.T) map[string]string {
	t.Helper()
	dir, err := conformanceFixtureDir(requiredTLSEnvValue(t, "CONFORMANCE_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := loadConformanceFile(filepath.Join(dir, tlsFixtureFile), tlsFixtureFile)
	if err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]string{}
	for _, row := range rows {
		if row.blockedBy == "" {
			outcomes[row.name] = asString(asMap(row.data["expect"])["outcome"])
		}
	}
	return outcomes
}

func requiredTLSEnvValue(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func tlsEnvPort(t *testing.T, name string) int {
	t.Helper()
	raw := requiredTLSEnvValue(t, name)
	port, err := strconv.Atoi(raw)
	if err != nil || port <= 0 || port > 65535 {
		t.Fatalf("%s must be a port, got %s", name, raw)
	}
	return port
}
