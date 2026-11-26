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
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const tlsFixtureFile = "tls/name-matching.yaml"

type tlsMatchingResult struct {
	Case   string `json:"case"`
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

// TestTlsMatchingCertificates writes a CA, server certificate and key for each
// tls/name-matching.yaml shape that has a live row, one directory per shape. The
// server certificate takes its subject CN, DNS names and IP addresses from the
// fixture's certs/<shape>.pem. shapes.tsv lists each shape with its CN, which the
// server uses as its tls-name. Skips unless TLS_MATCHING_CERTIFICATES is set.
func TestTlsMatchingCertificates(t *testing.T) {
	out := strings.TrimSpace(os.Getenv("TLS_MATCHING_CERTIFICATES"))
	if out == "" {
		t.Skip("set TLS_MATCHING_CERTIFICATES to write one certificate set per live tls/name-matching.yaml shape")
	}
	shapes := tlsMatchingLiveShapes(tlsMatchingRows(t))
	if len(shapes) == 0 {
		t.Fatalf("no live rows in %s", tlsFixtureFile)
	}
	names := make([]string, 0, len(shapes))
	for name := range shapes {
		names = append(names, name)
	}
	sort.Strings(names)

	var list strings.Builder
	for _, name := range names {
		if err := writeTlsMatchingCertificates(filepath.Join(out, name), shapes[name]); err != nil {
			t.Fatalf("shape %s: %v", name, err)
		}
		fmt.Fprintf(&list, "%s\t%s\n", name, shapes[name].Subject.CommonName)
	}
	if err := os.WriteFile(filepath.Join(out, "shapes.tsv"), []byte(list.String()), 0644); err != nil {
		t.Fatal(err)
	}
}

// TestTlsMatching runs the live tls/name-matching.yaml rows of one shape against
// an Enterprise server that serves that shape's certificate. An accept row passes
// when the client connects and a put and get succeed. A reject row passes when the
// client fails with a certificate name mismatch for the row's tlsName. Appends
// each outcome to TLS_MATCHING_RESULTS for TestTlsMatchingReport. Skips unless
// TLS_MATCHING_SHAPE is set.
func TestTlsMatching(t *testing.T) {
	shape := strings.TrimSpace(os.Getenv("TLS_MATCHING_SHAPE"))
	if shape == "" {
		t.Skip("set TLS_MATCHING_SHAPE to run the live tls/name-matching.yaml rows of that shape")
	}
	host := strings.TrimSpace(os.Getenv("TLS_MATCHING_HOST"))
	caPath := strings.TrimSpace(os.Getenv("TLS_MATCHING_CA"))
	resultsPath := strings.TrimSpace(os.Getenv("TLS_MATCHING_RESULTS"))
	if host == "" || caPath == "" || resultsPath == "" {
		t.Fatal("TLS_MATCHING_HOST, TLS_MATCHING_CA and TLS_MATCHING_RESULTS are required")
	}
	port := tlsMatchingPort(t)
	roots := tlsEnvRoots(t, caPath)

	var ran int
	for _, row := range tlsMatchingRows(t) {
		if row.blockedBy != "" || tlsMatchingShape(row) != shape {
			continue
		}
		ran++
		t.Run(row.file+":"+row.name, func(t *testing.T) {
			actual := tlsMatchingOutcome(row, host, port, roots)
			if err := appendTlsMatchingResult(resultsPath, row, actual); err != nil {
				t.Fatal(err)
			}
			if mismatch := conformanceMismatch(row, actual); mismatch != "" {
				t.Error(mismatch)
			}
		})
	}
	if ran == 0 {
		t.Fatalf("no live rows for shape %s in %s", shape, tlsFixtureFile)
	}
}

// TestTlsMatchingReport writes conformance-report.json for tls/name-matching.yaml
// from the outcomes in TLS_MATCHING_RESULTS, with blocked rows recorded from
// blockedBy. A live row with no outcome fails. Also writes go-declared.yaml next
// to the report with only the tls/name-matching.yaml declarations, because the
// collect action rejects declarations for cases outside the fixtures it is given.
// Skips unless TLS_MATCHING_RESULTS is set.
func TestTlsMatchingReport(t *testing.T) {
	resultsPath := strings.TrimSpace(os.Getenv("TLS_MATCHING_RESULTS"))
	if resultsPath == "" {
		t.Skip("set TLS_MATCHING_RESULTS to write conformance-report.json for tls/name-matching.yaml")
	}
	rows := tlsMatchingRows(t)
	results, err := readTlsMatchingResults(resultsPath)
	if err != nil {
		t.Fatal(err)
	}

	reportRows := make([]conformanceReportRow, 0, len(rows))
	for _, row := range rows {
		actual, ok := results[row.name]
		switch {
		case row.blockedBy != "":
			actual = conformanceOutcome{conformanceBlocked, row.blockedBy}
		case !ok:
			actual = conformanceOutcome{conformanceFail, "no live result"}
			t.Errorf("%s:%s has no live result", row.file, row.name)
		}
		reportRows = append(reportRows, conformanceReportEntry(row, actual))
	}
	if err := writeConformanceReport(reportRows); err != nil {
		t.Fatal(err)
	}
	if err := writeTlsMatchingDeclarations(filepath.Dir(conformanceReportPath()), rows); err != nil {
		t.Fatal(err)
	}
}

// tlsMatchingRows loads every fixture row so that go-declared.yaml validates in
// full, applies the declarations and keeps the tls/name-matching.yaml rows.
func tlsMatchingRows(t *testing.T) []*conformanceRow {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CONFORMANCE_DIR"))
	if raw == "" {
		t.Fatal("CONFORMANCE_DIR is required")
	}
	dir, err := conformanceFixtureDir(raw)
	if err != nil {
		t.Fatal(err)
	}
	all, err := loadConformanceRows(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyConformanceDeclarations(all); err != nil {
		t.Fatal(err)
	}
	var rows []*conformanceRow
	for _, row := range all {
		if row.file == tlsFixtureFile {
			rows = append(rows, row)
		}
	}
	if len(rows) == 0 {
		t.Fatalf("no rows in %s", tlsFixtureFile)
	}
	return rows
}

func tlsMatchingShape(row *conformanceRow) string {
	return asString(asMap(row.data["input"])["shape"])
}

// tlsMatchingLiveShapes maps each shape that has a live row to its fixture certificate.
func tlsMatchingLiveShapes(rows []*conformanceRow) map[string]*x509.Certificate {
	shapes := map[string]*x509.Certificate{}
	for _, row := range rows {
		if row.blockedBy == "" {
			shapes[tlsMatchingShape(row)] = row.certificate
		}
	}
	return shapes
}

// writeTlsMatchingCertificates writes ca.crt, server.crt and server.key to dir.
// The server certificate has the names of shape and is signed by a new CA.
func writeTlsMatchingCertificates(dir string, shape *x509.Certificate) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Aerospike-Server-Test-CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: shape.Subject.CommonName},
		DNSNames:     shape.DNSNames,
		IPAddresses:  shape.IPAddresses,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return err
	}

	files := []struct {
		name, kind string
		der        []byte
	}{
		{"ca.crt", "CERTIFICATE", caDER},
		{"server.crt", "CERTIFICATE", serverDER},
		{"server.key", "PRIVATE KEY", keyDER},
	}
	for _, file := range files {
		payload := pem.EncodeToMemory(&pem.Block{Type: file.kind, Bytes: file.der})
		if err := os.WriteFile(filepath.Join(dir, file.name), payload, 0644); err != nil {
			return err
		}
	}
	return nil
}

// tlsMatchingOutcome connects with the row's tlsName. On accept it also does a
// put and get on namespace test.
func tlsMatchingOutcome(row *conformanceRow, host string, port int, roots *x509.CertPool) conformanceOutcome {
	tlsName := asString(asMap(row.data["input"])["tlsName"])
	expected := asString(asMap(row.data["expect"])["outcome"])
	policy := NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.TlsConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	client, err := NewClientWithPolicyAndHost(policy, &Host{Name: host, TLSName: tlsName, Port: port})
	if err != nil {
		if expected == "reject" {
			var mismatch x509.HostnameError
			if !errors.As(err, &mismatch) || mismatch.Host != tlsName {
				return conformanceOutcome{conformanceFail, "want a certificate name mismatch for " + tlsName + ", got " + err.Error()}
			}
			return conformanceOutcome{conformancePass, ""}
		}
		return conformanceOutcome{conformanceFail, "expected accept, got " + err.Error()}
	}
	defer client.Close()
	if expected == "reject" {
		return conformanceOutcome{conformanceFail, "expected reject, got accept"}
	}
	key, keyErr := NewKey("test", "tls", row.name)
	if keyErr != nil {
		return conformanceOutcome{conformanceFail, keyErr.Error()}
	}
	if putErr := client.Put(nil, key, BinMap{"v": row.name}); putErr != nil {
		return conformanceOutcome{conformanceFail, putErr.Error()}
	}
	record, getErr := client.Get(nil, key)
	if getErr != nil {
		return conformanceOutcome{conformanceFail, getErr.Error()}
	}
	if record == nil || fmt.Sprint(record.Bins["v"]) != row.name {
		return conformanceOutcome{conformanceFail, "put succeeded, get did not return the value"}
	}
	return conformanceOutcome{conformancePass, ""}
}

// writeTlsMatchingDeclarations writes go-declared.yaml to dir with the
// declarations of rows that differ from the default result.
func writeTlsMatchingDeclarations(dir string, rows []*conformanceRow) error {
	cases := map[string]map[string]string{}
	for _, row := range rows {
		standard := conformancePass
		if row.blockedBy != "" {
			standard = conformanceBlocked
		}
		if row.declared.result == standard && row.declared.reason == "" {
			continue
		}
		declaration := map[string]string{"result": row.declared.result}
		if row.declared.reason != "" {
			declaration["reason"] = row.declared.reason
		}
		cases[row.name] = declaration
	}
	doc := map[string]any{}
	if len(cases) > 0 {
		doc[tlsFixtureFile] = cases
	}
	payload, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, conformanceDeclaredFile), payload, 0644)
}

func tlsMatchingPort(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("TLS_MATCHING_PORT"))
	if raw == "" {
		t.Fatal("TLS_MATCHING_PORT is required")
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port <= 0 {
		t.Fatalf("TLS_MATCHING_PORT must be a port, got %s", raw)
	}
	return port
}

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

func appendTlsMatchingResult(path string, row *conformanceRow, actual conformanceOutcome) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(tlsMatchingResult{Case: row.name, Result: actual.result, Reason: actual.reason})
}

func readTlsMatchingResults(path string) (map[string]conformanceOutcome, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	results := map[string]conformanceOutcome{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var result tlsMatchingResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		results[result.Case] = conformanceOutcome{result.Result, result.Reason}
	}
	return results, scanner.Err()
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
