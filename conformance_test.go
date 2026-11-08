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
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"gopkg.in/yaml.v3"
)

const (
	conformancePass        = "pass"
	conformanceFail        = "fail"
	conformanceUnsupported = "unsupported"
	conformanceBlocked     = "blocked"

	conformanceFixtureSetVersion = "1.1.0"
	conformanceProviderTimeoutMs = 2000
	conformanceFixtureServer     = "discovery/providers/fixture-server.py"
	conformanceAccessKey         = "AKIDCONFORMANCE00000"
	conformanceSecretKey         = "conformance-secret-key-not-real"
	conformanceSessionToken      = "conformance-session-token-not-real"
	conformanceDeclaredFile      = "go-declared.yaml"
	conformanceReportFile        = "conformance-report.json"
)

var conformanceErrorMarkers = map[string]string{
	"discovery-failed": "DNS seed discovery failed for every name",
	"aws-error":        "DescribeInstances failed",
	"timeout":          "timed out",

	"translation-collision": "to the same address",
}

var conformanceResultNames = map[string]struct{}{
	conformancePass:        {},
	conformanceFail:        {},
	conformanceUnsupported: {},
	conformanceBlocked:     {},
}

type conformanceRow struct {
	file        string
	kind        string
	name        string
	blockedBy   string
	data        map[string]any
	certificate *x509.Certificate
	declared    conformanceDeclaration
}

type conformanceDeclaration struct {
	result string
	reason string
}

type conformanceOutcome struct {
	result string
	reason string
}

type conformanceReportRow struct {
	File   string `json:"file"`
	Case   string `json:"case"`
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type conformanceCounts struct {
	Pass        int `json:"pass"`
	Fail        int `json:"fail"`
	Unsupported int `json:"unsupported"`
	Blocked     int `json:"blocked"`
}

type conformanceReport struct {
	Runtime           string                 `json:"runtime"`
	ClientVersion     string                 `json:"clientVersion"`
	FixtureSetVersion string                 `json:"fixtureSetVersion"`
	Counts            conformanceCounts      `json:"counts"`
	Rows              []conformanceReportRow `json:"rows"`
}

type conformanceRunner struct {
	dir      string
	httpPort int
	dnsPort  int
	http     *http.Client
}

type conformanceSeedScript struct {
	results [][]*Host
	next    int
}

func (provider *conformanceSeedScript) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	if provider.next >= len(provider.results) {
		return nil, fmt.Errorf("no scripted discovery result left")
	}
	hosts := provider.results[provider.next]
	provider.next++
	return hosts, nil
}

func (*conformanceSeedScript) NeedsPeriodicRefresh() bool { return true }
func (*conformanceSeedScript) SupportsDeadline() bool     { return false }
func (*conformanceSeedScript) Close()                     {}

func TestConformance(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("CONFORMANCE_DIR"))
	if raw == "" {
		t.Skip("set CONFORMANCE_DIR to an aerospike-client-conformance 1.1.0 checkout")
	}
	dir, err := conformanceFixtureDir(raw)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := loadConformanceRows(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyConformanceDeclarations(rows); err != nil {
		t.Fatal(err)
	}

	runner := &conformanceRunner{dir: dir}
	if needsFixtureServer(rows) {
		if err := runner.startFixtureServer(t); err != nil {
			t.Fatal(err)
		}
	}

	reportRows := make([]conformanceReportRow, 0, len(rows))
	t.Cleanup(func() {
		if writeErr := writeConformanceReport(conformanceFixtureSetVersion, reportRows); writeErr != nil {
			t.Errorf("conformance report: %v", writeErr)
		}
	})

	for _, row := range rows {
		t.Run(row.file+":"+row.name, func(t *testing.T) {
			actual := runner.execute(row)
			reportRows = append(reportRows, conformanceReportEntry(row, actual))
			if mismatch := conformanceMismatch(row, actual); mismatch != "" {
				t.Error(mismatch)
			}
		})
	}
}

// conformanceFixtureDir returns the absolute path of dir after checking that it
// is a conformance checkout at conformanceFixtureSetVersion.
func conformanceFixtureDir(dir string) (string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("conformance directory: %w", err)
	}
	if info, statErr := os.Stat(dir); statErr != nil || !info.IsDir() {
		return "", fmt.Errorf("conformance directory not found: %s", dir)
	}
	versionBytes, err := os.ReadFile(filepath.Join(dir, "VERSION"))
	if err != nil {
		return "", fmt.Errorf("not a conformance fixture set, VERSION not found: %s", dir)
	}
	if got := strings.TrimSpace(string(versionBytes)); got != conformanceFixtureSetVersion {
		return "", fmt.Errorf("conformance fixture set %s found, %s expected: check out that release or update the declarations",
			got, conformanceFixtureSetVersion)
	}
	return dir, nil
}

// conformanceReportEntry is the report row for an outcome. A declared reason
// replaces the actual one when the results agree, and the actual reason moves
// to detail.
func conformanceReportEntry(row *conformanceRow, actual conformanceOutcome) conformanceReportRow {
	reason := actual.reason
	if row.declared.reason != "" && row.declared.result == actual.result {
		reason = row.declared.reason
	}
	entry := conformanceReportRow{File: row.file, Case: row.name, Result: actual.result, Reason: reason}
	if actual.reason != "" && actual.reason != reason {
		entry.Detail = actual.reason
	}
	return entry
}

// conformanceMismatch describes an outcome that differs from the declared
// result, or returns "" when they agree.
func conformanceMismatch(row *conformanceRow, actual conformanceOutcome) string {
	if actual.result == row.declared.result {
		return ""
	}
	detail := ""
	if actual.reason != "" {
		detail = ": " + actual.reason
	}
	return fmt.Sprintf("declared %s, actual %s%s", row.declared.result, actual.result, detail)
}

func (runner *conformanceRunner) execute(row *conformanceRow) conformanceOutcome {
	if row.blockedBy != "" {
		return conformanceOutcome{conformanceBlocked, row.blockedBy}
	}

	switch row.kind {
	case "translate":
		return row.translate()
	case "collision":
		return row.collision()
	case "merge":
		return row.merge()
	case "tls":
		return row.tls()
	case "dns":
		provider, err := row.dnsProvider(runner.dnsPort)
		if err != nil {
			return conformanceOutcome{conformanceFail, err.Error()}
		}
		return runner.provider(row, provider)
	case "aws-tags":
		provider, err := row.awsProvider(runner.httpPort)
		if err != nil {
			return conformanceOutcome{conformanceFail, err.Error()}
		}
		return runner.provider(row, provider)
	default:
		return conformanceOutcome{conformanceFail, "unknown kind " + row.kind}
	}
}

func (row *conformanceRow) translation() (AddressTranslator, TranslationMissMode, *conformanceOutcome) {
	config := asMap(row.data["config"])
	translation := asMap(config["translation"])
	onMiss := TranslationMissPassThrough
	if translation == nil {
		return NewStaticMapAddressTranslator(stringMap(config["ip_map"])), onMiss, nil
	}
	if token, ok := translation["on_miss"].(string); ok {
		parsed, err := parseTranslationMissMode(token)
		if err != nil {
			return nil, onMiss, &conformanceOutcome{conformanceFail, err.Error()}
		}
		onMiss = parsed
	}

	provider, _ := translation["provider"].(string)
	switch provider {
	case translatorNameStaticMap:
		return NewStaticMapAddressTranslator(stringMap(translation["static_map"])), onMiss, nil
	case translatorNamePassThrough:
		return NewPassThroughAddressTranslator(), onMiss, nil
	case translatorNamePortMap:
		parsed, err := NewPortMapAddressTranslator(stringMap(translation["port_map"]))
		if err != nil {
			return nil, onMiss, &conformanceOutcome{conformanceFail, err.Error()}
		}
		return parsed, onMiss, nil
	default:
		return nil, onMiss, &conformanceOutcome{conformanceUnsupported, "translator " + provider + " is not implemented"}
	}
}

func (row *conformanceRow) translate() conformanceOutcome {
	translator, onMiss, failed := row.translation()
	if failed != nil {
		return *failed
	}
	input := asMap(row.data["input"])
	expect := asMap(row.data["expect"])

	sourceList := SourceListStandard
	if asString(input["sourceList"]) == "alternate" {
		sourceList = SourceListAlternate
	}
	advertised := Endpoint{
		NodeName:   asString(input["nodeName"]),
		TLSName:    asString(input["tlsName"]),
		Host:       asString(input["host"]),
		Port:       asInt(input["port"]),
		SourceList: sourceList,
	}

	actual, err := translate(translator, onMiss, advertised, nil)
	errorExpect := asMap(expect["error"])
	if err != nil {
		if !err.Matches(types.TRANSLATION_MISS) {
			return conformanceOutcome{conformanceFail, "threw " + err.Error()}
		}
		if errorExpect == nil || asString(errorExpect["category"]) != "translation-miss" {
			return conformanceOutcome{conformanceFail, "threw " + err.Error()}
		}
		for _, fact := range messageContains(errorExpect["messageContains"]) {
			if !strings.Contains(err.Error(), fact) {
				return conformanceOutcome{conformanceFail, "message does not contain " + fact + ": " + err.Error()}
			}
		}
		return conformanceOutcome{conformancePass, ""}
	}

	if errorExpect != nil {
		return conformanceOutcome{conformanceFail, "expected " + asString(errorExpect["category"]) + " error, got " +
			endpointString(actual.Name, actual.Port, actual.TLSName)}
	}

	expected := endpointString(asString(expect["host"]), asInt(expect["port"]), asString(expect["tlsName"]))
	got := endpointString(actual.Name, actual.Port, actual.TLSName)
	if expected != got {
		return conformanceOutcome{conformanceFail, "expected " + expected + ", got " + got}
	}
	return conformanceOutcome{conformancePass, ""}
}

func (row *conformanceRow) collision() conformanceOutcome {
	translator, onMiss, failed := row.translation()
	if failed != nil {
		return *failed
	}
	expect := asMap(row.data["expect"])

	var names, entries []string
	for _, item := range asList(row.data["input"]) {
		peer := asMap(item)
		names = append(names, asString(peer["nodeName"]))
		entries = append(entries, fmt.Sprintf("[%s,%s,[%s]]", asString(peer["nodeName"]), asString(peer["tlsName"]),
			net.JoinHostPort(asString(peer["host"]), asString(peer["port"]))))
	}

	server, err := newFakeInfoServer()
	if err != nil {
		return conformanceOutcome{conformanceFail, err.Error()}
	}
	defer server.close()
	server.setReply("peers-clear-std", "1,3000,[]")

	policy := NewClientPolicy()
	policy.AddressTranslator = translator
	policy.TranslationOnMiss = onMiss
	policy.Timeout = 200 * time.Millisecond
	policy.TendInterval = time.Hour
	previousLookup := lookupHost
	lookupHost = func(name string) ([]string, error) {
		return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
	}
	defer func() { lookupHost = previousLookup }()

	capture := &warnLogCapture{}
	previousLogger := logger.Logger.Logger
	logger.Logger.SetLogger(capture)
	logger.Logger.SetLevel(logger.WARNING)
	defer func() {
		logger.Logger.SetLogger(previousLogger)
		logger.Logger.SetLevel(logger.OFF)
	}()

	client, clientErr := NewClientWithPolicyAndHost(policy, server.host())
	if clientErr != nil {
		return conformanceOutcome{conformanceFail, clientErr.Error()}
	}
	defer client.Close()

	// One new peers reply, tended once on this goroutine. Translators ignore
	// sourceList, so alternate rows are served on the standard list.
	server.setReply("peers-generation", "2")
	server.setReply("peers-clear-std", "2,3000,["+strings.Join(entries, ",")+"]")
	_ = client.cluster.tend()

	var collisions []string
	for _, message := range capture.msgs {
		if strings.Contains(message, conformanceErrorMarkers["translation-collision"]) {
			collisions = append(collisions, message)
		}
	}
	reports := func(message string, item any) bool {
		for _, fact := range messageContains(asMap(item)["messageContains"]) {
			if !strings.Contains(message, fact) {
				return false
			}
		}
		return true
	}
	expectErrors := asList(expect["errors"])
	for _, item := range expectErrors {
		if !slices.ContainsFunc(collisions, func(message string) bool { return reports(message, item) }) {
			return conformanceOutcome{conformanceFail, fmt.Sprintf("no collision reported naming %v, got %v", asMap(item)["messageContains"], collisions)}
		}
	}
	for _, message := range collisions {
		if !slices.ContainsFunc(expectErrors, func(item any) bool { return reports(message, item) }) {
			return conformanceOutcome{conformanceFail, "unexpected collision: " + message}
		}
	}

	var nodes []string
	for _, name := range names {
		if !slices.ContainsFunc(collisions, func(message string) bool { return strings.Contains(message, name) }) {
			nodes = append(nodes, name)
		}
	}
	expNodes := stringList(expect["nodes"])
	slices.Sort(nodes)
	slices.Sort(expNodes)
	if !slices.Equal(nodes, expNodes) {
		return conformanceOutcome{conformanceFail, fmt.Sprintf("expected nodes %v, got %v", expNodes, nodes)}
	}
	return conformanceOutcome{conformancePass, ""}
}

func (row *conformanceRow) merge() conformanceOutcome {
	config := asMap(row.data["config"])
	discovery := asMap(config["discovery"])
	policy := NewClientPolicy()
	if discovery != nil && discovery["merge_policy"] != nil {
		mergePolicy, err := parseSeedMergePolicy(asString(discovery["merge_policy"]))
		if err != nil {
			return conformanceOutcome{conformanceFail, err.Error()}
		}
		policy.SeedMergePolicy = mergePolicy
	}

	var results [][]*Host
	for _, result := range asList(asMap(row.data["input"])["discoveryResults"]) {
		results = append(results, hostsFromYAML(result))
	}

	snapshots, err := refreshSeeds(policy, hostsFromYAML(config["seeds"]), results)
	if err != nil {
		return conformanceOutcome{conformanceFail, err.Error()}
	}

	expected := asList(asMap(row.data["expect"])["snapshots"])
	for i, item := range expected {
		exp := seedListString(hostsFromYAML(item))
		got := seedListString(snapshots[i])
		if exp != got {
			return conformanceOutcome{conformanceFail, fmt.Sprintf("snapshot %d: expected %s, got %s", i, exp, got)}
		}
	}
	return conformanceOutcome{conformancePass, ""}
}

func (row *conformanceRow) tls() conformanceOutcome {
	tlsName := asString(asMap(row.data["input"])["tlsName"])
	expected := asString(asMap(row.data["expect"])["outcome"])
	got := "accept"
	if err := row.certificate.VerifyHostname(tlsName); err != nil {
		got = "reject"
	}
	if expected != got {
		return conformanceOutcome{conformanceFail, "expected " + expected + ", got " + got}
	}
	return conformanceOutcome{conformancePass, ""}
}

func (row *conformanceRow) dnsProvider(dnsPort int) (SeedCandidateProvider, error) {
	dns := asMap(asMap(asMap(row.data["config"])["discovery"])["dns"])
	config := DnsSeedCandidateProviderConfig{
		Names:   stringList(dns["names"]),
		Timeout: time.Duration(intOr(dns["timeout_ms"], conformanceProviderTimeoutMs)) * time.Millisecond,
		Servers: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(dnsPort))},
	}
	if dns["record_type"] != nil {
		recordType, err := parseDnsRecordType(asString(dns["record_type"]))
		if err != nil {
			return nil, err
		}
		config.RecordType = recordType
	}
	if dns["port"] != nil {
		config.Port = asInt(dns["port"])
	}
	if dns["tls_name"] != nil {
		config.TLSName = asString(dns["tls_name"])
	}
	return NewDnsSeedCandidateProvider(config)
}

func (row *conformanceRow) awsProvider(httpPort int) (SeedCandidateProvider, error) {
	aws := asMap(asMap(asMap(row.data["config"])["discovery"])["aws"])
	config := AwsEc2TagSeedCandidateProviderConfig{
		Region:   asString(aws["region"]),
		Timeout:  time.Duration(intOr(aws["timeout_ms"], conformanceProviderTimeoutMs)) * time.Millisecond,
		Endpoint: fmt.Sprintf("http://127.0.0.1:%d", httpPort),
		Credentials: &AwsCredentials{
			AccessKeyID:     conformanceAccessKey,
			SecretAccessKey: conformanceSecretKey,
			SessionToken:    conformanceSessionToken,
		},
	}
	for _, item := range asList(aws["filters"]) {
		filter := asMap(item)
		config.Filters = append(config.Filters, AwsFilter{
			Name:   asString(filter["name"]),
			Values: stringList(filter["values"]),
		})
	}
	if aws["address_field"] != nil {
		field, err := parseAwsAddressField(asString(aws["address_field"]))
		if err != nil {
			return nil, err
		}
		config.AddressField = field
	}
	if aws["port"] != nil {
		config.Port = asInt(aws["port"])
	}
	if aws["tls_name"] != nil {
		config.TLSName = asString(aws["tls_name"])
	}
	return NewAwsEc2TagSeedCandidateProvider(config)
}

func (runner *conformanceRunner) provider(row *conformanceRow, provider SeedCandidateProvider) conformanceOutcome {
	config := asMap(asMap(row.data["config"])["discovery"])
	region := asString(asMap(config["aws"])["region"])
	refreshes := asList(asMap(row.data["input"])["refreshes"])
	expected := asList(asMap(row.data["expect"])["refreshes"])

	steps, err := refreshProvider(provider, len(refreshes), func(i int) error {
		return runner.selectRefresh(refreshes[i], region)
	})
	if err != nil {
		return conformanceOutcome{conformanceFail, err.Error()}
	}

	for i, item := range expected {
		exp := asMap(item)
		step := steps[i]
		errorExpect := asMap(exp["error"])
		if errorExpect != nil {
			if step.err == "" {
				return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: expected %s, got %s", i, asString(errorExpect["category"]), hostSet(step.hosts))}
			}
			facts := messageContains(errorExpect["messageContains"])
			if marker := conformanceErrorMarkers[asString(errorExpect["category"])]; marker != "" {
				facts = append([]string{marker}, facts...)
			}
			for _, fact := range facts {
				if !strings.Contains(step.err, fact) {
					return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: message lacks %q: %s", i, fact, step.err)}
				}
			}
			for _, secret := range []string{conformanceSecretKey, conformanceSessionToken} {
				if strings.Contains(step.err, secret) {
					return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: message contains a credential secret", i)}
				}
			}
		} else if step.err != "" {
			return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: failed: %s", i, step.err)}
		} else {
			expHosts := hostSet(hostsFromYAML(exp["hosts"]))
			got := hostSet(step.hosts)
			if expHosts != got {
				return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: expected hosts %s, got %s", i, expHosts, got)}
			}
		}

		expSeeds := hostSet(hostsFromYAML(exp["seeds"]))
		gotSeeds := hostSet(step.seeds)
		if expSeeds != gotSeeds {
			return conformanceOutcome{conformanceFail, fmt.Sprintf("refresh %d: expected seeds %s, got %s", i, expSeeds, gotSeeds)}
		}
	}
	return conformanceOutcome{conformancePass, ""}
}

type conformanceStep struct {
	hosts []*Host
	err   string
	seeds []*Host
}

func refreshSeeds(policy *ClientPolicy, staticSeeds []*Host, results [][]*Host) ([][]*Host, error) {
	policy.SeedProvider = &conformanceSeedScript{results: results}
	policy.DiscoveryExecutionMode = DiscoveryExecutionThread

	var snapshot []*Host
	refresher, err := newSeedRefresher(policy, staticSeeds, func(hosts []*Host) {
		snapshot = copyHosts(hosts)
	})
	if err != nil {
		return nil, err
	}
	defer refresher.close()

	discovered, refreshErr := refresher.refreshOnce(0)
	seeds, startErr := refresher.resolveStartupSeeds(discovered, refreshErr)
	if startErr != nil {
		return nil, startErr
	}
	snapshot = copyHosts(seeds)

	snapshots := make([][]*Host, 0, len(results))
	snapshots = append(snapshots, copyHosts(snapshot))
	for i := 1; i < len(results); i++ {
		discovered, refreshErr = refresher.refreshOnce(int64(i))
		if refreshErr == nil {
			if merged := refresher.merge(discovered, false); len(merged) > 0 {
				snapshot = copyHosts(merged)
			}
		}
		snapshots = append(snapshots, copyHosts(snapshot))
	}
	return snapshots, nil
}

func refreshProvider(provider SeedCandidateProvider, refreshes int, selectRefresh func(int) error) ([]conformanceStep, error) {
	policy := NewClientPolicy()
	policy.SeedProvider = provider
	policy.SeedMergePolicy = SeedMergePolicyDiscoveryOnly
	policy.DiscoveryExecutionMode = DiscoveryExecutionThread
	policy.DiscoveryRefreshTimeout = time.Duration(conformanceProviderTimeoutMs) * time.Millisecond

	var snapshot []*Host
	refresher, err := newSeedRefresher(policy, nil, func(hosts []*Host) {
		snapshot = copyHosts(hosts)
	})
	if err != nil {
		return nil, err
	}
	defer refresher.close()

	steps := make([]conformanceStep, 0, refreshes)
	for i := 0; i < refreshes; i++ {
		if err := selectRefresh(i); err != nil {
			return nil, err
		}
		discovered, refreshErr := refresher.refreshOnce(int64(i))
		step := conformanceStep{hosts: discovered, seeds: copyHosts(snapshot)}
		if refreshErr != nil {
			step.err = refreshErr.Error()
		}
		if i == 0 {
			seeds, startErr := refresher.resolveStartupSeeds(discovered, refreshErr)
			if startErr != nil {
				return nil, startErr
			}
			snapshot = copyHosts(seeds)
		} else if refreshErr == nil {
			if merged := refresher.merge(discovered, false); len(merged) > 0 {
				snapshot = copyHosts(merged)
			}
		}
		step.seeds = copyHosts(snapshot)
		steps = append(steps, step)
	}
	return steps, nil
}

func (runner *conformanceRunner) selectRefresh(refresh any, region string) error {
	var regionValue any
	if region != "" {
		regionValue = region
	}
	body, err := json.Marshal(map[string]any{"refresh": refresh, "region": regionValue})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/fixture/select", runner.httpPort)
	resp, err := runner.http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("fixture server unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("fixture server rejected the refresh: %s", payload)
	}
	return nil
}

func (runner *conformanceRunner) startFixtureServer(t *testing.T) error {
	script := filepath.Join(runner.dir, conformanceFixtureServer)
	if _, err := os.Stat(script); err != nil {
		return fmt.Errorf("fixture server not found: %s", script)
	}
	if err := exec.Command("python3", "--version").Run(); err != nil {
		return fmt.Errorf("python3 is required to run the dns and aws-tags fixtures: %w", err)
	}

	logFile, err := os.CreateTemp(t.TempDir(), "conformance-fixture-server-*.log")
	if err != nil {
		return err
	}

	cmd := exec.Command("python3", script)
	cmd.Stderr = logFile
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return fmt.Errorf("fixture server exited during startup, see %s: %w", logFile.Name(), err)
	}

	var ports struct {
		HTTP int `json:"http"`
		DNS  int `json:"dns"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &ports); err != nil {
		return fmt.Errorf("unexpected fixture server ports line: %s", strings.TrimSpace(line))
	}
	runner.httpPort = ports.HTTP
	runner.dnsPort = ports.DNS
	runner.http = &http.Client{Timeout: 10 * time.Second}
	return nil
}

func loadConformanceRows(dir string) ([]*conformanceRow, error) {
	var files []string
	for _, root := range []string{"discovery", "tls"} {
		err := filepath.Walk(filepath.Join(dir, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, ".yaml") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(files)

	var rows []*conformanceRow
	for _, path := range files {
		file, err := filepath.Rel(dir, path)
		if err != nil {
			return nil, err
		}
		file = filepath.ToSlash(file)
		loaded, err := loadConformanceFile(path, file)
		if err != nil {
			return nil, err
		}
		rows = append(rows, loaded...)
	}
	return rows, nil
}

func loadConformanceFile(path, file string) ([]*conformanceRow, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var top map[string]any
	if err := yaml.Unmarshal(payload, &top); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}

	kind := asString(top["kind"])
	certificates := map[string]*x509.Certificate{}
	var rows []*conformanceRow
	for _, item := range asList(top["rows"]) {
		data := asMap(item)
		var certificate *x509.Certificate
		if kind == "tls" {
			shape := asString(asMap(data["input"])["shape"])
			if certificates[shape] == nil {
				cert, err := loadConformanceCertificate(filepath.Join(filepath.Dir(path), "certs", shape+".pem"))
				if err != nil {
					return nil, err
				}
				certificates[shape] = cert
			}
			certificate = certificates[shape]
		}

		blockedBy := ""
		if asString(data["state"]) == conformanceBlocked {
			blockedBy = asString(data["blockedBy"])
		}
		declared := conformanceDeclaration{result: conformancePass}
		if blockedBy != "" {
			declared.result = conformanceBlocked
		}
		rows = append(rows, &conformanceRow{
			file:        file,
			kind:        kind,
			name:        asString(data["case"]),
			blockedBy:   blockedBy,
			data:        data,
			certificate: certificate,
			declared:    declared,
		})
	}
	return rows, nil
}

func loadConformanceCertificate(path string) (*x509.Certificate, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read certificate %s: %w", path, err)
	}
	block, _ := pem.Decode(payload)
	if block == nil {
		return nil, fmt.Errorf("cannot read certificate %s: no PEM block", path)
	}
	return x509.ParseCertificate(block.Bytes)
}

func applyConformanceDeclarations(rows []*conformanceRow) error {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("declarations not found: %s", conformanceDeclaredFile)
	}
	path := filepath.Join(filepath.Dir(thisFile), conformanceDeclaredFile)
	payload, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("declarations not found: %s", path)
	}

	var doc any
	if err := yaml.Unmarshal(payload, &doc); err != nil {
		return fmt.Errorf("%s: %w", conformanceDeclaredFile, err)
	}
	files := asMap(doc)

	byFile := map[string]map[string]*conformanceRow{}
	for _, row := range rows {
		if byFile[row.file] == nil {
			byFile[row.file] = map[string]*conformanceRow{}
		}
		byFile[row.file][row.name] = row
	}

	for fileName, fileValue := range files {
		fileRows := byFile[fileName]
		if fileRows == nil {
			return fmt.Errorf("%s: unknown fixture file %s", conformanceDeclaredFile, fileName)
		}
		for caseName, caseValue := range asMap(fileValue) {
			where := conformanceDeclaredFile + ": " + fileName + ":" + caseName
			row := fileRows[caseName]
			if row == nil {
				return fmt.Errorf("%s: unknown case", where)
			}
			decl := asMap(caseValue)
			for key := range decl {
				if key != "result" && key != "reason" {
					return fmt.Errorf("%s: unknown key %s", where, key)
				}
			}
			result := asString(decl["result"])
			if _, ok := conformanceResultNames[result]; !ok {
				return fmt.Errorf("%s: invalid result %s", where, result)
			}
			reason := asString(decl["reason"])
			if result != conformancePass && reason == "" {
				return fmt.Errorf("%s: a %s declaration needs a reason", where, result)
			}
			row.declared = conformanceDeclaration{result: result, reason: reason}
		}
	}
	return nil
}

func writeConformanceReport(fixtureSetVersion string, rows []conformanceReportRow) error {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].File+":"+rows[i].Case < rows[j].File+":"+rows[j].Case
	})

	counts := conformanceCounts{}
	for _, row := range rows {
		switch row.Result {
		case conformancePass:
			counts.Pass++
		case conformanceFail:
			counts.Fail++
		case conformanceUnsupported:
			counts.Unsupported++
		case conformanceBlocked:
			counts.Blocked++
		}
	}

	file, err := os.Create(conformanceReportPath())
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(conformanceReport{
		Runtime:           "go",
		ClientVersion:     getLibraryVersion(aesModule),
		FixtureSetVersion: fixtureSetVersion,
		Counts:            counts,
		Rows:              rows,
	})
}

func conformanceReportPath() string {
	if path := os.Getenv("CONFORMANCE_REPORT"); path != "" {
		return path
	}
	return conformanceReportFile
}

func needsFixtureServer(rows []*conformanceRow) bool {
	for _, row := range rows {
		if row.kind == "dns" || row.kind == "aws-tags" {
			return true
		}
	}
	return false
}

func hostsFromYAML(value any) []*Host {
	list := asList(value)
	hosts := make([]*Host, 0, len(list))
	for _, item := range list {
		seed := asMap(item)
		hosts = append(hosts, &Host{
			Name:    asString(seed["host"]),
			Port:    asInt(seed["port"]),
			TLSName: asString(seed["tlsName"]),
		})
	}
	return hosts
}

func seedListString(hosts []*Host) string {
	parts := make([]string, 0, len(hosts))
	for _, host := range hosts {
		parts = append(parts, host.Name+" "+strconv.Itoa(host.Port))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func hostSet(hosts []*Host) string {
	set := map[string]struct{}{}
	for _, host := range hosts {
		key := canonicalHost(host.Name) + " " + strconv.Itoa(host.Port)
		if host.TLSName != "" {
			key += " " + host.TLSName
		}
		set[key] = struct{}{}
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return "[" + strings.Join(keys, ", ") + "]"
}

func canonicalHost(host string) string {
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

func endpointString(host string, port int, tlsName string) string {
	printed := tlsName
	if printed == "" {
		printed = "null"
	}
	return fmt.Sprintf("host=%s port=%d tlsName=%s", host, port, printed)
}

func messageContains(contains any) []string {
	var facts []string
	switch values := contains.(type) {
	case map[string]any:
		for _, value := range values {
			if list, isList := value.([]any); isList {
				facts = append(facts, messageContains(list)...)
			} else {
				facts = append(facts, fmt.Sprint(value))
			}
		}
	case []any:
		for _, value := range values {
			facts = append(facts, fmt.Sprint(value))
		}
	}
	return facts
}

func stringMap(value any) map[string]string {
	raw := asMap(value)
	if raw == nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, item := range raw {
		out[key] = asString(item)
	}
	return out
}

func stringList(value any) []string {
	list := asList(value)
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, asString(item))
	}
	return out
}

func asMap(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return nil
}

func asList(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return typed
	}
	return fmt.Sprint(value)
}

func asInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case uint64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		n, _ := strconv.Atoi(typed)
		return n
	default:
		return 0
	}
}

func intOr(value any, otherwise int) int {
	if value == nil {
		return otherwise
	}
	return asInt(value)
}
