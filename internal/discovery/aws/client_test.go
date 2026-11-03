package aws

import (
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const (
	testKey    = "AKIDEXAMPLE"
	testSecret = exampleSecret
	testToken  = "FwoGZXIvYXdzEXAMPLESESSIONTOKEN"
)

func allInstancesXML() string {
	return describeInstancesXML("",
		instanceXML("i-0b", "10.0.0.2", "198.51.100.2", "ip-10-0-0-2.ec2.internal", "ec2-198-51-100-2.compute-1.amazonaws.com", false),
		instanceXML("i-0a", "10.0.0.1", "", "ip-10-0-0-1.ec2.internal", "", true),
		instanceXML("i-0c", "10.0.0.3", "198.51.100.3", "ip-10-0-0-3.ec2.internal", "ec2-198-51-100-3.compute-1.amazonaws.com", false),
		instanceXML("i-0d", "10.0.0.1", "", "ip-10-0-0-1.ec2.internal", "", false),
	)
}

func TestLookupUsesAddressFieldAndSkipsInstancesMissingIt(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	stub.setEC2Default(stubReply{status: 200, body: allInstancesXML()})

	expected := map[AddressField][]string{
		AddressFieldPrivateIP:  {"10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"},
		AddressFieldPublicIP:   {"198.51.100.2:3000", "198.51.100.3:3000"},
		AddressFieldPrivateDNS: {"ip-10-0-0-1.ec2.internal:3000", "ip-10-0-0-2.ec2.internal:3000", "ip-10-0-0-3.ec2.internal:3000"},
		AddressFieldPublicDNS:  {"ec2-198-51-100-2.compute-1.amazonaws.com:3000", "ec2-198-51-100-3.compute-1.amazonaws.com:3000"},
	}

	for field, want := range expected {
		logs := captureLogs(t, logger.DEBUG)
		provider, err := NewClient(Config{
			Region:       "us-east-1",
			Filters:      []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			AddressField: field,
			Env:          env,
			Timeout:      5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := provider.Lookup(provider.Config().Timeout)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(candidateNames(got), ",") != strings.Join(want, ",") {
			t.Fatalf("%s: got %v want %v", field.Element(), candidateNames(got), want)
		}
		skips := field == AddressFieldPublicIP || field == AddressFieldPublicDNS
		saw := false
		for _, msg := range logs.messages() {
			if msg == "AWS EC2 seed provider skipped 2 instances without "+field.Element() {
				saw = true
			}
		}
		if skips != saw {
			t.Fatalf("%s skip log=%v messages=%v", field.Element(), saw, logs.messages())
		}
	}
}

func TestLookupAppliesConfiguredPortAndTLSName(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	stub.setEC2Default(stubReply{status: 200, body: allInstancesXML()})

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Port:    4333,
		TLSName: "aerospike-tls",
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Lookup(provider.Config().Timeout)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.1:4333/aerospike-tls", "10.0.0.2:4333/aerospike-tls", "10.0.0.3:4333/aerospike-tls"}
	if strings.Join(candidateNames(got), ",") != strings.Join(want, ",") {
		t.Fatalf("got %v", candidateNames(got))
	}
}

func TestDescribeInstancesSendsFiltersAddsRunningAndSignsTheRequest(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)

	provider, err := NewClient(Config{
		Region: "us-east-1",
		Filters: []Filter{
			{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}},
			{Name: "subnet-id", Values: []string{"subnet-1", "subnet-2"}},
		},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Lookup(provider.Config().Timeout); err != nil {
		t.Fatal(err)
	}

	recorded := stub.recorded()
	if len(recorded) != 1 {
		t.Fatalf("requests=%d", len(recorded))
	}
	request := recorded[0]
	want := map[string]string{
		"Action":           "DescribeInstances",
		"Version":          "2016-11-15",
		"Filter.1.Name":    "tag:aerospike-cluster",
		"Filter.1.Value.1": "prod-a",
		"Filter.2.Name":    "subnet-id",
		"Filter.2.Value.1": "subnet-1",
		"Filter.2.Value.2": "subnet-2",
		"Filter.3.Name":    "instance-state-name",
		"Filter.3.Value.1": "running",
	}
	for key, value := range want {
		if request.Params[key] != value {
			t.Fatalf("param %s=%q want %q params=%v", key, request.Params[key], value, request.Params)
		}
	}
	if request.Method != "POST" {
		t.Fatalf("method %s", request.Method)
	}
	if request.Headers["x-amz-security-token"] != testToken {
		t.Fatalf("token header %q", request.Headers["x-amz-security-token"])
	}

	gotAuth := request.Headers["authorization"]
	matched, _ := regexp.MatchString(`^AWS4-HMAC-SHA256 Credential=`+testKey+`/\d{8}/us-east-1/ec2/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=[0-9a-f]{64}$`, gotAuth)
	if !matched {
		t.Fatalf("authorization %s", gotAuth)
	}

	date := request.Headers["x-amz-date"]
	headers := map[string]string{
		"content-type":         request.Headers["content-type"],
		"host":                 request.Headers["host"],
		"x-amz-date":           date,
		"x-amz-security-token": testToken,
	}
	var body strings.Builder
	order := []string{"Action", "Version", "Filter.1.Name", "Filter.1.Value.1", "Filter.2.Name", "Filter.2.Value.1", "Filter.2.Value.2", "Filter.3.Name", "Filter.3.Value.1"}
	for i, key := range order {
		if i > 0 {
			body.WriteByte('&')
		}
		body.WriteString(encode(key))
		body.WriteByte('=')
		body.WriteString(encode(request.Params[key]))
	}
	expected := authorization(testKey, testSecret, "POST", "/", "", headers, []byte(body.String()), "us-east-1", "ec2", date)
	if gotAuth != expected {
		t.Fatalf("signature mismatch\n got %s\nwant %s", gotAuth, expected)
	}
}

func TestLookupFollowsNextTokenAcrossPages(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	logs := captureLogs(t, logger.DEBUG)
	stub.setEC2(
		stubReply{status: 200, body: describeInstancesXML("page-2-token", instanceXML("i-1", "10.0.0.1", "", "", "", false))},
		stubReply{status: 200, body: describeInstancesXML("page-3-token+/=", instanceXML("i-2", "10.0.0.2", "", "", "", false))},
		stubReply{status: 200, body: describeInstancesXML("", instanceXML("i-3", "10.0.0.3", "", "", "", false))},
	)

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Lookup(provider.Config().Timeout)
	if err != nil {
		t.Fatal(err)
	}
	requests := stub.recorded()
	if len(requests) != 3 {
		t.Fatalf("pages=%d", len(requests))
	}
	if requests[0].Params["NextToken"] != "" || requests[1].Params["NextToken"] != "page-2-token" || requests[2].Params["NextToken"] != "page-3-token+/=" {
		t.Fatalf("tokens %q %q %q", requests[0].Params["NextToken"], requests[1].Params["NextToken"], requests[2].Params["NextToken"])
	}
	if requests[2].Params["Filter.2.Name"] != "instance-state-name" {
		t.Fatalf("running filter missing: %v", requests[2].Params)
	}
	if strings.Join(candidateNames(got), ",") != "10.0.0.1:3000,10.0.0.2:3000,10.0.0.3:3000" {
		t.Fatalf("got %v", candidateNames(got))
	}
	saw := false
	for _, msg := range logs.messages() {
		if strings.Contains(msg, "from 3 instances in 3 pages") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("debug %v", logs.messages())
	}
}

func TestLookupSurfacesEC2ErrorCodeAndRequestIDWithoutSecrets(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	stub.setEC2Default(stubReply{status: 403, body: ec2ErrorXML("UnauthorizedOperation", "You are not authorized to perform this operation.", "ea966190-f9aa-478e-9ede-example")})

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Lookup(provider.Config().Timeout)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "AWS EC2 seed provider: DescribeInstances failed: HTTP 403 UnauthorizedOperation: You are not authorized to perform this operation. (request id ea966190-f9aa-478e-9ede-example)") {
		t.Fatalf("message %s", msg)
	}
	authorization := stub.recorded()[0].Headers["authorization"]
	signature := authorization[strings.LastIndex(authorization, "=")+1:]
	assertNoSecret(t, msg, signature)
}

func TestLookupRedactsSecretsEchoedInEC2ErrorMessages(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	stub.setEC2Default(stubReply{status: 400, body: ec2ErrorXML("AuthFailure", "token "+testToken+" secret "+testSecret, "req-1")})

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Lookup(provider.Config().Timeout)
	if err == nil || !strings.Contains(err.Error(), "AuthFailure: token <redacted> secret <redacted> (request id req-1)") {
		t.Fatalf("got %v", err)
	}
	assertNoSecret(t, err.Error(), "")
}

func TestLookupRejectsDescribeInstancesXMLWithDOCTYPE(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", testKey).set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	stub.setEC2Default(stubReply{status: 200, body: `<?xml version="1.0"?><!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><DescribeInstancesResponse><reservationSet>&x;</reservationSet></DescribeInstancesResponse>`})

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Lookup(provider.Config().Timeout)
	if err == nil || !strings.Contains(err.Error(), "invalid DescribeInstances response") || !strings.Contains(err.Error(), "DOCTYPE") {
		t.Fatalf("got %v", err)
	}
}

func TestLookupHonorsTheBudgetItIsGiven(t *testing.T) {
	silent, err := listenSilent()
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()

	provider, err := NewClient(Config{
		Region:      "us-east-1",
		Filters:     []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Endpoint:    "http://" + silent.Addr().String(),
		Timeout:     80 * time.Millisecond,
		Credentials: &Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret, SessionToken: testToken},
	})
	if err != nil {
		t.Fatal(err)
	}

	elapsedInit, msg := elapsedLookup(provider, 600*time.Millisecond)
	if !strings.Contains(msg, "timed out after 600ms") {
		t.Fatalf("init message %s", msg)
	}
	if elapsedInit < 600*time.Millisecond || elapsedInit > 850*time.Millisecond {
		t.Fatalf("init elapsed %s", elapsedInit)
	}

	elapsedTend, msg := elapsedLookup(provider, provider.Config().Timeout)
	if !strings.Contains(msg, "timed out after 80ms") {
		t.Fatalf("tend message %s", msg)
	}
	if elapsedTend < 80*time.Millisecond || elapsedTend > 330*time.Millisecond {
		t.Fatalf("tend elapsed %s", elapsedTend)
	}
}

func TestLookupBoundsIMDSTokenWaitByTimeout(t *testing.T) {
	silent, err := listenSilent()
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()

	stub := startStubAWSServer()
	defer stub.close()
	imds := newTestEnv("http://" + silent.Addr().(*net.TCPAddr).String())

	provider, err := NewClient(Config{
		Region:   "us-east-1",
		Filters:  []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Endpoint: stub.url(),
		Timeout:  80 * time.Millisecond,
		Env:      imds,
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed, msg := elapsedLookup(provider, provider.Config().Timeout)
	if !strings.Contains(msg, "IMDS token timed out") {
		t.Fatalf("message %s", msg)
	}
	if elapsed < 80*time.Millisecond || elapsed > 330*time.Millisecond {
		t.Fatalf("elapsed %s", elapsed)
	}
}

func TestClientStringAndDefaultConfig(t *testing.T) {
	provider, err := NewClient(Config{
		Region:  "eu-west-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider.String() != "aws-tags eu-west-1 {tag:aerospike-cluster=[prod-a]}" {
		t.Fatalf("string %s", provider.String())
	}
	cfg := provider.Config()
	if cfg.Port != 3000 || cfg.Timeout != 80*time.Millisecond {
		t.Fatalf("defaults %+v", cfg)
	}
}

func elapsedLookup(provider *Client, budget time.Duration) (time.Duration, string) {
	begin := time.Now()
	_, err := provider.Lookup(budget)
	elapsed := time.Since(begin)
	if err == nil {
		return elapsed, ""
	}
	return elapsed, err.Error()
}

func candidateNames(candidates []Candidate) []string {
	out := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		name := candidate.Address + ":" + strconv.Itoa(candidate.Port)
		if candidate.TLSName != "" {
			name += "/" + candidate.TLSName
		}
		out = append(out, name)
	}
	return out
}

func assertNoSecret(t *testing.T, text, signature string) {
	t.Helper()
	if strings.Contains(text, testSecret) || strings.Contains(text, testToken) {
		t.Fatalf("leaked secret: %s", text)
	}
	if signature != "" && strings.Contains(text, signature) {
		t.Fatalf("leaked signature: %s", text)
	}
}
