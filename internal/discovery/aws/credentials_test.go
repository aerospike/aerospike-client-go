package aws

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const (
	roleARN  = "arn:aws:iam::123456789012:role/seed-role"
	webToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVYQU1QTEUifQ.web-identity-token"
)

func TestCredentialsPreferConfigOverEnvironment(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", "AKIAENV").set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)

	provider, err := NewClient(Config{
		Region:      "us-east-1",
		Filters:     []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:         env,
		Timeout:     5 * time.Second,
		Credentials: &Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret},
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != testKey {
		t.Fatal("expected config keys")
	}
	if stub.recorded()[len(stub.recorded())-1].Headers["x-amz-security-token"] != "" {
		t.Fatal("session token should be absent")
	}
}

func TestCredentialsFromEnvironmentIncludeSessionTokenAndDoNotLogSecrets(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	logs := captureLogs(t, logger.INFO)
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", "AKIAENV").set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIAENV" {
		t.Fatal("expected environment key")
	}
	if provider.Source() != sourceEnvironment {
		t.Fatalf("source %s", provider.Source())
	}
	if lastEC2(stub).Headers["x-amz-security-token"] != testToken {
		t.Fatal("missing session token")
	}
	info := logs.messages()
	joined := strings.Join(info, "\n")
	if !strings.Contains(joined, "AWS EC2 seed provider credentials from environment") {
		t.Fatalf("info %v", info)
	}
	if strings.Contains(joined, "AKIAENV") {
		t.Fatalf("leaked key in logs: %v", info)
	}
	assertNoSecret(t, joined, "")
}

func TestCredentialsFromNamedSharedProfile(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	file := writeProfile(t, dir, "credentials")
	env := newTestEnv(stub.url()).set("AWS_SHARED_CREDENTIALS_FILE", file).set("AWS_PROFILE", "seed")
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIASEEDPROFILE" {
		t.Fatal("expected profile key")
	}
	if provider.Source() != sourceProfile {
		t.Fatalf("source %s", provider.Source())
	}
	if lastEC2(stub).Headers["x-amz-security-token"] != testToken {
		t.Fatal("missing session token")
	}
}

func TestCredentialsFromDefaultProfileInHome(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".aws"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeProfile(t, dir, ".aws/credentials")
	env := newTestEnv(stub.url())
	env.home = dir
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIADEFAULT" {
		t.Fatal("expected default profile key")
	}
}

func TestCredentialsSkipSSOOnlyProfileAndFallThroughToIMDS(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	file := writeProfile(t, dir, "credentials")
	env := newTestEnv(stub.url()).set("AWS_SHARED_CREDENTIALS_FILE", file).set("AWS_PROFILE", "sso-only")
	stub.setCredentials(credentialsJSON("ASIAIMDS", testSecret, testToken, "2099-01-01T00:00:00Z"))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS" {
		t.Fatal("expected IMDS")
	}
	if provider.Source() != sourceIMDS {
		t.Fatalf("source %s", provider.Source())
	}
}

func TestCredentialsFromWebIdentitySTSUnsignedAssumeRole(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	env := newTestEnv(stub.url())
	enableWebIdentity(t, dir, env)
	stub.setSTS(stubReply{status: 200, body: assumeRoleXML("ASIASTS", testSecret, testToken, "2099-01-01T00:00:00Z")})

	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIASTS" {
		t.Fatal("expected STS key")
	}
	if provider.Source() != sourceWebIdentity {
		t.Fatalf("source %s", provider.Source())
	}
	sts := stub.recorded()[0]
	if sts.Path != "/sts" || sts.Params["Action"] != "AssumeRoleWithWebIdentity" || sts.Params["Version"] != "2011-06-15" {
		t.Fatalf("sts request %+v", sts)
	}
	if sts.Params["RoleArn"] != roleARN || sts.Params["RoleSessionName"] != "seed-session" || sts.Params["WebIdentityToken"] != webToken {
		t.Fatalf("sts params %+v", sts.Params)
	}
	if sts.Headers["authorization"] != "" {
		t.Fatal("STS must not be signed")
	}
	if lastEC2(stub).Headers["x-amz-security-token"] != testToken {
		t.Fatal("missing session token on EC2")
	}
}

func TestCredentialsRedactWebIdentityTokenInSTSErrors(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	env := newTestEnv(stub.url())
	enableWebIdentity(t, dir, env)
	stub.setSTS(stubReply{status: 400, body: `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>Token ` + webToken + ` is invalid</Message></Error><RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId></ErrorResponse>`})

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
	if err == nil || !strings.Contains(err.Error(), "STS AssumeRoleWithWebIdentity failed: HTTP 400 InvalidIdentityToken: Token <redacted> is invalid (request id c6104cbe-af31-11e0-8154-cbc7ccf896c7)") {
		t.Fatalf("got %v", err)
	}
	if stub.count("/") != 0 {
		t.Fatalf("ec2 called: %d", stub.count("/"))
	}
}

func TestCredentialsFromECSRelativeURIWithAuthorizationHeader(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1").set("AWS_CONTAINER_AUTHORIZATION_TOKEN", stubECSAuthorization)
	stub.setCredentials(credentialsJSON("ASIAECS", testSecret, testToken, "2099-01-01T00:00:00Z"))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAECS" {
		t.Fatal("expected ECS key")
	}
	if provider.Source() != sourceECS {
		t.Fatalf("source %s", provider.Source())
	}
	if stub.recorded()[0].Path != "/ecs/v2/credentials/task-1" {
		t.Fatalf("path %s", stub.recorded()[0].Path)
	}
}

func TestCredentialsFromECSFullURIPreferTokenFileOverEnvToken(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "ecs-token")
	if err := os.WriteFile(tokenFile, []byte(stubECSAuthorization), 0o600); err != nil {
		t.Fatal(err)
	}
	env := newTestEnv(stub.url()).
		set("AWS_CONTAINER_CREDENTIALS_FULL_URI", stub.url()+"/ecs/full").
		set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", tokenFile).
		set("AWS_CONTAINER_AUTHORIZATION_TOKEN", "wrong")
	stub.setCredentials(credentialsJSON("ASIAECSFULL", testSecret, testToken, "2099-01-01T00:00:00Z"))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAECSFULL" {
		t.Fatal("expected ECS full URI key")
	}
}

func TestCredentialsRejectECSWithoutAuthorization(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
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
	if err == nil || !strings.Contains(err.Error(), "ECS credentials failed: HTTP 401") {
		t.Fatalf("got %v", err)
	}
}

func TestCredentialsFromIMDSv2TokenThenRoleThenJSON(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url())
	stub.setCredentials(credentialsJSON("ASIAIMDS", testSecret, testToken, "2099-01-01T00:00:00Z"))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS" {
		t.Fatal("expected IMDS key")
	}
	if provider.Source() != sourceIMDS {
		t.Fatalf("source %s", provider.Source())
	}
	requests := stub.recorded()
	if requests[0].Method != "PUT" || requests[0].Path != "/latest/api/token" || requests[0].Headers["x-aws-ec2-metadata-token-ttl-seconds"] != "21600" {
		t.Fatalf("token request %+v", requests[0])
	}
	if requests[1].Path != "/latest/meta-data/iam/security-credentials/" {
		t.Fatalf("role path %s", requests[1].Path)
	}
	if requests[2].Path != "/latest/meta-data/iam/security-credentials/"+stubIMDSRole {
		t.Fatalf("creds path %s", requests[2].Path)
	}
	if requests[2].Headers["x-aws-ec2-metadata-token"] != stubIMDSToken {
		t.Fatal("missing IMDS token on credentials")
	}
}

func TestCredentialsFailWhenEverySourceIsUnavailable(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_EC2_METADATA_DISABLED", "true")
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
	if err == nil || !strings.Contains(err.Error(), "no AWS credentials found") {
		t.Fatalf("got %v", err)
	}
	if len(stub.recorded()) != 0 {
		t.Fatalf("unexpected requests %v", stub.recorded())
	}
}

func TestCredentialsPreferHookThenConfigThenEnvThenProfileThenWebIdentityThenECSThenIMDS(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	dir := t.TempDir()
	stub.setCredentials(credentialsJSON("ASIAIMDS", testSecret, testToken, "2099-01-01T00:00:00Z"))
	env := newTestEnv(stub.url()).set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1").set("AWS_CONTAINER_AUTHORIZATION_TOKEN", stubECSAuthorization)

	cfg := func() Config {
		return Config{
			Region:  "us-east-1",
			Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			Env:     env,
			Timeout: 5 * time.Second,
		}
	}

	provider, err := NewClient(cfg())
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS" {
		t.Fatal("ecs over imds uses shared JSON key")
	}

	enableWebIdentity(t, dir, env)
	stub.setSTS(stubReply{status: 200, body: assumeRoleXML("ASIASTS", testSecret, testToken, "2099-01-01T00:00:00Z")})
	provider, err = NewClient(cfg())
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIASTS" {
		t.Fatal("web identity over ecs")
	}

	env.set("AWS_SHARED_CREDENTIALS_FILE", writeProfile(t, dir, "credentials")).set("AWS_PROFILE", "seed")
	provider, err = NewClient(cfg())
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIASEEDPROFILE" {
		t.Fatal("profile over web identity")
	}

	env.set("AWS_ACCESS_KEY_ID", "AKIAENV").set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	provider, err = NewClient(cfg())
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIAENV" {
		t.Fatal("environment over profile")
	}

	config := cfg()
	config.Credentials = &Credentials{AccessKeyID: testKey, SecretAccessKey: testSecret}
	provider, err = NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != testKey {
		t.Fatal("config over environment")
	}

	hook := cfg()
	hook.CredentialsHook = func() (*Credentials, error) {
		return &Credentials{AccessKeyID: "AKIAHOOK", SecretAccessKey: testSecret}, nil
	}
	provider, err = NewClient(hook)
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "AKIAHOOK" {
		t.Fatal("hook over config")
	}
	if stub.count("/sts") != 1 {
		t.Fatalf("sts count %d", stub.count("/sts"))
	}
	if stub.count("/latest/api/token") != 0 {
		t.Fatalf("imds count %d", stub.count("/latest/api/token"))
	}
}

func TestCredentialsRefreshWithinFiveMinutesOfExpiry(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url())
	expiration := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	stub.setCredentials(credentialsJSON("ASIAIMDS1", testSecret, testToken, expiration))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS1" {
		t.Fatal("first key")
	}
	if stub.count("/latest/api/token") != 1 || stub.count("/latest/meta-data/iam/security-credentials/") != 1 {
		t.Fatalf("counts token=%d role=%d", stub.count("/latest/api/token"), stub.count("/latest/meta-data/iam/security-credentials/"))
	}

	stub.setCredentials(credentialsJSON("ASIAIMDS2", testSecret, testToken, "2099-01-01T00:00:00Z"))
	env.offset = 54 * time.Minute
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS1" {
		t.Fatal("6 minutes before expiry should reuse")
	}
	if stub.count("/latest/api/token") != 1 {
		t.Fatal("should not refresh yet")
	}

	env.offset = 56 * time.Minute
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS2" {
		t.Fatal("4 minutes before expiry should refresh")
	}
	if stub.count("/latest/api/token") != 2 {
		t.Fatalf("token count %d", stub.count("/latest/api/token"))
	}
	if stub.count("/latest/meta-data/iam/security-credentials/") != 1 {
		t.Fatal("role name should stay cached")
	}
}

func TestCredentialsKeepCachedValueWhenRefreshFailsBeforeExpiry(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url())
	logs := captureLogs(t, logger.WARNING)
	expiration := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	stub.setCredentials(credentialsJSON("ASIAIMDS1", testSecret, testToken, expiration))
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS1" {
		t.Fatal("first key")
	}

	stub.setCredentials(`{"Code":"Failure"}`)
	env.offset = 58 * time.Minute
	if refreshAccessKey(t, provider, stub) != "ASIAIMDS1" {
		t.Fatal("should keep cached")
	}
	saw := false
	for _, msg := range logs.messages() {
		if strings.HasPrefix(msg, "AWS EC2 seed provider: credentials refresh from imds failed, using cached credentials until they expire") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("warnings %v", logs.messages())
	}

	env.offset = 61 * time.Minute
	_, err = provider.Lookup(provider.Config().Timeout)
	if err == nil || !strings.Contains(err.Error(), "IMDS credentials failed: Failure") {
		t.Fatalf("got %v", err)
	}
}

func TestCredentialsHookReplacesTheChainAndRespectsExpiry(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url()).set("AWS_ACCESS_KEY_ID", "AKIAENV").set("AWS_SECRET_ACCESS_KEY", testSecret).set("AWS_SESSION_TOKEN", testToken)
	var calls atomic.Int32
	provider, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
		CredentialsHook: func() (*Credentials, error) {
			n := calls.Add(1)
			expiry := time.Now().Add(60 * time.Minute)
			if n == 1 {
				expiry = time.Now().Add(4 * time.Minute)
			}
			return &Credentials{AccessKeyID: "ASIAHOOK", SecretAccessKey: testSecret, SessionToken: testToken, Expiration: expiry}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if refreshAccessKey(t, provider, stub) != "ASIAHOOK" {
		t.Fatal("hook key")
	}
	if provider.Source() != sourceHook {
		t.Fatalf("source %s", provider.Source())
	}
	if lastEC2(stub).Headers["x-amz-security-token"] != testToken {
		t.Fatal("missing session token")
	}
	refreshAccessKey(t, provider, stub)
	refreshAccessKey(t, provider, stub)
	if calls.Load() != 2 {
		t.Fatalf("calls %d", calls.Load())
	}
}

func TestCredentialsHookNullAndPanicDoNotFallThrough(t *testing.T) {
	stub := startStubAWSServer()
	defer stub.close()
	env := newTestEnv(stub.url())
	provider, err := NewClient(Config{
		Region:          "us-east-1",
		Filters:         []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:             env,
		Timeout:         5 * time.Second,
		CredentialsHook: func() (*Credentials, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Lookup(provider.Config().Timeout)
	if err == nil || !strings.Contains(err.Error(), "credentials hook returned nil") {
		t.Fatalf("got %v", err)
	}

	throwing, err := NewClient(Config{
		Region:  "us-east-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Env:     env,
		Timeout: 5 * time.Second,
		CredentialsHook: func() (*Credentials, error) {
			panic(testSecret)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = throwing.Lookup(throwing.Config().Timeout)
	if err == nil || !strings.Contains(err.Error(), "credentials hook failed:") {
		t.Fatalf("got %v", err)
	}
	assertNoSecret(t, err.Error(), "")
}

func refreshAccessKey(t *testing.T, provider *Client, stub *stubAWSServer) string {
	t.Helper()
	if _, err := provider.Lookup(provider.Config().Timeout); err != nil {
		t.Fatal(err)
	}
	return lastEC2(stub).accessKey()
}

func lastEC2(stub *stubAWSServer) stubRequest {
	var last stubRequest
	for _, request := range stub.recorded() {
		if request.Path == "/" {
			last = request
		}
	}
	return last
}

func writeProfile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "# shared credentials\n[default]\naws_access_key_id = AKIADEFAULT\n" +
		"aws_secret_access_key = " + testSecret + "\n\n[seed]\naws_access_key_id=AKIASEEDPROFILE\n" +
		"aws_secret_access_key=" + testSecret + "\naws_session_token=" + testToken + "\n\n[sso-only]\n" +
		"sso_session = corp\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func enableWebIdentity(t *testing.T, dir string, env *testEnv) {
	t.Helper()
	token := filepath.Join(dir, "web-identity-token")
	if err := os.WriteFile(token, []byte(webToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env.set("AWS_WEB_IDENTITY_TOKEN_FILE", token).set("AWS_ROLE_ARN", roleARN).set("AWS_ROLE_SESSION_NAME", "seed-session")
}
