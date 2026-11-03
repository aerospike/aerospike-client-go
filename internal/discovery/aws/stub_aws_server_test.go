package aws

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const (
	stubIMDSToken        = "imds-session-token"
	stubIMDSRole         = "aerospike-seed-role"
	stubECSAuthorization = "ecs-authorization-token"
)

type stubRequest struct {
	Method  string
	Path    string
	Headers map[string]string
	Params  map[string]string
}

func (request stubRequest) accessKey() string {
	auth := request.Headers["authorization"]
	if auth == "" {
		return ""
	}
	begin := strings.Index(auth, "Credential=") + 11
	end := strings.Index(auth[begin:], "/")
	if begin < 11 || end < 0 {
		return ""
	}
	return auth[begin : begin+end]
}

type stubReply struct {
	status int
	body   string
}

type stubAWSServer struct {
	server      *httptest.Server
	lock        sync.Mutex
	requests    []stubRequest
	counts      map[string]*atomic.Int32
	ec2         []stubReply
	ec2Default  stubReply
	sts         *stubReply
	credentials string
}

func startStubAWSServer() *stubAWSServer {
	stub := &stubAWSServer{
		counts:     map[string]*atomic.Int32{},
		ec2Default: stubReply{status: 200, body: describeInstancesXML("")},
	}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	return stub
}

func (stub *stubAWSServer) url() string {
	return stub.server.URL
}

func (stub *stubAWSServer) close() {
	stub.server.Close()
}

func (stub *stubAWSServer) setEC2(replies ...stubReply) {
	stub.lock.Lock()
	stub.ec2 = append([]stubReply(nil), replies...)
	stub.lock.Unlock()
}

func (stub *stubAWSServer) setEC2Default(reply stubReply) {
	stub.lock.Lock()
	stub.ec2Default = reply
	stub.lock.Unlock()
}

func (stub *stubAWSServer) setSTS(reply stubReply) {
	stub.lock.Lock()
	copied := reply
	stub.sts = &copied
	stub.lock.Unlock()
}

func (stub *stubAWSServer) setCredentials(jsonBody string) {
	stub.lock.Lock()
	stub.credentials = jsonBody
	stub.lock.Unlock()
}

func (stub *stubAWSServer) recorded() []stubRequest {
	stub.lock.Lock()
	defer stub.lock.Unlock()
	out := make([]stubRequest, len(stub.requests))
	copy(out, stub.requests)
	return out
}

func (stub *stubAWSServer) count(path string) int {
	stub.lock.Lock()
	counter := stub.counts[path]
	stub.lock.Unlock()
	if counter == nil {
		return 0
	}
	return int(counter.Load())
}

func (stub *stubAWSServer) handle(writer http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	headers := map[string]string{}
	for name, values := range req.Header {
		if len(values) > 0 {
			headers[strings.ToLower(name)] = values[0]
		}
	}
	if req.Host != "" {
		headers["host"] = req.Host
	}

	recorded := stubRequest{
		Method:  req.Method,
		Path:    req.URL.Path,
		Headers: headers,
		Params:  formParams(string(body)),
	}

	stub.lock.Lock()
	stub.requests = append(stub.requests, recorded)
	if stub.counts[req.URL.Path] == nil {
		stub.counts[req.URL.Path] = &atomic.Int32{}
	}
	stub.counts[req.URL.Path].Add(1)
	reply := stub.replyLocked(req.Method, req.URL.Path, headers)
	stub.lock.Unlock()

	contentType := "text/plain"
	if req.URL.Path == "/" || req.URL.Path == "/sts" {
		contentType = "text/xml"
	}
	writer.Header().Set("Content-Type", contentType)
	writer.WriteHeader(reply.status)
	_, _ = writer.Write([]byte(reply.body))
}

func (stub *stubAWSServer) replyLocked(method, path string, headers map[string]string) stubReply {
	switch {
	case path == "/":
		if len(stub.ec2) > 0 {
			reply := stub.ec2[0]
			stub.ec2 = stub.ec2[1:]
			return reply
		}
		return stub.ec2Default
	case path == "/sts":
		if stub.sts == nil {
			return stubReply{status: 500}
		}
		return *stub.sts
	case strings.HasPrefix(path, "/ecs/"):
		if headers["authorization"] == stubECSAuthorization {
			return stubReply{status: 200, body: stub.credentials}
		}
		return stubReply{status: 401, body: `{"message":"unauthorized"}`}
	case path == "/latest/api/token":
		if method == http.MethodPut && headers["x-aws-ec2-metadata-token-ttl-seconds"] != "" {
			return stubReply{status: 200, body: stubIMDSToken}
		}
		return stubReply{status: 400}
	case strings.HasPrefix(path, "/latest/meta-data/iam/security-credentials/"):
		if headers["x-aws-ec2-metadata-token"] != stubIMDSToken {
			return stubReply{status: 401}
		}
		if strings.HasSuffix(path, "/") {
			return stubReply{status: 200, body: stubIMDSRole + "\n"}
		}
		if strings.HasSuffix(path, "/"+stubIMDSRole) {
			return stubReply{status: 200, body: stub.credentials}
		}
		return stubReply{status: 404}
	default:
		return stubReply{status: 404}
	}
}

func formParams(body string) map[string]string {
	params := map[string]string{}
	if body == "" {
		return params
	}
	for _, pair := range strings.Split(body, "&") {
		key, value, _ := strings.Cut(pair, "=")
		decodedKey, _ := url.QueryUnescape(key)
		decodedValue, _ := url.QueryUnescape(value)
		params[decodedKey] = decodedValue
	}
	return params
}

func describeInstancesXML(nextToken string, instances ...string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">` + "\n")
	b.WriteString("  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>\n  <reservationSet>\n")
	if len(instances) > 0 {
		b.WriteString("    <item>\n      <reservationId>r-1234567890abcdef0</reservationId>\n")
		b.WriteString("      <ownerId>123456789012</ownerId>\n      <groupSet/>\n      <instancesSet>\n")
		for _, instance := range instances {
			b.WriteString(instance)
		}
		b.WriteString("      </instancesSet>\n    </item>\n")
	}
	b.WriteString("  </reservationSet>\n")
	if nextToken != "" {
		b.WriteString("  <nextToken>" + nextToken + "</nextToken>\n")
	}
	b.WriteString("</DescribeInstancesResponse>\n")
	return b.String()
}

func instanceXML(id, privateIP, publicIP, privateDNS, publicDNS string, includeEmptyPublicDNS bool) string {
	var b strings.Builder
	b.WriteString("        <item>\n          <instanceId>" + id + "</instanceId>\n")
	b.WriteString("          <imageId>ami-0abcdef1234567890</imageId>\n")
	b.WriteString("          <instanceState>\n            <code>16</code>\n            <name>running</name>\n          </instanceState>\n")
	writeXMLField(&b, "privateDnsName", privateDNS, false)
	writeXMLField(&b, "dnsName", publicDNS, includeEmptyPublicDNS)
	b.WriteString("          <instanceType>c5.large</instanceType>\n")
	writeXMLField(&b, "privateIpAddress", privateIP, false)
	writeXMLField(&b, "ipAddress", publicIP, false)
	b.WriteString("          <networkInterfaceSet>\n            <item>\n")
	b.WriteString("              <privateIpAddress>192.0.2.254</privateIpAddress>\n")
	b.WriteString("            </item>\n          </networkInterfaceSet>\n")
	b.WriteString("          <tagSet>\n            <item>\n              <key>aerospike-cluster</key>\n")
	b.WriteString("              <value>prod-a</value>\n            </item>\n          </tagSet>\n")
	b.WriteString("        </item>\n")
	return b.String()
}

func writeXMLField(b *strings.Builder, name, value string, includeEmpty bool) {
	if value == "" && !includeEmpty {
		return
	}
	b.WriteString("          <" + name + ">" + value + "</" + name + ">\n")
}

func ec2ErrorXML(code, message, requestID string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>` + "\n<Response><Errors><Error><Code>" + code +
		"</Code><Message>" + message + "</Message></Error></Errors><RequestID>" + requestID +
		"</RequestID></Response>"
}

func assumeRoleXML(key, secret, token, expiration string) string {
	return `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <SessionToken>` + token + `</SessionToken>
      <SecretAccessKey>` + secret + `</SecretAccessKey>
      <Expiration>` + expiration + `</Expiration>
      <AccessKeyId>` + key + `</AccessKeyId>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata>
    <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>
  </ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>
`
}

func credentialsJSON(key, secret, token, expiration string) string {
	return "{\n  \"Code\" : \"Success\",\n  \"LastUpdated\" : \"2012-04-26T16:39:16Z\",\n" +
		"  \"Type\" : \"AWS-HMAC\",\n  \"AccessKeyId\" : \"" + key + "\",\n  \"SecretAccessKey\" : \"" + secret +
		"\",\n  \"Token\" : \"" + token + "\",\n  \"Expiration\" : \"" + expiration + "\"\n}"
}

func listenSilent() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

type testEnv struct {
	vars     map[string]string
	endpoint string
	home     string
	offset   time.Duration
}

func newTestEnv(endpoint string) *testEnv {
	return &testEnv{
		vars:     map[string]string{},
		endpoint: endpoint,
		home:     "/nonexistent-home",
	}
}

func (env *testEnv) set(name, value string) *testEnv {
	env.vars[name] = value
	return env
}

func (env *testEnv) Getenv(name string) string { return env.vars[name] }
func (env *testEnv) UserHome() string          { return env.home }
func (env *testEnv) EC2Endpoint(string) string { return env.endpoint }
func (env *testEnv) STSEndpoint(string) string { return env.endpoint + "/sts" }
func (env *testEnv) ECSEndpoint() string       { return env.endpoint + "/ecs" }
func (env *testEnv) IMDSEndpoint() string      { return env.endpoint }
func (env *testEnv) Now() time.Time            { return time.Now().Add(env.offset) }

type logCapture struct {
	msgs []string
}

func (capture *logCapture) Printf(format string, v ...any) {
	capture.msgs = append(capture.msgs, fmt.Sprintf(format, v...))
}

func (capture *logCapture) messages() []string {
	return append([]string(nil), capture.msgs...)
}

func captureLogs(t *testing.T, level logger.LogPriority) *logCapture {
	t.Helper()
	capture := &logCapture{}
	logger.Logger.SetLogger(capture)
	logger.Logger.SetLevel(level)
	t.Cleanup(func() {
		logger.Logger.SetLevel(logger.OFF)
		logger.Logger.SetLogger(log.New(io.Discard, "", 0))
	})
	return capture
}
