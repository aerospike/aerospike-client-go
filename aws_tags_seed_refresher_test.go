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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// Clock gap used by the timeout specs: plugin timeout_ms < delayed body <
// refresh_timeout_ms. A leftover-seed result with elapsed in
// [awsPluginTimeout/2, awsSlowReply) means I/O used timeout_ms. Getting
// 10.0.0.2/10.0.0.3 means I/O waited for the body (refresh_timeout_ms).
const (
	awsPluginTimeout = 80 * time.Millisecond
	awsFrameworkWait = 2 * time.Second
	awsSlowReply     = 300 * time.Millisecond
)

// AWS tags seed refresher tests cover the Java AwsTagsSeedRefresherTest
// I/O contract through newSeedRefresher and a sequenced DescribeInstances
// stub. Timeout-versus-deadline construction is in seed_refresher_test.go.
var _ = gg.Describe("AWS tags seed refresher", gg.Label("discovery"), func() {

	gg.It("reports the display name aws-tags", func() {
		provider := newAwsStubProvider("", 0)
		gm.Expect(seedProviderDisplayName(provider)).To(gm.Equal(seedProviderNameAwsTags))
	})

	gg.It("rejects tend mode because the provider cannot take a deadline", func() {
		server := startDescribeInstancesStub(describeOne("10.0.0.2"))
		defer server.Close()

		provider := newAwsStubProvider(server.URL, 0)
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryExecutionMode = DiscoveryExecutionTend

		_, err := newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Seed candidate provider aws-tags does not support a deadline and cannot run in tend execution mode. Use thread execution mode"))
		gm.Expect(server.callCount()).To(gm.Equal(0))
	})

	gg.Context("thread-mode start", func() {
		gg.It("merges DescribeInstances hosts with constructor seeds", func() {
			server := startDescribeInstancesStub(describeOne("10.0.0.2"))
			defer server.Close()

			refresher, recorder := mustThreadRefresher(newAwsStubProvider(server.URL, 0), []*Host{NewHost("10.0.0.1", 3000)}, 0)
			defer refresher.close()

			seeds, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(server.callCount()).To(gm.Equal(1))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("fails with SEED_DISCOVERY_FAILED when DescribeInstances times out and there are no constructor seeds", func() {
			silent := mustListenLocal()
			defer silent.Close()

			refresher, recorder := mustThreadRefresher(newAwsStubProvider("http://"+silent.Addr().String(), awsPluginTimeout), nil, awsFrameworkWait)
			defer refresher.close()

			began := time.Now()
			seeds, startErr := refresher.start()
			expectElapsedIn(time.Since(began), awsPluginTimeout/2, awsSlowReply)
			gm.Expect(seeds).To(gm.BeNil())
			gm.Expect(startErr).ToNot(gm.BeNil())
			gm.Expect(startErr.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
			gm.Expect(startErr.Error()).To(gm.ContainSubstring("Seed candidate provider aws-tags failed and no seed host is left: AWS EC2 seed provider: DescribeInstances timed out after 80ms"))
			gm.Expect(startErr.Error()).ToNot(gm.ContainSubstring("the initial refresh did not finish within"))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("starts from constructor seeds when DescribeInstances times out", func() {
			silent := mustListenLocal()
			defer silent.Close()

			refresher, recorder := mustThreadRefresher(newAwsStubProvider("http://"+silent.Addr().String(), awsPluginTimeout), []*Host{NewHost("127.0.0.1", 1)}, awsFrameworkWait)
			defer refresher.close()

			began := time.Now()
			seeds, err := refresher.start()
			expectElapsedIn(time.Since(began), awsPluginTimeout/2, awsSlowReply)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})
	})

	gg.Context("thread-mode I/O uses timeout_ms", func() {
		gg.It("times out the first refresh at timeout_ms, not refresh_timeout_ms", func() {
			server := startSequencedDescribeStub(
				describeReply{delay: awsSlowReply, body: describeOne("10.0.0.2")},
			)
			defer server.Close()

			refresher, recorder := mustThreadRefresher(newAwsStubProvider(server.URL, awsPluginTimeout), []*Host{NewHost("10.0.0.1", 3000)}, awsFrameworkWait)
			defer refresher.close()

			began := time.Now()
			seeds, startErr := refresher.start()
			expectElapsedIn(time.Since(began), awsPluginTimeout/2, awsSlowReply)
			gm.Expect(startErr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000"}))
			gm.Expect(server.callCount()).To(gm.Equal(1))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("times out later refreshes at timeout_ms, not refresh_timeout_ms", func() {
			server := startSequencedDescribeStub(
				describeReply{body: describeOne("10.0.0.2")},
				describeReply{delay: awsSlowReply, body: describeOne("10.0.0.3")},
			)
			defer server.Close()

			refresher, recorder := mustThreadRefresher(newAwsStubProvider(server.URL, awsPluginTimeout), []*Host{NewHost("10.0.0.1", 3000)}, awsFrameworkWait)
			defer refresher.close()

			seeds, startErr := refresher.start()
			gm.Expect(startErr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(server.callCount()).To(gm.Equal(1))
			gm.Expect(recorder.count()).To(gm.Equal(0))

			refresher.requestUrgentRefresh()
			gm.Eventually(func() int { return server.callCount() }, "1s", "10ms").Should(gm.Equal(2))
			gm.Consistently(func() []string { return hostAddresses(recorder.last()) }, "400ms", "20ms").ShouldNot(gm.ContainElement("10.0.0.3:3000"))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("does not cap later timeout_ms to the refresh interval", func() {
			const (
				interval           = 50 * time.Millisecond
				pluginTimeout      = 250 * time.Millisecond
				slowerThanInterval = 150 * time.Millisecond
			)
			server := startSequencedDescribeStub(
				describeReply{body: describeOne("10.0.0.2")},
				describeReply{delay: slowerThanInterval, body: describeOne("10.0.0.3")},
			)
			defer server.Close()

			policy := threadClientPolicy(newAwsStubProvider(server.URL, pluginTimeout), 0)
			policy.DiscoveryRefreshInterval = interval
			policy.TendInterval = 10 * time.Millisecond
			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			refresher.activate()
			defer refresher.close()

			seeds, startErr := refresher.start()
			gm.Expect(startErr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(server.callCount()).To(gm.Equal(1))
			gm.Expect(recorder.count()).To(gm.Equal(0))

			gm.Eventually(func() int { return server.callCount() }, "1s", "20ms").Should(gm.BeNumerically(">=", 2))
			gm.Eventually(func() []string { return hostAddresses(recorder.last()) }, "2s", "20ms").Should(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.3:3000"}))
		})
	})
})

func awsStubCredentials() *AwsCredentials {
	return &AwsCredentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
}

func newAwsStubProvider(endpoint string, timeout time.Duration) SeedCandidateProvider {
	provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
		Region:      "us-east-1",
		Filters:     []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		Endpoint:    endpoint,
		Timeout:     timeout,
		Credentials: awsStubCredentials(),
	})
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return provider
}

type describeReply struct {
	delay time.Duration
	body  string
}

type describeStub struct {
	*httptest.Server
	mu    sync.Mutex
	calls int
}

func (stub *describeStub) callCount() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.calls
}

func startDescribeInstancesStub(body string) *describeStub {
	return startSequencedDescribeStub(describeReply{body: body})
}

func startSequencedDescribeStub(replies ...describeReply) *describeStub {
	stub := &describeStub{}
	stub.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		_, _ = io.ReadAll(req.Body)
		stub.mu.Lock()
		n := stub.calls
		stub.calls++
		stub.mu.Unlock()
		if n >= len(replies) {
			n = len(replies) - 1
		}
		reply := replies[n]
		if reply.delay > 0 {
			time.Sleep(reply.delay)
		}
		writer.Header().Set("Content-Type", "text/xml")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(reply.body))
	}))
	return stub
}

func describeOne(privateIP string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">`)
	b.WriteString(`<reservationSet><item><instancesSet><item>`)
	b.WriteString(`<privateIpAddress>` + privateIP + `</privateIpAddress>`)
	b.WriteString(`<networkInterfaceSet><item><privateIpAddress>192.0.2.254</privateIpAddress></item></networkInterfaceSet>`)
	b.WriteString(`</item></instancesSet></item></reservationSet>`)
	b.WriteString(`</DescribeInstancesResponse>`)
	return b.String()
}
