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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

var _ = gg.Describe("AWS tags seed refresher", gg.Label("discovery"), func() {

	gg.It("reports the display name aws-tags", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:  "us-east-1",
			Filters: []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(seedProviderDisplayName(provider)).To(gm.Equal(seedProviderNameAwsTags))
		provider.Close()
	})

	gg.It("rejects tend mode because the provider cannot take a deadline", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:  "us-east-1",
			Filters: []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryExecutionMode = DiscoveryExecutionTend

		_, err = newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Seed provider aws-tags does not support a deadline and cannot run in tend execution mode. Use thread execution mode"))
		provider.Close()
	})

	gg.It("merges DescribeInstances hosts with constructor seeds on a successful thread-mode start", func() {
		server := startDescribeInstancesStub(describeOne("10.0.0.2"))
		defer server.Close()

		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:      "us-east-1",
			Filters:     []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			Endpoint:    server.URL,
			Credentials: &AwsCredentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		recorder := &publishRecorder{}
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, recorder.publish)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, err := refresher.start()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
	})

	gg.It("fails thread-mode start with SEED_DISCOVERY_FAILED when DescribeInstances times out and there are no constructor seeds", func() {
		silent, err := net.Listen("tcp", "127.0.0.1:0")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer silent.Close()

		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:      "us-east-1",
			Filters:     []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			Endpoint:    "http://" + silent.Addr().String(),
			Credentials: &AwsCredentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond
		refresher, err := newSeedRefresher(policy, nil, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(seeds).To(gm.BeNil())
		gm.Expect(startErr).To(gm.HaveOccurred())
		gm.Expect(startErr.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
		gm.Expect(startErr.Error()).To(gm.ContainSubstring("Seed provider aws-tags failed and no seed host is left"))
		// The lookup and the start wait share refresh_timeout_ms, so either may report first.
		gm.Expect(startErr.Error()).To(gm.Or(
			gm.ContainSubstring("DescribeInstances timed out after 80ms"),
			gm.ContainSubstring("the initial refresh did not finish within 80ms"),
		))
	})

	gg.It("starts from constructor seeds in thread mode when DescribeInstances times out", func() {
		silent, err := net.Listen("tcp", "127.0.0.1:0")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer silent.Close()

		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:      "us-east-1",
			Filters:     []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			Endpoint:    "http://" + silent.Addr().String(),
			Credentials: &AwsCredentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"},
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, err := refresher.start()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"127.0.0.1:1"}))
	})

	gg.It("constructs in thread mode when timeout_ms exceeds refresh_timeout_ms", func() {
		provider, err := NewAwsEc2TagSeedCandidateProvider(AwsEc2TagSeedCandidateProviderConfig{
			Region:      "us-east-1",
			Filters:     []AwsFilter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
			Timeout:     400 * time.Millisecond,
			Credentials: awsStubCredentials(),
		})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond

		_, err = newSeedRefresher(policy, []*Host{NewHost("127.0.0.1", 1)}, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		provider.Close()
	})

	gg.It("starts within refresh_timeout_ms when DescribeInstances is slower", func() {
		server := startSequencedDescribeStub(
			describeReply{delay: 300 * time.Millisecond, body: describeOne("10.0.0.2")},
		)
		defer server.Close()

		provider := newAwsStubProvider(server.URL, 80*time.Millisecond)
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 80 * time.Millisecond
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		began := time.Now()
		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(time.Since(began)).To(gm.BeNumerically("<", 250*time.Millisecond))
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000"}))
	})

	gg.It("bounds the first thread-mode refresh by refresh_timeout_ms, not timeout_ms", func() {
		server := startSequencedDescribeStub(
			describeReply{delay: 300 * time.Millisecond, body: describeOne("10.0.0.2")},
		)
		defer server.Close()

		provider := newAwsStubProvider(server.URL, 80*time.Millisecond)
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		policy.DiscoveryRefreshTimeout = 2 * time.Second
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, func([]*Host) {})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
	})

	gg.It("uses timeout_ms on later thread-mode refreshes, not refresh_timeout_ms", func() {
		server := startSequencedDescribeStub(
			describeReply{body: describeOne("10.0.0.2")},
			describeReply{delay: 300 * time.Millisecond, body: describeOne("10.0.0.3")},
		)
		defer server.Close()

		provider := newAwsStubProvider(server.URL, 80*time.Millisecond)
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = time.Hour
		recorder := &publishRecorder{}
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, recorder.publish)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))

		refresher.requestUrgentRefresh()
		gm.Consistently(func() []string { return hostAddresses(recorder.last()) }, "400ms", "20ms").ShouldNot(gm.ContainElement("10.0.0.3:3000"))
		gm.Expect(recorder.count()).To(gm.Equal(0))
	})

	gg.It("does not cap later timeout_ms to the refresh interval", func() {
		server := startSequencedDescribeStub(
			describeReply{body: describeOne("10.0.0.2")},
			describeReply{delay: 150 * time.Millisecond, body: describeOne("10.0.0.3")},
		)
		defer server.Close()

		provider := newAwsStubProvider(server.URL, 250*time.Millisecond)
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.DiscoveryRefreshInterval = 50 * time.Millisecond
		recorder := &publishRecorder{}
		refresher, err := newSeedRefresher(policy, []*Host{NewHost("10.0.0.1", 3000)}, recorder.publish)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		defer refresher.close()

		seeds, startErr := refresher.start()
		gm.Expect(startErr).ToNot(gm.HaveOccurred())
		gm.Expect(hostAddresses(seeds)).To(gm.ContainElement("10.0.0.2:3000"))

		gm.Eventually(func() []string { return hostAddresses(recorder.last()) }, "2s", "20ms").Should(gm.ContainElement("10.0.0.3:3000"))
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

func startDescribeInstancesStub(body string) *httptest.Server {
	return startSequencedDescribeStub(describeReply{body: body})
}

func startSequencedDescribeStub(replies ...describeReply) *httptest.Server {
	var lock sync.Mutex
	var calls int
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		_, _ = io.ReadAll(req.Body)
		lock.Lock()
		n := calls
		calls++
		lock.Unlock()
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
