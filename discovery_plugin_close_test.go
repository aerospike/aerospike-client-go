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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Discovery plugin close", gg.Label("discovery"), func() {

	deadSeed := NewHost("127.0.0.1", 1)

	// buildClient reports a construction panic as an error.
	buildClient := func(policy *ClientPolicy, seed *Host) (client *Client, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("%v", recovered)
			}
		}()
		client, clientErr := NewClientWithPolicyAndHost(policy, seed)
		if clientErr != nil {
			return client, clientErr
		}
		return client, nil
	}

	gg.It("closes the translator once, after a translation in the regular tend returns, when the client closes", func() {
		server, err := newFakeInfoServer()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gg.DeferCleanup(server.close)
		server.setReply("peers-clear-std", "1,3000,[[BB9000000000001,,[127.0.0.1:1]]]")

		translator := &closeTrackingTranslator{release: make(chan struct{})}
		gg.DeferCleanup(func() {
			translator.held.Store(false)
			select {
			case <-translator.release:
			default:
				close(translator.release)
			}
		})
		policy := discoveryPluginPolicy(nil, translator)
		policy.TendInterval = 50 * time.Millisecond
		policy.Timeout = time.Second
		client, buildErr := buildClient(policy, server.host())
		gm.Expect(buildErr).ToNot(gm.HaveOccurred())
		gm.Expect(client.IsConnected()).To(gm.BeTrue())

		// A new peers generation makes the next regular tend translate again.
		translator.held.Store(true)
		server.setReply("peers-generation", "2")
		gm.Eventually(translator.inFlight.Load, "2s", "10ms").Should(gm.BeTrue(), "the regular tend never reached Translate")

		closed := make(chan struct{})
		go func() {
			client.Close()
			close(closed)
		}()
		gm.Consistently(closed, "300ms", "20ms").ShouldNot(gm.BeClosed(), "Close returned while the regular tend was translating")
		gm.Expect(translator.closeCalls.Load()).To(gm.BeZero(), "the translator was closed while the regular tend was translating")

		close(translator.release)

		gm.Eventually(closed, "2s").Should(gm.BeClosed())
		gm.Expect(translator.closeCalls.Load()).To(gm.Equal(int64(1)), "Close returned before closing the translator")
		client.Close()
		gm.Consistently(translator.closeCalls.Load, "300ms", "20ms").Should(gm.Equal(int64(1)))
		gm.Expect(translator.closedDuringTranslate.Load()).To(gm.BeFalse())
		gm.Expect(translator.translatedAfterClose.Load()).To(gm.BeFalse())
	})

	gg.DescribeTable("closes the provider and the translator once when the client fails to build",
		func(failBuild func(policy *ClientPolicy), closedWhenBuildReturns bool) {
			provider := &scriptedSeedProvider{results: [][]*Host{{deadSeed}}, periodic: true}
			translator := &closeTrackingTranslator{}
			policy := discoveryPluginPolicy(provider, translator)
			policy.FailIfNotConnected = true
			failBuild(policy)

			_, err := buildClient(policy, deadSeed)

			gm.Expect(err).To(gm.HaveOccurred())
			if closedWhenBuildReturns {
				gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)), "the provider was not closed by the time the build returned")
				gm.Expect(translator.closeCalls.Load()).To(gm.Equal(int64(1)), "the translator was not closed by the time the build returned")
			}
			gm.Eventually(provider.closeCalls.Load, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Eventually(translator.closeCalls.Load, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Consistently(func() int64 { return provider.closeCalls.Load() + translator.closeCalls.Load() },
				"300ms", "20ms").Should(gm.Equal(int64(2)))
		},
		gg.Entry("when the discovery settings are rejected", func(policy *ClientPolicy) {
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
		}, true),
		gg.Entry("when external authentication has no TLS", func(policy *ClientPolicy) {
			policy.User = "user"
			policy.Password = "password"
			policy.AuthMode = AuthModeExternal
		}, true),
		gg.Entry("when a PKI user also sets a password", func(policy *ClientPolicy) {
			policy.Password = "password"
			policy.AuthMode = AuthModePKI
		}, true),
		gg.Entry("when the minimum connections exceed the pool size", func(policy *ClientPolicy) {
			policy.MinConnectionsPerNode = policy.ConnectionQueueSize + 1
		}, true),
		// The construction tend may still be finishing when the build returns.
		gg.Entry("when no seed is reachable", func(*ClientPolicy) {}, false),
	)

	// The construction tend runs in its own goroutine, which outlives a build
	// that times out. These specs hold that tend inside Translate past the
	// build timeout, so a close requested meanwhile must wait for it.
	gg.Context("while the construction tend is still translating past the build timeout", func() {

		var (
			server           *fakeInfoServer
			translator       *closeTrackingTranslator
			releaseTranslate func()
		)

		gg.BeforeEach(func() {
			var err error
			server, err = newFakeInfoServer()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gg.DeferCleanup(server.close)
			server.setReply("peers-clear-std", "1,3000,[[BB9000000000001,,[10.0.0.9:3000]]]")

			translator = &closeTrackingTranslator{release: make(chan struct{})}
			translator.held.Store(true)
			var releaseOnce sync.Once
			releaseTranslate = func() { releaseOnce.Do(func() { close(translator.release) }) }
			gg.DeferCleanup(releaseTranslate)
		})

		// slowTranslationPolicy keeps the regular tend loop idle, so the
		// construction tend is the only caller of Translate.
		slowTranslationPolicy := func(failIfNotConnected bool) *ClientPolicy {
			policy := discoveryPluginPolicy(nil, translator)
			policy.FailIfNotConnected = failIfNotConnected
			policy.TendInterval = time.Hour
			return policy
		}

		// buildInBackground returns a channel that receives the build result,
		// so a build that waits for the translation fails the spec, not hangs it.
		type buildResult struct {
			client *Client
			err    error
		}
		buildInBackground := func(policy *ClientPolicy) <-chan buildResult {
			results := make(chan buildResult, 1)
			go func() {
				client, err := buildClient(policy, server.host())
				results <- buildResult{client: client, err: err}
			}()
			return results
		}

		expectClosedOnceOnlyAfterTranslationReturns := func() {
			gm.Eventually(translator.inFlight.Load, "2s", "10ms").Should(gm.BeTrue(), "the construction tend never reached Translate")
			gm.Consistently(translator.closeCalls.Load, "300ms", "20ms").Should(gm.BeZero(),
				"the translator was closed while the construction tend was still translating")

			releaseTranslate()

			gm.Eventually(translator.closeCalls.Load, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Consistently(translator.closeCalls.Load, "300ms", "20ms").Should(gm.Equal(int64(1)))
			gm.Expect(translator.closedDuringTranslate.Load()).To(gm.BeFalse())
			gm.Expect(translator.translatedAfterClose.Load()).To(gm.BeFalse())
		}

		gg.It("fails the build at its timeout and closes the translator once the translation returns", func() {
			builds := buildInBackground(slowTranslationPolicy(true))

			var build buildResult
			gm.Eventually(builds, "2s").Should(gm.Receive(&build), "the failed build waited for the translation")
			gm.Expect(build.err).To(gm.HaveOccurred())

			expectClosedOnceOnlyAfterTranslationReturns()
		})

		gg.It("returns the client at its timeout and closes the translator once the translation returns after the client closes", func() {
			builds := buildInBackground(slowTranslationPolicy(false))

			var build buildResult
			gm.Eventually(builds, "2s").Should(gm.Receive(&build), "the build waited for the translation")
			gm.Expect(build.err).To(gm.HaveOccurred())
			gm.Expect(build.client).ToNot(gm.BeNil())

			closed := make(chan struct{})
			go func() {
				build.client.Close()
				build.client.Close()
				close(closed)
			}()
			gm.Eventually(closed, "2s").Should(gm.BeClosed(), "Close waited for the translation")
			gm.Expect(build.client.IsConnected()).To(gm.BeFalse())

			expectClosedOnceOnlyAfterTranslationReturns()
		})
	})

	gg.DescribeTable("logs a failing Close and still closes the client",
		func(policy *ClientPolicy, closeCalls *atomic.Int64, warning string) {
			logs := captureWarnLogs()
			client, _ := buildClient(policy, deadSeed)
			gm.Expect(client).ToNot(gm.BeNil())

			gm.Expect(client.Close).ToNot(gm.Panic())

			gm.Eventually(closeCalls.Load, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Eventually(logs.snapshot, "2s", "10ms").Should(gm.ContainElement(warning))
			gm.Expect(client.IsConnected()).To(gm.BeFalse())
		},
		closeFailureEntry("when a provider Close returns an error", func() (SeedCandidateProvider, AddressTranslator, *atomic.Int64) {
			provider := &scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, closeErr: errors.New("lease already released")}
			return provider, nil, &provider.closeCalls
		}, "Seed candidate provider *aerospike.scriptedSeedProvider close failed: lease already released"),
		closeFailureEntry("when a provider Close panics", func() (SeedCandidateProvider, AddressTranslator, *atomic.Int64) {
			provider := &scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, closePanics: true}
			return provider, nil, &provider.closeCalls
		}, "Seed candidate provider *aerospike.scriptedSeedProvider close failed: provider close failure"),
		closeFailureEntry("when a translator Close returns an error", func() (SeedCandidateProvider, AddressTranslator, *atomic.Int64) {
			translator := &closeTrackingTranslator{closeErr: errors.New("socket already gone")}
			return nil, translator, &translator.closeCalls
		}, "Address translator *aerospike.closeTrackingTranslator close failed: socket already gone"),
		closeFailureEntry("when a translator Close panics", func() (SeedCandidateProvider, AddressTranslator, *atomic.Int64) {
			translator := &closeTrackingTranslator{closePanics: true}
			return nil, translator, &translator.closeCalls
		}, "Address translator *aerospike.closeTrackingTranslator close failed: translator close failure"),
	)

	gg.It("never calls a provider or translator Close that has no error result", func() {
		provider := &closeWithoutErrorSeedProvider{}
		translator := &closeWithoutErrorTranslator{}
		client, _ := buildClient(discoveryPluginPolicy(provider, translator), deadSeed)
		gm.Expect(client).ToNot(gm.BeNil())

		client.Close()

		gm.Consistently(func() int64 { return provider.closeCalls.Load() + translator.closeCalls.Load() },
			"300ms", "20ms").Should(gm.BeZero())
	})
})

// discoveryPluginPolicy is a client policy with the given plugins and short
// timeouts, for a client whose only seed is unreachable.
func discoveryPluginPolicy(provider SeedCandidateProvider, translator AddressTranslator) *ClientPolicy {
	policy := NewClientPolicy()
	policy.SeedProvider = provider
	policy.AddressTranslator = translator
	policy.FailIfNotConnected = false
	policy.Timeout = 100 * time.Millisecond
	policy.TendInterval = 250 * time.Millisecond
	return policy
}

// closeFailureEntry builds one entry of the failing-Close table from the
// plugins that build returns and the close counter of the failing one.
func closeFailureEntry(description string, build func() (SeedCandidateProvider, AddressTranslator, *atomic.Int64), warning string) gg.TableEntry {
	provider, translator, closeCalls := build()
	return gg.Entry(description, discoveryPluginPolicy(provider, translator), closeCalls, warning)
}

// closeTrackingTranslator passes every address through, counts Close calls
// and records a Close that ran while Translate was still running, and a
// Translate that started after Close. While held is set, Translate blocks
// until release closes. Close fails with closeErr, or with a panic when
// closePanics is set.
type closeTrackingTranslator struct {
	release     chan struct{}
	closeErr    error
	closePanics bool

	held                  atomic.Bool
	inFlight              atomic.Bool
	closedDuringTranslate atomic.Bool
	translatedAfterClose  atomic.Bool
	closeCalls            atomic.Int64
}

func (translator *closeTrackingTranslator) Translate(advertised Endpoint) (Host, bool) {
	if translator.closeCalls.Load() > 0 {
		translator.translatedAfterClose.Store(true)
	}
	translator.inFlight.Store(true)
	defer translator.inFlight.Store(false)
	if translator.held.Load() {
		<-translator.release
	}
	return Host{Name: advertised.Host, Port: advertised.Port}, true
}

func (translator *closeTrackingTranslator) Close() error {
	if translator.inFlight.Load() {
		translator.closedDuringTranslate.Store(true)
	}
	translator.closeCalls.Add(1)
	if translator.closePanics {
		panic("translator close failure")
	}
	return translator.closeErr
}

// noHostsSeedProvider returns an empty list on every refresh and has no Close.
type noHostsSeedProvider struct{}

func (noHostsSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	return nil, nil
}
func (noHostsSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (noHostsSeedProvider) SupportsDeadline() bool     { return false }

// closeWithoutErrorSeedProvider has a Close that does not satisfy io.Closer.
type closeWithoutErrorSeedProvider struct {
	noHostsSeedProvider
	closeCalls atomic.Int64
}

func (provider *closeWithoutErrorSeedProvider) Close() { provider.closeCalls.Add(1) }

// closeWithoutErrorTranslator has a Close that does not satisfy io.Closer.
type closeWithoutErrorTranslator struct {
	passThroughAddressTranslator
	closeCalls atomic.Int64
}

func (translator *closeWithoutErrorTranslator) Close() { translator.closeCalls.Add(1) }

func (capture *warnLogCapture) snapshot() []string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return append([]string(nil), capture.msgs...)
}
