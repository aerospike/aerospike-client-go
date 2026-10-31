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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
)

// scriptedSeedProvider returns one scripted result per call and records the
// calls it received. A nil results entry reports scriptedError. When blockOn
// is set, every call after the first waits on it.
type scriptedSeedProvider struct {
	results  [][]*Host
	periodic bool
	deadline bool

	blockOn chan struct{}
	sleep   atomic.Int64 // nanoseconds slept by every call

	calls      atomic.Int64
	closeCalls atomic.Int64
}

var errScriptedRefresh = errors.New("scripted refresh failure")

func (provider *scriptedSeedProvider) RefreshSeedCandidates(seedContext *SeedCandidateContext) ([]*Host, error) {
	call := provider.calls.Add(1)

	if provider.blockOn != nil && call > 1 {
		<-provider.blockOn
	}
	if pause := provider.sleep.Load(); pause > 0 {
		time.Sleep(time.Duration(pause))
	}

	index := int(call) - 1
	if index >= len(provider.results) {
		index = len(provider.results) - 1
	}
	if provider.results[index] == nil {
		return nil, errScriptedRefresh
	}
	return provider.results[index], nil
}

func (provider *scriptedSeedProvider) NeedsPeriodicRefresh() bool { return provider.periodic }
func (provider *scriptedSeedProvider) SupportsDeadline() bool     { return provider.deadline }
func (provider *scriptedSeedProvider) Close()                     { provider.closeCalls.Add(1) }

// publishRecorder collects every published seed list.
type publishRecorder struct {
	lock      sync.Mutex
	published [][]*Host
}

func (recorder *publishRecorder) publish(seeds []*Host) {
	recorder.lock.Lock()
	recorder.published = append(recorder.published, seeds)
	recorder.lock.Unlock()
}

func (recorder *publishRecorder) count() int {
	recorder.lock.Lock()
	defer recorder.lock.Unlock()
	return len(recorder.published)
}

func (recorder *publishRecorder) last() []*Host {
	recorder.lock.Lock()
	defer recorder.lock.Unlock()
	if len(recorder.published) == 0 {
		return nil
	}
	return recorder.published[len(recorder.published)-1]
}

// hostAddresses extracts each host's address for order-sensitive comparison.
func hostAddresses(hosts []*Host) []string {
	addresses := make([]string, 0, len(hosts))
	for _, host := range hosts {
		addresses = append(addresses, host.String())
	}
	return addresses
}

var _ = gg.Describe("Seed refresher", func() {

	staticSeeds := []*Host{NewHost("seed-1.internal", 3000), NewHost("seed-2.internal", 3000)}

	newTendRefresherWithDeadline := func(provider SeedCandidateProvider, mergePolicy SeedMergePolicy, deadline time.Duration) (*seedRefresher, *publishRecorder) {
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.SeedMergePolicy = mergePolicy
		policy.DiscoveryExecutionMode = DiscoveryExecutionTend
		policy.DiscoveryRefreshInterval = 10 * time.Second
		policy.DiscoveryTendRefreshDeadline = deadline
		policy.TendInterval = time.Second

		recorder := &publishRecorder{}
		refresher, err := newSeedRefresher(policy, staticSeeds, recorder.publish)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return refresher, recorder
	}

	newTendRefresher := func(provider SeedCandidateProvider, mergePolicy SeedMergePolicy) (*seedRefresher, *publishRecorder) {
		return newTendRefresherWithDeadline(provider, mergePolicy, 100*time.Millisecond)
	}

	gg.Describe("merge policies", func() {

		discovered := []*Host{NewHost("found-1.internal", 3000), NewHost("seed-2.internal", 3000)}

		gg.It("merge keeps the configured seeds first and de-duplicates", func() {
			refresher, _ := newTendRefresher(&scriptedSeedProvider{results: [][]*Host{discovered}, periodic: true, deadline: true}, SeedMergePolicyMerge)

			merged, err := refresher.start()

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))
		})

		gg.It("replace keeps only discovered seeds after initialization", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{discovered, {NewHost("found-2.internal", 3000)}}, periodic: true, deadline: true}
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyReplace)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))

			refresher.tendTick(10)
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"found-2.internal:3000"}))
		})

		gg.It("discovery_only falls back to the configured seeds when discovery is empty", func() {
			refresher, _ := newTendRefresher(&scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, deadline: true}, SeedMergePolicyDiscoveryOnly)

			merged, err := refresher.start()

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000"}))
		})

		gg.It("collapses the same host and port under different TLS names to the first", func() {
			first := &Host{Name: "seed-1.internal", Port: 3000, TLSName: "name-a"}
			second := &Host{Name: "seed-1.internal", Port: 3000, TLSName: "name-b"}

			distinct := distinctHosts([]*Host{first, second})

			gm.Expect(distinct).To(gm.HaveLen(1))
			gm.Expect(distinct[0].TLSName).To(gm.Equal("name-a"))
		})
	})

	gg.Describe("ownership", func() {

		gg.It("is unaffected by the caller mutating its seeds and the provider mutating its results", func() {
			providerResult := []*Host{NewHost("found-1.internal", 3000)}
			provider := &scriptedSeedProvider{results: [][]*Host{providerResult}, periodic: true, deadline: true}

			callerSeeds := []*Host{NewHost("seed-1.internal", 3000)}
			policy := NewClientPolicy()
			policy.SeedProvider = provider
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.TendInterval = time.Second

			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(policy, callerSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			callerSeeds[0].Name = "changed.internal"

			merged, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			providerResult[0].Name = "changed-too.internal"
			merged[0].Port = 9

			gm.Expect(hostAddresses(refresher.startupSeeds)).To(gm.Equal([]string{"seed-1.internal:3000", "found-1.internal:3000"}))
		})
	})

	gg.Describe("tend execution mode", func() {

		gg.It("rejects a provider without deadline support at construction", func() {
			policy := NewClientPolicy()
			policy.SeedProvider = &scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, deadline: false}
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("does not support a deadline"))
		})

		gg.It("rejects a refresh interval below the tend interval", func() {
			policy := NewClientPolicy()
			policy.SeedProvider = &scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, deadline: true}
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.DiscoveryRefreshInterval = 500 * time.Millisecond
			policy.TendInterval = time.Second

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("must be greater or equal to the tend interval"))
		})

		gg.It("rejects a deadline above half the tend interval", func() {
			policy := NewClientPolicy()
			policy.SeedProvider = &scriptedSeedProvider{results: [][]*Host{{}}, periodic: true, deadline: true}
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.DiscoveryTendRefreshDeadline = 600 * time.Millisecond
			policy.TendInterval = time.Second

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("at most half the tend interval"))
		})

		gg.It("refreshes once per interval worth of tends", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{{NewHost("found-1.internal", 3000)}}, periodic: true, deadline: true}
			refresher, _ := newTendRefresher(provider, SeedMergePolicyMerge)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))

			// The interval is ten tends; only multiples of ten refresh.
			for tendCount := 1; tendCount <= 25; tendCount++ {
				refresher.tendTick(tendCount)
			}

			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(3)))
		})

		gg.It("keeps the current seeds when a refresh fails", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{{NewHost("found-1.internal", 3000)}, nil}, periodic: true, deadline: true}
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyMerge)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			refresher.tendTick(10)

			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(recorder.count()).To(gm.BeZero())
		})

		gg.It("discards an overrunning result, trips after three and restores the startup seeds", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{
				{NewHost("found-1.internal", 3000)},
				{NewHost("late-1.internal", 3000)},
				{NewHost("late-2.internal", 3000)},
				{NewHost("late-3.internal", 3000)},
			}, periodic: true, deadline: true}
			refresher, recorder := newTendRefresherWithDeadline(provider, SeedMergePolicyReplace, 20*time.Millisecond)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Every refresh sleeps five times past the 20ms deadline.
			provider.sleep.Store(int64(100 * time.Millisecond))

			refresher.tendTick(10)
			refresher.tendTick(20)
			gm.Expect(recorder.count()).To(gm.BeZero())
			gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())

			refresher.tendTick(30)
			gm.Expect(refresher.tripped.Get()).To(gm.BeTrue())
			gm.Expect(recorder.count()).To(gm.Equal(1))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(hostAddresses(merged)))

			// A tripped provider is never called again.
			callsAtTrip := provider.calls.Load()
			refresher.tendTick(40)
			gm.Expect(provider.calls.Load()).To(gm.Equal(callsAtTrip))
		})

		gg.It("an on-time refresh resets the overrun count", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{{NewHost("found-1.internal", 3000)}}, periodic: true, deadline: true}
			refresher, _ := newTendRefresherWithDeadline(provider, SeedMergePolicyMerge, 20*time.Millisecond)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			provider.sleep.Store(int64(100 * time.Millisecond))
			refresher.tendTick(10)
			refresher.tendTick(20)
			provider.sleep.Store(0)
			refresher.tendTick(30)
			provider.sleep.Store(int64(100 * time.Millisecond))
			refresher.tendTick(40)
			refresher.tendTick(50)

			gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())
		})

		gg.It("contains a panicking provider", func() {
			panicking := &panickingSeedProvider{}
			policy := NewClientPolicy()
			policy.SeedProvider = panicking
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.TendInterval = time.Second

			refresher, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			merged, serr := refresher.start()

			// The panic is treated as a failed refresh; the configured seeds remain.
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000"}))
		})

		gg.It("fails construction only when discovery errors and no seed is left", func() {
			policy := NewClientPolicy()
			policy.SeedProvider = &scriptedSeedProvider{results: [][]*Host{nil}, periodic: true, deadline: true}
			policy.SeedMergePolicy = SeedMergePolicyReplace
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.TendInterval = time.Second

			refresher, err := newSeedRefresher(policy, nil, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, serr := refresher.start()

			gm.Expect(serr).To(gm.HaveOccurred())
			gm.Expect(serr.Error()).To(gm.ContainSubstring("no seed host is left"))
		})
	})

	gg.Describe("thread execution mode", func() {

		newThreadRefresher := func(provider SeedCandidateProvider) (*seedRefresher, *publishRecorder) {
			policy := NewClientPolicy()
			policy.SeedProvider = provider
			policy.DiscoveryRefreshInterval = time.Hour

			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(policy, staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return refresher, recorder
		}

		gg.It("starts no goroutine for the default static provider", func() {
			policy := NewClientPolicy()
			recorder := &publishRecorder{}

			before := runtime.NumGoroutine()
			refresher, err := newSeedRefresher(policy, staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			_, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			gm.Expect(runtime.NumGoroutine()).To(gm.Equal(before))
			refresher.close()
		})

		gg.It("publishes a periodic result and wakes immediately on an urgent request", func() {
			release := make(chan struct{}, 8)
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}, {NewHost("found-2.internal", 3000)}},
				periodic: true,
				blockOn:  release,
			}
			refresher, recorder := newThreadRefresher(provider)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.ContainElement("found-1.internal:3000"))

			// The loop sleeps for an hour; the urgent request wakes it now.
			refresher.requestUrgentRefresh()
			release <- struct{}{}

			gm.Eventually(func() int { return recorder.count() }, "2s", "10ms").Should(gm.BeNumerically(">=", 1))
			gm.Expect(hostAddresses(recorder.last())).To(gm.ContainElement("found-2.internal:3000"))

			refresher.close()
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))
		})

		gg.It("close waits for an in-flight refresh and closes the provider once", func() {
			release := make(chan struct{})
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}, {NewHost("found-2.internal", 3000)}},
				periodic: true,
				blockOn:  release,
			}
			refresher, _ := newThreadRefresher(provider)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Wake the loop into a refresh that blocks on the release channel.
			refresher.requestUrgentRefresh()
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(2)))

			closed := make(chan struct{})
			go func() {
				refresher.close()
				close(closed)
			}()

			gm.Consistently(closed, "150ms", "20ms").ShouldNot(gm.BeClosed())

			close(release)
			gm.Eventually(closed, "2s", "10ms").Should(gm.BeClosed())
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))

			refresher.close()
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))
		})

		gg.It("ignores an urgent request while a refresh is in flight instead of queuing a second refresh", func() {
			release := make(chan struct{})
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}, {NewHost("found-2.internal", 3000)}},
				periodic: true,
				blockOn:  release,
			}
			refresher, _ := newThreadRefresher(provider)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Wake the sleeping loop into a refresh that blocks.
			refresher.requestUrgentRefresh()
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(2)))

			// A request during the running refresh does nothing.
			refresher.requestUrgentRefresh()
			close(release)

			gm.Consistently(func() int64 { return provider.calls.Load() }, "300ms", "20ms").Should(gm.Equal(int64(2)))
			refresher.close()
		})

		gg.It("close frees the application and the provider when a refresh hangs on unbounded network I/O", func() {
			// A server that accepts connections and never responds.
			silentServer, err := net.Listen("tcp", "127.0.0.1:0")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer silentServer.Close()
			go func() {
				for {
					if _, acceptErr := silentServer.Accept(); acceptErr != nil {
						return
					}
				}
			}()

			provider := newHangingSeedProvider("http://" + silentServer.Addr().String())

			policy := NewClientPolicy()
			policy.SeedProvider = provider
			policy.DiscoveryRefreshInterval = time.Hour
			policy.DiscoveryRefreshTimeout = 150 * time.Millisecond

			refresher, rerr := newSeedRefresher(policy, staticSeeds, func([]*Host) {})
			gm.Expect(rerr).ToNot(gm.HaveOccurred())

			// Initialization survives the hang: the bounded first-refresh wait
			// expires and the client starts from the configured seeds.
			merged, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal(hostAddresses(staticSeeds)))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))

			// The application's close returns within the bound despite the hang.
			closed := make(chan struct{})
			go func() {
				refresher.close()
				close(closed)
			}()
			gm.Eventually(closed, "2s", "10ms").Should(gm.BeClosed())
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))

			// Closing the provider is what unblocks the hung refresh.
			gm.Eventually(func() int64 { return provider.refreshReturned.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
		})

		gg.It("readers always see one complete published list", func() {
			seeds := iatomic.NewSyncVal([]*Host(nil))

			publish := func(hosts []*Host) { seeds.Set(hosts) }
			stop := make(chan struct{})

			var readerGroup sync.WaitGroup
			torn := atomic.Bool{}
			for reader := 0; reader < 4; reader++ {
				readerGroup.Add(1)
				go func() {
					defer readerGroup.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						list := seeds.Get()
						for _, host := range list {
							if len(list) > 0 && host.Name != list[0].Name {
								torn.Store(true)
							}
						}
					}
				}()
			}

			for generation := 0; generation < 5000; generation++ {
				name := fmt.Sprintf("gen-%d.internal", generation)
				listSize := generation%3 + 1
				list := make([]*Host, 0, listSize)
				for i := 0; i < listSize; i++ {
					list = append(list, NewHost(name, 3000+i))
				}
				publish(list)
			}

			close(stop)
			readerGroup.Wait()
			gm.Expect(torn.Load()).To(gm.BeFalse())
		})
	})
})

// hangingSeedProvider queries an HTTP endpoint with no timeout configured.
// Its Close cancels the in-flight request, as a well-behaved provider's
// cleanup does.
type hangingSeedProvider struct {
	endpoint string
	ctx      context.Context
	cancel   context.CancelFunc

	calls           atomic.Int64
	refreshReturned atomic.Int64
	closeCalls      atomic.Int64
}

func newHangingSeedProvider(endpoint string) *hangingSeedProvider {
	ctx, cancel := context.WithCancel(context.Background())
	return &hangingSeedProvider{endpoint: endpoint, ctx: ctx, cancel: cancel}
}

func (provider *hangingSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	provider.calls.Add(1)
	defer provider.refreshReturned.Add(1)

	request, err := http.NewRequestWithContext(provider.ctx, http.MethodGet, provider.endpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	response.Body.Close()
	return []*Host{NewHost("found-1.internal", 3000)}, nil
}

func (provider *hangingSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (provider *hangingSeedProvider) SupportsDeadline() bool     { return false }

func (provider *hangingSeedProvider) Close() {
	provider.closeCalls.Add(1)
	provider.cancel()
}

type panickingSeedProvider struct{}

func (panickingSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	panic("provider failure")
}
func (panickingSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (panickingSeedProvider) SupportsDeadline() bool     { return true }
func (panickingSeedProvider) Close()                     {}
