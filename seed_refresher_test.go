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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v8/types"
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

func periodicDeadlineProvider(results ...[]*Host) *scriptedSeedProvider {
	return &scriptedSeedProvider{results: results, periodic: true, deadline: true}
}

// gatedSeedProvider holds the first RefreshSeedCandidates until releaseRefresh.
// waitUntilStarted returns after that call has begun, so a test can prove
// start returned while the lookup was still in flight.
type gatedSeedProvider struct {
	begin   chan struct{}
	release chan struct{}
	hosts   []*Host

	beginOnce sync.Once
	calls     atomic.Int64
}

func newGatedSeedProvider(hosts []*Host) *gatedSeedProvider {
	return &gatedSeedProvider{
		begin:   make(chan struct{}),
		release: make(chan struct{}),
		hosts:   hosts,
	}
}

func (provider *gatedSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	provider.calls.Add(1)
	provider.beginOnce.Do(func() { close(provider.begin) })
	<-provider.release
	return copyHosts(provider.hosts), nil
}

func (*gatedSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (*gatedSeedProvider) SupportsDeadline() bool     { return false }
func (*gatedSeedProvider) Close()                     {}

func (provider *gatedSeedProvider) waitUntilStarted(within time.Duration) bool {
	select {
	case <-provider.begin:
		return true
	case <-time.After(within):
		return false
	}
}

func (provider *gatedSeedProvider) releaseRefresh() {
	close(provider.release)
}

type firstStartResult struct {
	seeds []*Host
	err   Error
}

func beginStart(refresher *seedRefresher) <-chan firstStartResult {
	done := make(chan firstStartResult, 1)
	go func() {
		seeds, err := refresher.start()
		done <- firstStartResult{seeds: seeds, err: err}
	}()
	return done
}

func waitStart(done <-chan firstStartResult, within time.Duration) (firstStartResult, error) {
	select {
	case result := <-done:
		if result.err != nil {
			return result, result.err
		}
		return result, nil
	case <-time.After(within):
		return firstStartResult{}, fmt.Errorf("start did not return: constructor and refresh loop are deadlocked")
	}
}

func waitUntil(within time.Duration, ready func() bool) bool {
	deadline := time.Now().Add(within)
	for {
		if ready() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func hostListHas(hosts []*Host, name string) bool {
	for _, host := range hosts {
		if host != nil && host.Name == name {
			return true
		}
	}
	return false
}

func threadClientPolicy(provider SeedCandidateProvider, timeout time.Duration) *ClientPolicy {
	policy := NewClientPolicy()
	policy.SeedProvider = provider
	policy.DiscoveryRefreshInterval = time.Hour
	if timeout > 0 {
		policy.DiscoveryRefreshTimeout = timeout
	}
	return policy
}

func newThreadRefresherFor(provider SeedCandidateProvider, seeds []*Host, timeout time.Duration) (*seedRefresher, *publishRecorder, error) {
	recorder := &publishRecorder{}
	refresher, err := newSeedRefresher(threadClientPolicy(provider, timeout), seeds, recorder.publish)
	if err == nil {
		refresher.activate()
		refresher.markTending()
	}
	return refresher, recorder, err
}

// triggerRefresh wakes the loop for its next refresh now, bypassing the
// urgent rate limit. For specs that drive refreshes, not test the limit.
func triggerRefresh(refresher *seedRefresher) {
	refresher.lastUrgent.Store(0)
	refresher.requestUrgentRefresh()
}

func mustThreadRefresher(provider SeedCandidateProvider, seeds []*Host, timeout time.Duration) (*seedRefresher, *publishRecorder) {
	refresher, recorder, err := newThreadRefresherFor(provider, seeds, timeout)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return refresher, recorder
}

// expectElapsedIn checks that a timed-out lookup waited for the plugin
// timeout (not an instant empty result) and returned before max (the delayed
// body, or refresh_timeout_ms).
func expectElapsedIn(elapsed, min, max time.Duration) {
	gm.Expect(elapsed).To(gm.And(
		gm.BeNumerically(">=", min),
		gm.BeNumerically("<", max),
	))
}

func dnsTimeoutProvider(timeout time.Duration) SeedCandidateProvider {
	provider, err := NewDnsSeedCandidateProvider(DnsSeedCandidateProviderConfig{
		Names:   []string{"seeds.example.com"},
		Servers: []string{"127.0.0.1"},
		Timeout: timeout,
	})
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return provider
}

func mustListenLocal() net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return listener
}

func assertLoopMovedOn(provider *gatedSeedProvider, refresher *seedRefresher) error {
	refresher.requestUrgentRefresh()
	if !waitUntil(2*time.Second, func() bool { return provider.calls.Load() >= 2 }) {
		return fmt.Errorf("refresh loop did not run again; it is stuck on the first handoff")
	}
	began := time.Now()
	refresher.close()
	if time.Since(began) >= 400*time.Millisecond {
		return fmt.Errorf("close blocked as if the refresh loop were stuck on the first handoff")
	}
	return nil
}

func exerciseOnTimeHandoff(staticSeeds []*Host) error {
	discovered := NewHost("found-1.internal", 3000)
	provider := newGatedSeedProvider([]*Host{discovered})
	provider.releaseRefresh()
	refresher, recorder, err := newThreadRefresherFor(provider, staticSeeds, 2*time.Second)
	if err != nil {
		return err
	}
	defer refresher.close()

	started, err := waitStart(beginStart(refresher), 2*time.Second)
	if err != nil {
		return err
	}
	if !hostListHas(started.seeds, discovered.Name) {
		return fmt.Errorf("on-time start missed %s in %v", discovered.Name, hostAddresses(started.seeds))
	}
	if recorder.count() != 0 {
		return fmt.Errorf("on-time start published %d lists; the first result belongs on start's return", recorder.count())
	}
	return assertLoopMovedOn(provider, refresher)
}

func exerciseLateHandoff(staticSeeds []*Host) error {
	discovered := NewHost("found-late.internal", 3000)
	provider := newGatedSeedProvider([]*Host{discovered})
	refresher, recorder, err := newThreadRefresherFor(provider, staticSeeds, 50*time.Millisecond)
	if err != nil {
		return err
	}
	defer refresher.close()

	done := beginStart(refresher)
	if !provider.waitUntilStarted(2 * time.Second) {
		return fmt.Errorf("refresh loop never started the first lookup")
	}
	started, err := waitStart(done, 2*time.Second)
	if err != nil {
		return err
	}
	if got, want := hostAddresses(started.seeds), hostAddresses(staticSeeds); !slices.Equal(got, want) {
		return fmt.Errorf("abandoned start got %v, want constructor seeds %v", got, want)
	}
	if recorder.count() != 0 {
		return fmt.Errorf("abandoned start published %d lists before the lookup finished", recorder.count())
	}

	provider.releaseRefresh()
	if !waitUntil(2*time.Second, func() bool { return hostListHas(recorder.last(), discovered.Name) }) {
		return fmt.Errorf("late success was not published")
	}
	return assertLoopMovedOn(provider, refresher)
}

func exerciseFirstHandoff(staticSeeds []*Host, late bool) error {
	if late {
		return exerciseLateHandoff(staticSeeds)
	}
	return exerciseOnTimeHandoff(staticSeeds)
}

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

var _ = gg.Describe("Seed refresher", gg.Label("discovery"), func() {

	staticSeeds := []*Host{NewHost("seed-1.internal", 3000), NewHost("seed-2.internal", 3000)}

	newTendRefresherOn := func(provider SeedCandidateProvider, mergePolicy SeedMergePolicy, seeds []*Host, deadline time.Duration) (*seedRefresher, *publishRecorder) {
		policy := NewClientPolicy()
		policy.SeedProvider = provider
		policy.SeedMergePolicy = mergePolicy
		policy.DiscoveryExecutionMode = DiscoveryExecutionTend
		policy.DiscoveryRefreshInterval = 10 * time.Second
		policy.DiscoveryTendRefreshDeadline = deadline
		policy.TendInterval = time.Second

		recorder := &publishRecorder{}
		refresher, err := newSeedRefresher(policy, seeds, recorder.publish)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return refresher, recorder
	}

	newTendRefresherWithDeadline := func(provider SeedCandidateProvider, mergePolicy SeedMergePolicy, deadline time.Duration) (*seedRefresher, *publishRecorder) {
		return newTendRefresherOn(provider, mergePolicy, staticSeeds, deadline)
	}

	newTendRefresher := func(provider SeedCandidateProvider, mergePolicy SeedMergePolicy) (*seedRefresher, *publishRecorder) {
		return newTendRefresherWithDeadline(provider, mergePolicy, 100*time.Millisecond)
	}

	gg.Describe("merge policies", func() {

		staticAddrs := []string{"seed-1.internal:3000", "seed-2.internal:3000"}
		d1 := NewHost("found-1.internal", 3000)
		d2 := NewHost("found-2.internal", 3000)
		discovered := []*Host{d1, NewHost("seed-2.internal", 3000)}

		gg.It("orders init and later results per policy, including empty discovery", func() {
			s1 := NewHost("10.0.0.1", 3000)
			s2 := NewHost("10.0.0.2", 3000)
			found1 := NewHost("10.0.1.1", 3000)
			found2 := NewHost("10.0.1.2", 3000)
			s2tls := &Host{Name: "10.0.0.2", Port: 3000, TLSName: "tls"}
			static := []*Host{s1, s2}
			hosts := []*Host{found1, s2tls, found2, found1}

			mergeRef, _ := newTendRefresherOn(periodicDeadlineProvider([]*Host{}), SeedMergePolicyMerge, static, 100*time.Millisecond)
			gm.Expect(hostAddresses(mergeRef.merge(hosts, true))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000", "10.0.1.1:3000", "10.0.1.2:3000"}))
			gm.Expect(mergeRef.merge(hosts, true)[1].TLSName).To(gm.BeEmpty())
			gm.Expect(hostAddresses(mergeRef.merge(hosts, false))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000", "10.0.1.1:3000", "10.0.1.2:3000"}))
			gm.Expect(hostAddresses(mergeRef.merge(nil, true))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(hostAddresses(mergeRef.merge([]*Host{}, false))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))

			replaceRef, _ := newTendRefresherOn(periodicDeadlineProvider([]*Host{}), SeedMergePolicyReplace, static, 100*time.Millisecond)
			gm.Expect(hostAddresses(replaceRef.merge(hosts, true))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000", "10.0.1.1:3000", "10.0.1.2:3000"}))
			laterReplace := replaceRef.merge(hosts, false)
			gm.Expect(hostAddresses(laterReplace)).To(gm.Equal([]string{"10.0.1.1:3000", "10.0.0.2:3000", "10.0.1.2:3000"}))
			gm.Expect(laterReplace[1].TLSName).To(gm.Equal("tls"))
			gm.Expect(hostAddresses(replaceRef.merge([]*Host{}, true))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(replaceRef.merge([]*Host{}, false)).To(gm.BeEmpty())

			onlyRef, _ := newTendRefresherOn(periodicDeadlineProvider([]*Host{}), SeedMergePolicyDiscoveryOnly, static, 100*time.Millisecond)
			gm.Expect(hostAddresses(onlyRef.merge(hosts, true))).To(gm.Equal([]string{"10.0.1.1:3000", "10.0.0.2:3000", "10.0.1.2:3000"}))
			gm.Expect(hostAddresses(onlyRef.merge(hosts, false))).To(gm.Equal([]string{"10.0.1.1:3000", "10.0.0.2:3000", "10.0.1.2:3000"}))
			gm.Expect(onlyRef.merge(hosts, false)[1].TLSName).To(gm.Equal("tls"))
			gm.Expect(hostAddresses(onlyRef.merge([]*Host{}, true))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
			gm.Expect(hostAddresses(onlyRef.merge([]*Host{}, false))).To(gm.Equal([]string{"10.0.0.1:3000", "10.0.0.2:3000"}))
		})

		gg.It("merge keeps the configured seeds first and de-duplicates", func() {
			refresher, _ := newTendRefresher(periodicDeadlineProvider(discovered), SeedMergePolicyMerge)

			merged, err := refresher.start()

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))
		})

		gg.It("merge later refresh is still static then discovered, and empty discovery falls back to static only", func() {
			provider := periodicDeadlineProvider([]*Host{d1}, []*Host{d2}, []*Host{})
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyMerge)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
			gm.Expect(recorder.count()).To(gm.Equal(0))

			refresher.tendTick(10)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-2.internal:3000"}))
			gm.Expect(recorder.count()).To(gm.Equal(1))

			refresher.tendTick(20)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(3)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(staticAddrs))
			gm.Expect(recorder.count()).To(gm.Equal(2))
		})

		gg.It("replace keeps only discovered seeds after initialization", func() {
			provider := periodicDeadlineProvider(discovered, []*Host{d2})
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyReplace)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))

			refresher.tendTick(10)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"found-2.internal:3000"}))
			gm.Expect(recorder.count()).To(gm.Equal(1))
		})

		gg.It("replace init with empty discovery keeps only the constructor seeds", func() {
			provider := periodicDeadlineProvider([]*Host{})
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyReplace)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal(staticAddrs))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("replace later empty discovery publishes an empty seed list", func() {
			provider := periodicDeadlineProvider([]*Host{d1}, []*Host{d2}, []*Host{})
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyReplace)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal([]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"}))

			refresher.tendTick(10)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"found-2.internal:3000"}))
			gm.Expect(recorder.count()).To(gm.Equal(1))

			refresher.tendTick(20)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(3)))
			gm.Expect(recorder.count()).To(gm.Equal(2))
			gm.Expect(recorder.last()).ToNot(gm.BeNil())
			gm.Expect(recorder.last()).To(gm.BeEmpty())
		})

		gg.It("discovery_only falls back to the configured seeds when discovery is empty", func() {
			refresher, recorder := newTendRefresher(periodicDeadlineProvider([]*Host{}), SeedMergePolicyDiscoveryOnly)

			merged, err := refresher.start()

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal(staticAddrs))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

		gg.It("discovery_only later refresh drops the constructor seeds, and empty discovery restores them", func() {
			provider := periodicDeadlineProvider([]*Host{}, []*Host{d1, d2}, []*Host{})
			refresher, recorder := newTendRefresher(provider, SeedMergePolicyDiscoveryOnly)

			merged, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(merged)).To(gm.Equal(staticAddrs))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
			gm.Expect(recorder.count()).To(gm.Equal(0))

			refresher.tendTick(10)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal([]string{"found-1.internal:3000", "found-2.internal:3000"}))
			gm.Expect(recorder.count()).To(gm.Equal(1))

			refresher.tendTick(20)
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(3)))
			gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(staticAddrs))
			gm.Expect(recorder.count()).To(gm.Equal(2))
		})

		gg.It("collapses the same host and port under different TLS names to the first", func() {
			first := &Host{Name: "seed-1.internal", Port: 3000, TLSName: "name-a"}
			second := &Host{Name: "seed-1.internal", Port: 3000, TLSName: "name-b"}

			distinct := distinctHosts([]*Host{first, second})

			gm.Expect(distinct).To(gm.HaveLen(1))
			gm.Expect(distinct[0].TLSName).To(gm.Equal("name-a"))
		})

		gg.DescribeTable("failed or overrunning later refresh keeps the snapshot and a successful empty one is published",
			func(policy SeedMergePolicy, seeds []*Host) {
				provider := periodicDeadlineProvider([]*Host{d1}, []*Host{d2}, nil, []*Host{}, []*Host{})
				refresher, recorder := newTendRefresherOn(provider, policy, seeds, 20*time.Millisecond)

				_, err := refresher.start()
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))

				refresher.tendTick(10)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
				gm.Expect(recorder.count()).To(gm.Equal(1))
				previous := hostAddresses(recorder.last())
				gm.Expect(previous).To(gm.ContainElement("found-2.internal:3000"))

				refresher.tendTick(20)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(3)))
				gm.Expect(recorder.count()).To(gm.Equal(1))
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(previous))

				provider.sleep.Store(int64(100 * time.Millisecond))
				refresher.tendTick(30)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(4)))
				gm.Expect(refresher.overrunCount).To(gm.Equal(1))
				gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())
				gm.Expect(recorder.count()).To(gm.Equal(1))
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(previous))

				emptyWant := hostAddresses(seeds)
				if policy == SeedMergePolicyReplace {
					emptyWant = []string{}
				}
				provider.sleep.Store(0)
				refresher.tendTick(40)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(5)))
				gm.Expect(refresher.overrunCount).To(gm.Equal(0))
				gm.Expect(recorder.count()).To(gm.Equal(2))
				gm.Expect(recorder.last()).ToNot(gm.BeNil())
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(emptyWant))
			},
			gg.Entry("merge with constructor seeds", SeedMergePolicyMerge, staticSeeds),
			gg.Entry("replace with constructor seeds", SeedMergePolicyReplace, staticSeeds),
			gg.Entry("discovery_only with constructor seeds", SeedMergePolicyDiscoveryOnly, staticSeeds),
			gg.Entry("merge with no constructor seeds", SeedMergePolicyMerge, []*Host(nil)),
			gg.Entry("replace with no constructor seeds", SeedMergePolicyReplace, []*Host(nil)),
			gg.Entry("discovery_only with no constructor seeds", SeedMergePolicyDiscoveryOnly, []*Host(nil)),
		)
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

		gg.It("rejects a dns timeout above the tend refresh deadline in tend mode", func() {
			policy := NewClientPolicy()
			policy.SeedProvider = dnsTimeoutProvider(200 * time.Millisecond)
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.DiscoveryTendRefreshDeadline = 100 * time.Millisecond

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("DNS seed provider timeout 200ms must be less or equal to the discovery tend refresh deadline 100ms"))
		})

		gg.It("does not check dns timeout against the tend deadline in thread mode", func() {
			provider := dnsTimeoutProvider(200 * time.Millisecond)
			defer provider.Close()
			policy := threadClientPolicy(provider, 0)
			policy.DiscoveryTendRefreshDeadline = 100 * time.Millisecond

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.It("skips tend-mode range checks when the provider is not periodic", func() {
			policy := NewClientPolicy()
			policy.DiscoveryExecutionMode = DiscoveryExecutionTend
			policy.DiscoveryRefreshInterval = 500 * time.Millisecond
			policy.DiscoveryTendRefreshDeadline = 600 * time.Millisecond
			policy.TendInterval = time.Second

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})

			gm.Expect(err).ToNot(gm.HaveOccurred())
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

		gg.It("discards an overrunning result, trips the circuit breaker after three consecutive overruns and restores the startup seeds", func() {
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

		gg.DescribeTable("trips restore the init-merge snapshot for every merge policy",
			func(policy SeedMergePolicy, initSeeds, laterSeeds []string) {
				found1 := NewHost("found-1.internal", 3000)
				found2 := NewHost("found-2.internal", 3000)
				provider := periodicDeadlineProvider([]*Host{found1}, []*Host{found2}, []*Host{found2}, []*Host{found2}, []*Host{found2})
				refresher, recorder := newTendRefresherWithDeadline(provider, policy, 20*time.Millisecond)

				merged, err := refresher.start()
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(hostAddresses(merged)).To(gm.Equal(initSeeds))
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
				gm.Expect(recorder.count()).To(gm.Equal(0))

				refresher.tendTick(10)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(laterSeeds))
				gm.Expect(recorder.count()).To(gm.Equal(1))

				provider.sleep.Store(int64(100 * time.Millisecond))
				refresher.tendTick(20)
				refresher.tendTick(30)
				gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(laterSeeds))
				gm.Expect(recorder.count()).To(gm.Equal(1))

				refresher.tendTick(40)
				gm.Expect(provider.calls.Load()).To(gm.Equal(int64(5)))
				gm.Expect(refresher.tripped.Get()).To(gm.BeTrue())
				gm.Expect(hostAddresses(recorder.last())).To(gm.Equal(initSeeds))
				gm.Expect(recorder.count()).To(gm.Equal(2))

				callsAtTrip := provider.calls.Load()
				refresher.tendTick(50)
				gm.Expect(provider.calls.Load()).To(gm.Equal(callsAtTrip))
			},
			gg.Entry("merge", SeedMergePolicyMerge,
				[]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"},
				[]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-2.internal:3000"}),
			gg.Entry("replace", SeedMergePolicyReplace,
				[]string{"seed-1.internal:3000", "seed-2.internal:3000", "found-1.internal:3000"},
				[]string{"found-2.internal:3000"}),
			gg.Entry("discovery_only", SeedMergePolicyDiscoveryOnly,
				[]string{"found-1.internal:3000"},
				[]string{"found-2.internal:3000"}),
		)

		gg.It("resets the overrun count after an on-time refresh so the circuit breaker does not trip", func() {
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

		gg.It("fails start when discovery errors and no seed is left", func() {
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

		newThreadRefresherWithTimeout := func(provider SeedCandidateProvider, timeout time.Duration) (*seedRefresher, *publishRecorder) {
			return mustThreadRefresher(provider, staticSeeds, timeout)
		}

		newThreadRefresher := func(provider SeedCandidateProvider) (*seedRefresher, *publishRecorder) {
			return mustThreadRefresher(provider, staticSeeds, 0)
		}

		gg.It("rejects a refresh interval below the tend interval", func() {
			policy := threadClientPolicy(&scriptedSeedProvider{results: [][]*Host{{}}, periodic: true}, 0)
			policy.DiscoveryRefreshInterval = 500 * time.Millisecond
			policy.TendInterval = time.Second

			_, err := newSeedRefresher(policy, staticSeeds, func([]*Host) {})

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.ContainSubstring("must be greater or equal to the tend interval"))
		})

		gg.It("rejects a dns timeout above the refresh timeout", func() {
			provider := dnsTimeoutProvider(400 * time.Millisecond)
			defer provider.Close()
			_, err := newSeedRefresher(threadClientPolicy(provider, 80*time.Millisecond), staticSeeds, func([]*Host) {})
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())
			gm.Expect(err.Error()).To(gm.ContainSubstring("DNS seed provider timeout 400ms must be less or equal to the discovery refresh timeout 80ms"))
		})

		gg.It("constructs when a dns timeout equals the refresh timeout", func() {
			provider := dnsTimeoutProvider(80 * time.Millisecond)
			_, err := newSeedRefresher(threadClientPolicy(provider, 80*time.Millisecond), staticSeeds, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())
			provider.Close()
		})

		gg.It("rejects an aws timeout above the refresh timeout", func() {
			provider := newAwsStubProvider("", 400*time.Millisecond)
			_, err := newSeedRefresher(threadClientPolicy(provider, 80*time.Millisecond), staticSeeds, func([]*Host) {})
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())
			gm.Expect(err.Error()).To(gm.ContainSubstring("AWS EC2 seed provider timeout 400ms must be less or equal to the discovery refresh timeout 80ms"))
			provider.Close()
		})

		gg.It("constructs when an aws timeout equals the refresh timeout", func() {
			provider := newAwsStubProvider("", 80*time.Millisecond)
			_, err := newSeedRefresher(threadClientPolicy(provider, 80*time.Millisecond), staticSeeds, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())
			provider.Close()
		})

		gg.It("does not deadlock when the first lookup finishes before start waits", func() {
			gm.Expect(exerciseOnTimeHandoff(staticSeeds)).To(gm.Succeed())
		})

		gg.It("does not deadlock when start gives up while the first lookup is still running", func() {
			gm.Expect(exerciseLateHandoff(staticSeeds)).To(gm.Succeed())
		})

		gg.It("does not deadlock when on-time and late first handoffs run together", func() {
			const n = 8
			errs := make(chan error, n)
			var wg sync.WaitGroup
			wg.Add(n)
			for i := 0; i < n; i++ {
				go func(late bool) {
					defer wg.Done()
					errs <- exerciseFirstHandoff(staticSeeds, late)
				}(i%2 == 0)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		gg.It("does not publish the first lookup when it fails after the construction wait expires", func() {
			provider := &scriptedSeedProvider{
				results:  [][]*Host{nil},
				periodic: true,
			}
			provider.sleep.Store(int64(200 * time.Millisecond))
			refresher, recorder := newThreadRefresherWithTimeout(provider, 50*time.Millisecond)
			defer refresher.close()

			started, waitErr := waitStart(beginStart(refresher), 2*time.Second)
			gm.Expect(waitErr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(started.seeds)).To(gm.Equal(hostAddresses(staticSeeds)))
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Consistently(func() int { return recorder.count() }, "400ms", "20ms").Should(gm.BeZero())
		})

		gg.It("publishes an empty later refresh instead of keeping the previous snapshot", func() {
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}, {}},
				periodic: true,
			}
			policy := threadClientPolicy(provider, 0)
			policy.SeedMergePolicy = SeedMergePolicyReplace
			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(policy, staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			refresher.activate()
			refresher.markTending()
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.ContainElement("found-1.internal:3000"))

			refresher.requestUrgentRefresh()
			gm.Eventually(func() int { return recorder.count() }, "2s", "10ms").Should(gm.Equal(1))
			gm.Expect(recorder.last()).To(gm.BeEmpty())
		})

		gg.It("does not trip the circuit breaker after three overruns and still publishes each successful result", func() {
			provider := &scriptedSeedProvider{
				results: [][]*Host{
					{NewHost("late-1.internal", 3000)},
					{NewHost("late-2.internal", 3000)},
					{NewHost("late-3.internal", 3000)},
				},
				periodic: true,
			}
			provider.sleep.Store(int64(80 * time.Millisecond))
			refresher, recorder := newThreadRefresherWithTimeout(provider, 20*time.Millisecond)
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal(hostAddresses(staticSeeds)))

			gm.Eventually(func() []string { return hostAddresses(recorder.last()) }, "2s", "10ms").Should(gm.Equal([]string{
				"seed-1.internal:3000", "seed-2.internal:3000", "late-1.internal:3000",
			}))
			gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())

			triggerRefresh(refresher)
			gm.Eventually(func() []string { return hostAddresses(recorder.last()) }, "2s", "10ms").Should(gm.Equal([]string{
				"seed-1.internal:3000", "seed-2.internal:3000", "late-2.internal:3000",
			}))
			gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())

			triggerRefresh(refresher)
			gm.Eventually(func() []string { return hostAddresses(recorder.last()) }, "2s", "10ms").Should(gm.Equal([]string{
				"seed-1.internal:3000", "seed-2.internal:3000", "late-3.internal:3000",
			}))
			gm.Expect(refresher.tripped.Get()).To(gm.BeFalse())
		})

		gg.It("fails start when discovery errors and no seed is left", func() {
			policy := threadClientPolicy(&scriptedSeedProvider{results: [][]*Host{nil}, periodic: true}, 0)
			policy.SeedMergePolicy = SeedMergePolicyReplace
			refresher, err := newSeedRefresher(policy, nil, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer refresher.close()

			_, serr := refresher.start()

			gm.Expect(serr).To(gm.HaveOccurred())
			gm.Expect(serr.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
			gm.Expect(serr.Error()).To(gm.ContainSubstring("no seed host is left"))
		})

		gg.It("fails start when a panicking periodic provider leaves no seed", func() {
			refresher, _ := mustThreadRefresher(&panickingSeedProvider{}, nil, 0)
			defer refresher.close()

			seeds, serr := refresher.start()

			gm.Expect(seeds).To(gm.BeNil())
			gm.Expect(serr).To(gm.HaveOccurred())
			gm.Expect(serr.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
			gm.Expect(serr.Error()).To(gm.ContainSubstring("seed provider panicked"))
		})

		gg.It("starts from constructor seeds when a panicking periodic provider fails the first refresh", func() {
			refresher, recorder := newThreadRefresher(&panickingSeedProvider{})
			defer refresher.close()

			seeds, serr := refresher.start()

			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal(hostAddresses(staticSeeds)))
			gm.Expect(recorder.count()).To(gm.BeZero())
		})

		gg.It("fails start when a one-shot provider errors and no seed is left", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{nil}}
			refresher, _ := mustThreadRefresher(provider, nil, 0)
			defer refresher.close()

			_, serr := refresher.start()

			gm.Expect(serr).To(gm.HaveOccurred())
			gm.Expect(serr.Matches(types.SEED_DISCOVERY_FAILED)).To(gm.BeTrue())
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
			gm.Consistently(func() int64 { return provider.calls.Load() }, "200ms", "20ms").Should(gm.Equal(int64(1)))
		})

		gg.It("starts from constructor seeds when a one-shot provider errors and does not refresh again", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{nil}}
			refresher, recorder := newThreadRefresher(provider)
			defer refresher.close()

			seeds, serr := refresher.start()

			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal(hostAddresses(staticSeeds)))
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
			gm.Consistently(func() int64 { return provider.calls.Load() }, "200ms", "20ms").Should(gm.Equal(int64(1)))
			gm.Expect(recorder.count()).To(gm.BeZero())
		})

		gg.It("keeps the current seeds when a later periodic refresh fails and publishes again when it recovers", func() {
			provider := &scriptedSeedProvider{
				results: [][]*Host{
					{NewHost("found-1.internal", 3000)},
					nil,
					{NewHost("found-3.internal", 3000)},
				},
				periodic: true,
			}
			refresher, recorder := newThreadRefresher(provider)
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.ContainElement("found-1.internal:3000"))

			refresher.requestUrgentRefresh()
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(2)))
			gm.Consistently(func() int { return recorder.count() }, "200ms", "20ms").Should(gm.Equal(0))

			triggerRefresh(refresher)
			gm.Eventually(func() []string {
				return hostAddresses(recorder.last())
			}, "2s", "10ms").Should(gm.ContainElement("found-3.internal:3000"))
		})

		gg.It("keeps the current seeds when a later periodic refresh blocks on I/O then fails, and continues after", func() {
			release := make(chan struct{})
			provider := &scriptedSeedProvider{
				results: [][]*Host{
					{NewHost("found-1.internal", 3000)},
					nil,
					{NewHost("found-3.internal", 3000)},
				},
				periodic: true,
				blockOn:  release,
			}
			refresher, recorder := newThreadRefresher(provider)
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.ContainElement("found-1.internal:3000"))

			refresher.requestUrgentRefresh()
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(2)))
			gm.Consistently(func() int64 { return provider.calls.Load() }, "150ms", "20ms").Should(gm.Equal(int64(2)))
			gm.Expect(recorder.count()).To(gm.Equal(0))

			close(release)
			gm.Consistently(func() int { return recorder.count() }, "200ms", "20ms").Should(gm.Equal(0))

			triggerRefresh(refresher)
			gm.Eventually(func() []string {
				return hostAddresses(recorder.last())
			}, "2s", "10ms").Should(gm.ContainElement("found-3.internal:3000"))
		})

		gg.It("keeps the current seeds when a later periodic refresh panics and the loop continues", func() {
			provider := &panickingAfterSeedProvider{hosts: []*Host{NewHost("found-1.internal", 3000)}, after: 1, periodic: true}
			refresher, recorder := newThreadRefresher(provider)
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.ContainElement("found-1.internal:3000"))

			refresher.requestUrgentRefresh()
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.BeNumerically(">=", 2))
			gm.Consistently(func() int { return recorder.count() }, "200ms", "20ms").Should(gm.Equal(0))

			triggerRefresh(refresher)
			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.BeNumerically(">=", 3))
			gm.Expect(recorder.count()).To(gm.Equal(0))
		})

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

			// A request during the running refresh does nothing, even with the
			// rate limit cleared.
			triggerRefresh(refresher)
			close(release)

			gm.Consistently(func() int64 { return provider.calls.Load() }, "300ms", "20ms").Should(gm.Equal(int64(2)))
			refresher.close()
		})

		gg.Describe("urgent refresh gate", func() {
			// The refresh interval is an hour, so the periodic timer never
			// fires here: every provider call after the first is an honoured
			// urgent request.
			startUrgentRefresher := func(provider *scriptedSeedProvider, refreshTimeout time.Duration, tending bool) *seedRefresher {
				refresher, err := newSeedRefresher(threadClientPolicy(provider, refreshTimeout), staticSeeds, func([]*Host) {})
				gm.Expect(err).ToNot(gm.HaveOccurred())
				refresher.activate()
				if tending {
					refresher.markTending()
				}
				gg.DeferCleanup(refresher.close)

				_, serr := refresher.start()
				gm.Expect(serr).ToNot(gm.HaveOccurred())
				return refresher
			}

			quickProvider := func() *scriptedSeedProvider {
				return &scriptedSeedProvider{results: [][]*Host{{NewHost("found-1.internal", 3000)}}, periodic: true}
			}

			expectCalls := func(provider *scriptedSeedProvider, n int64) {
				gm.Eventually(provider.calls.Load, "2s", "10ms").Should(gm.Equal(n))
				gm.Consistently(provider.calls.Load, "200ms", "20ms").Should(gm.Equal(n))
			}

			waitIdle := func(refresher *seedRefresher) {
				gm.Eventually(refresher.refreshing.Get, "2s", "5ms").Should(gm.BeFalse())
			}

			gg.It("honours the first request once the client is tending", func() {
				provider := quickProvider()
				refresher := startUrgentRefresher(provider, 0, true)
				expectCalls(provider, 1)

				refresher.requestUrgentRefresh()

				expectCalls(provider, 2)
				gm.Expect(refresher.lastUrgent.Load()).To(gm.BeNumerically(">", 0))
			})

			gg.It("makes one provider call per interval while tend keeps asking during an outage", func() {
				provider := quickProvider()
				refresher := startUrgentRefresher(provider, 0, true)
				expectCalls(provider, 1)

				refresher.requestUrgentRefresh()
				expectCalls(provider, 2)
				waitIdle(refresher)
				honoured := refresher.lastUrgent.Load()

				// Tend with zero nodes asks on every tend; simulate 60 tends.
				for tend := 0; tend < 60; tend++ {
					refresher.requestUrgentRefresh()
					time.Sleep(5 * time.Millisecond)
				}

				expectCalls(provider, 2)
				gm.Expect(refresher.urgentCh).To(gm.BeEmpty())
				gm.Expect(refresher.lastUrgent.Load()).To(gm.Equal(honoured))
			})

			gg.It("honours a request again once a refresh interval has passed", func() {
				provider := quickProvider()
				refresher := startUrgentRefresher(provider, 0, true)
				expectCalls(provider, 1)

				refresher.requestUrgentRefresh()
				expectCalls(provider, 2)
				waitIdle(refresher)

				refresher.requestUrgentRefresh()
				expectCalls(provider, 2)

				// Move the refresher's clock origin back one interval, as if an
				// hour had passed. Only this goroutine reads created.
				refresher.created = refresher.created.Add(-time.Hour)
				refresher.requestUrgentRefresh()

				expectCalls(provider, 3)
			})

			gg.It("ignores requests from construction tends without using up the interval", func() {
				provider := quickProvider()
				refresher := startUrgentRefresher(provider, 0, false)
				expectCalls(provider, 1)

				for i := 0; i < 3; i++ {
					refresher.requestUrgentRefresh()
				}

				expectCalls(provider, 1)
				gm.Expect(refresher.urgentCh).To(gm.BeEmpty())
				gm.Expect(refresher.lastUrgent.Load()).To(gm.BeZero())

				// The first outage after the tend goroutine starts is refreshed at once.
				refresher.markTending()
				refresher.requestUrgentRefresh()
				expectCalls(provider, 2)
			})

			gg.It("does not use up the interval with a request ignored during an in-flight refresh", func() {
				provider := quickProvider()
				provider.sleep.Store(int64(300 * time.Millisecond))
				// start gives up after 50ms while the first refresh keeps running.
				refresher := startUrgentRefresher(provider, 50*time.Millisecond, true)
				gm.Expect(refresher.refreshing.Get()).To(gm.BeTrue())

				refresher.requestUrgentRefresh()
				gm.Expect(refresher.lastUrgent.Load()).To(gm.BeZero())
				gm.Expect(refresher.urgentCh).To(gm.BeEmpty())

				waitIdle(refresher)
				expectCalls(provider, 1)

				refresher.requestUrgentRefresh()
				gm.Eventually(provider.calls.Load, "2s", "10ms").Should(gm.Equal(int64(2)))
			})

			gg.It("lets exactly one of many concurrent requests through", func() {
				provider := quickProvider()
				refresher := startUrgentRefresher(provider, 0, true)
				expectCalls(provider, 1)

				const callers = 32
				gate := make(chan struct{})
				var callersDone sync.WaitGroup
				callersDone.Add(callers)
				for i := 0; i < callers; i++ {
					go func() {
						defer callersDone.Done()
						<-gate
						refresher.requestUrgentRefresh()
					}()
				}
				close(gate)
				callersDone.Wait()

				expectCalls(provider, 2)
				gm.Expect(refresher.lastUrgent.Load()).To(gm.BeNumerically(">", 0))
			})
		})

		gg.It("close returns within the refresh timeout and closes the provider only after a hung refresh returns", func() {
			// A server that accepts connections and never responds.
			silentServer := mustListenLocal()
			defer silentServer.Close()
			go func() {
				for {
					if _, acceptErr := silentServer.Accept(); acceptErr != nil {
						return
					}
				}
			}()

			provider := newHangingSeedProvider("http://" + silentServer.Addr().String())
			refresher, _ := newThreadRefresherWithTimeout(provider, 150*time.Millisecond)

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

			// The refresh is still in flight, so the provider stays open.
			gm.Consistently(func() int64 { return provider.closeCalls.Load() }, "200ms", "20ms").Should(gm.BeZero())
			gm.Expect(provider.refreshReturned.Load()).To(gm.BeZero())

			// Once the hung call returns, the goroutine closes the provider.
			provider.cancel()
			gm.Eventually(func() int64 { return provider.closeCalls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Expect(provider.refreshReturned.Load()).To(gm.Equal(int64(1)))
		})

		gg.It("never calls Close while a refresh is in flight", func() {
			release := make(chan struct{})
			provider := &overlapSeedProvider{release: release}
			refresher, _ := newThreadRefresherWithTimeout(provider, 50*time.Millisecond)

			_, err := refresher.start()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			refresher.close()
			close(release)

			gm.Eventually(func() int64 { return provider.closeCalls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Expect(provider.overlapped.Load()).To(gm.BeFalse())
		})

		gg.It("abort returns without waiting for a refresh in flight and the provider closes after it returns", func() {
			release := make(chan struct{})
			provider := &overlapSeedProvider{release: release}
			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(threadClientPolicy(provider, 200*time.Millisecond), staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			began := time.Now()
			refresher.abort()
			gm.Expect(time.Since(began)).To(gm.BeNumerically("<", 100*time.Millisecond))
			gm.Expect(provider.closeCalls.Load()).To(gm.BeZero())

			refresher.close()
			gm.Expect(time.Since(began)).To(gm.BeNumerically("<", 100*time.Millisecond))

			close(release)
			gm.Eventually(func() int64 { return provider.closeCalls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Expect(provider.overlapped.Load()).To(gm.BeFalse())
			gm.Consistently(func() int { return recorder.count() }, "200ms", "20ms").Should(gm.BeZero())
		})

		gg.It("abort closes the provider at once when no refresh goroutine runs", func() {
			provider := &scriptedSeedProvider{results: [][]*Host{{NewHost("found-1.internal", 3000)}}}
			refresher, err := newSeedRefresher(threadClientPolicy(provider, 0), staticSeeds, func([]*Host) {})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			refresher.abort()
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))
			refresher.close()
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))
		})

		gg.It("a loop restarted after a panic during close makes no further provider call", func() {
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}, {NewHost("found-2.internal", 3000)}},
				periodic: true,
			}
			entered := make(chan struct{})
			proceed := make(chan struct{})
			var publishes atomic.Int64
			publish := func([]*Host) {
				if publishes.Add(1) == 1 {
					close(entered)
					<-proceed
					panic("publish failure")
				}
			}
			refresher, err := newSeedRefresher(threadClientPolicy(provider, 0), staticSeeds, publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			refresher.activate()
			refresher.markTending()

			_, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			refresher.requestUrgentRefresh()
			gm.Eventually(entered, "2s", "10ms").Should(gm.BeClosed())

			closed := make(chan struct{})
			go func() {
				refresher.close()
				close(closed)
			}()
			gm.Eventually(refresher.stopCh, "2s", "10ms").Should(gm.BeClosed())

			close(proceed)
			gm.Eventually(closed, "2s", "10ms").Should(gm.BeClosed())
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(2)))
			gm.Expect(provider.closeCalls.Load()).To(gm.Equal(int64(1)))
		})

		gg.It("publishes a late first result only after activate", func() {
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}},
				periodic: true,
			}
			provider.sleep.Store(int64(150 * time.Millisecond))
			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(threadClientPolicy(provider, 50*time.Millisecond), staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer refresher.close()

			seeds, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())
			gm.Expect(hostAddresses(seeds)).To(gm.Equal(hostAddresses(staticSeeds)))

			gm.Eventually(func() int64 { return provider.calls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Consistently(func() int { return recorder.count() }, "300ms", "20ms").Should(gm.BeZero())

			refresher.activate()
			gm.Eventually(func() int { return recorder.count() }, "2s", "10ms").Should(gm.Equal(1))
			gm.Expect(hostAddresses(recorder.last())).To(gm.ContainElement("found-1.internal:3000"))
		})

		gg.It("drops a late first result when construction fails, and closes the provider", func() {
			provider := &scriptedSeedProvider{
				results:  [][]*Host{{NewHost("found-1.internal", 3000)}},
				periodic: true,
			}
			provider.sleep.Store(int64(150 * time.Millisecond))
			recorder := &publishRecorder{}
			refresher, err := newSeedRefresher(threadClientPolicy(provider, 50*time.Millisecond), staticSeeds, recorder.publish)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, serr := refresher.start()
			gm.Expect(serr).ToNot(gm.HaveOccurred())

			// Construction fails: close without activate.
			refresher.close()

			gm.Eventually(func() int64 { return provider.closeCalls.Load() }, "2s", "10ms").Should(gm.Equal(int64(1)))
			gm.Consistently(func() int { return recorder.count() }, "300ms", "20ms").Should(gm.BeZero())
			gm.Expect(provider.calls.Load()).To(gm.Equal(int64(1)))
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

// overlapSeedProvider blocks every refresh until release closes and records
// whether Close ran while a refresh was still in flight.
type overlapSeedProvider struct {
	release    chan struct{}
	inFlight   atomic.Bool
	overlapped atomic.Bool
	closeCalls atomic.Int64
}

func (provider *overlapSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	provider.inFlight.Store(true)
	defer provider.inFlight.Store(false)
	<-provider.release
	return []*Host{NewHost("found-1.internal", 3000)}, nil
}

func (provider *overlapSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (provider *overlapSeedProvider) SupportsDeadline() bool     { return false }

func (provider *overlapSeedProvider) Close() {
	if provider.inFlight.Load() {
		provider.overlapped.Store(true)
	}
	provider.closeCalls.Add(1)
}

type panickingSeedProvider struct{}

func (panickingSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	panic("provider failure")
}
func (panickingSeedProvider) NeedsPeriodicRefresh() bool { return true }
func (panickingSeedProvider) SupportsDeadline() bool     { return true }
func (panickingSeedProvider) Close()                     {}

// panickingAfterSeedProvider succeeds after times, then panics on every later call.
type panickingAfterSeedProvider struct {
	hosts    []*Host
	after    int64
	periodic bool
	calls    atomic.Int64
}

func (provider *panickingAfterSeedProvider) RefreshSeedCandidates(*SeedCandidateContext) ([]*Host, error) {
	if provider.calls.Add(1) > provider.after {
		panic("provider failure")
	}
	return copyHosts(provider.hosts), nil
}
func (provider *panickingAfterSeedProvider) NeedsPeriodicRefresh() bool { return provider.periodic }
func (*panickingAfterSeedProvider) SupportsDeadline() bool              { return false }
func (*panickingAfterSeedProvider) Close()                              {}
