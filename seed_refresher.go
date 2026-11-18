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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const (
	seedRefreshMaxOverruns         = 3
	defaultDiscoveryRefreshTimeout = 10 * time.Second
)

// seedRefresher runs the configured SeedCandidateProvider and publishes the
// merged seed list. In thread mode a periodic provider gets one dedicated
// goroutine; in tend mode it runs on the tend goroutine under a declared
// deadline. The static default never starts a goroutine.
type seedRefresher struct {
	provider     SeedCandidateProvider
	providerName string
	publish      func([]*Host)

	clusterName string
	staticSeeds []*Host
	mergePolicy SeedMergePolicy

	executionMode   DiscoveryExecutionMode
	periodic        bool
	refreshInterval time.Duration
	refreshTimeout  time.Duration
	tendDeadline    time.Duration
	tendInterval    time.Duration
	refreshCadence  int

	// startupSeeds is the list published at initialization, and the overrun trip
	// restores exactly it.
	startupSeeds []*Host

	// Tend-mode consecutive overruns. Thread mode counts overruns on the
	// same field and never trips. tripped is atomic so tests can read it.
	overrunCount int
	tripped      iatomic.Bool

	// Thread-mode loop lifecycle. refreshing is set while the loop is
	// inside a provider call, so an urgent request then does nothing.
	// activeCh is closed once the client is built; until then the loop
	// publishes nothing. A running loop owns provider.Close.
	stopCh      chan struct{}
	urgentCh    chan struct{}
	activeCh    chan struct{}
	refreshing  iatomic.Bool
	loopStarted iatomic.Bool
	loopWg      sync.WaitGroup
	closeOnce   sync.Once

	// Urgent refresh gate. tending is set once the tend goroutine starts;
	// construction tends request nothing. lastUrgent is the monotonic time
	// since created of the last honoured request, 0 when none yet.
	created    time.Time
	tending    iatomic.Bool
	lastUrgent atomic.Int64

	// firstDoneCh is closed when the first lookup is stored. start waits on
	// it; the refresh goroutine never sends, so it cannot block. firstOnce
	// is the first hand-off only (panic-restart must not run it again).
	firstMu        sync.Mutex
	firstAbandoned bool
	firstResult    firstRefreshResult
	firstDoneCh    chan struct{}
	firstOnce      sync.Once
}

// firstRefreshResult is the first thread-mode lookup, handed to start or
// published if start already left.
type firstRefreshResult struct {
	hosts []*Host
	err   error
}

// newSeedRefresher validates the discovery settings and returns a refresher
// that is not yet started. staticSeeds are the constructor hosts after TLS
// name defaulting; publish stores a new seed list on the cluster.
func newSeedRefresher(policy *ClientPolicy, staticSeeds []*Host, publish func([]*Host)) (*seedRefresher, Error) {
	provider := policy.SeedProvider
	if provider == nil {
		provider = NewStaticSeedCandidateProvider(staticSeeds...)
	}

	refresher := &seedRefresher{
		provider:        provider,
		providerName:    seedProviderDisplayName(provider),
		publish:         publish,
		clusterName:     policy.ClusterName,
		staticSeeds:     copyHosts(staticSeeds),
		mergePolicy:     policy.SeedMergePolicy,
		executionMode:   policy.DiscoveryExecutionMode,
		periodic:        provider.NeedsPeriodicRefresh(),
		refreshInterval: policy.DiscoveryRefreshInterval,
		refreshTimeout:  policy.DiscoveryRefreshTimeout,
		tendDeadline:    policy.DiscoveryTendRefreshDeadline,
		tendInterval:    policy.TendInterval,
		stopCh:          make(chan struct{}),
		urgentCh:        make(chan struct{}, 1),
		activeCh:        make(chan struct{}),
		firstDoneCh:     make(chan struct{}),
		created:         time.Now(),
	}

	if refresher.refreshInterval <= 0 {
		refresher.refreshInterval = 30 * time.Second
	}
	if refresher.refreshTimeout <= 0 {
		refresher.refreshTimeout = defaultDiscoveryRefreshTimeout
	}
	if refresher.tendDeadline <= 0 {
		refresher.tendDeadline = 100 * time.Millisecond
	}
	if refresher.tendInterval < 10*time.Millisecond {
		// The tend loop floors its interval the same way.
		refresher.tendInterval = 10 * time.Millisecond
	}

	if err := refresher.validate(policy); err != nil {
		return nil, err
	}

	cadence := int(refresher.refreshInterval / refresher.tendInterval)
	if cadence < 1 {
		cadence = 1
	}
	refresher.refreshCadence = cadence

	return refresher, nil
}

func (refresher *seedRefresher) validate(policy *ClientPolicy) Error {
	if !refresher.periodic {
		return nil
	}
	if refresher.refreshInterval < refresher.tendInterval {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Discovery refresh interval %s must be greater or equal to the tend interval %s", refresher.refreshInterval, refresher.tendInterval))
	}
	if refresher.executionMode != DiscoveryExecutionTend {
		return refresher.validateProviderTimeout(refresher.refreshTimeout, "discovery refresh timeout")
	}
	if !refresher.provider.SupportsDeadline() {
		return newError(types.PARAMETER_ERROR, "Seed provider "+refresher.providerName+" does not support a deadline and cannot run in tend execution mode. Use thread execution mode")
	}
	if refresher.tendDeadline > refresher.tendInterval/2 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid discovery tend refresh deadline %s. Must be greater than zero and at most half the tend interval %s", refresher.tendDeadline, refresher.tendInterval))
	}
	return refresher.validateProviderTimeout(refresher.tendDeadline, "discovery tend refresh deadline")
}

func (refresher *seedRefresher) validateProviderTimeout(limit time.Duration, limitName string) Error {
	switch provider := refresher.provider.(type) {
	case *dnsSeedCandidateProvider:
		if provider.timeout > limit {
			return newError(types.PARAMETER_ERROR, fmt.Sprintf("DNS seed provider timeout %s must be less or equal to the %s %s", provider.timeout, limitName, limit))
		}
	case *awsEc2TagSeedCandidateProvider:
		timeout := provider.client.Config().Timeout
		if timeout > limit {
			return newError(types.PARAMETER_ERROR, fmt.Sprintf("AWS EC2 seed provider timeout %s must be less or equal to the %s %s", timeout, limitName, limit))
		}
	}
	return nil
}

// start runs the initial refresh and, for a periodic provider in thread
// mode, starts the refresh goroutine. It returns the initial seed list and
// fails only when the provider errored and no seed is left.
func (refresher *seedRefresher) start() ([]*Host, Error) {
	if refresher.periodic && refresher.executionMode == DiscoveryExecutionThread {
		return refresher.startThreadMode()
	}

	discovered, err := refresher.refreshOnce(0)
	return refresher.resolveStartupSeeds(discovered, err)
}

// activate lets the refresh goroutine publish. Call once, after the client
// is built; a client that fails to build calls close instead.
func (refresher *seedRefresher) activate() {
	close(refresher.activeCh)
}

func (refresher *seedRefresher) startThreadMode() ([]*Host, Error) {
	refresher.loopStarted.Set(true)
	refresher.loopWg.Add(1)
	go refresher.refreshLoop()

	select {
	case <-refresher.firstDoneCh:
		return refresher.takeFirstResult()
	case <-time.After(refresher.refreshTimeout):
		refresher.firstMu.Lock()
		select {
		case <-refresher.firstDoneCh:
			res := refresher.firstResult
			refresher.firstMu.Unlock()
			return refresher.resolveStartupSeeds(res.hosts, res.err)
		default:
		}
		refresher.firstAbandoned = true
		refresher.firstMu.Unlock()
		logger.Logger.Warn("Seed provider %s did not finish the initial refresh within %s; starting from the configured seeds", refresher.providerName, refresher.refreshTimeout)
		return refresher.resolveStartupSeeds(nil, fmt.Errorf("the initial refresh did not finish within %s", refresher.refreshTimeout))
	}
}

func (refresher *seedRefresher) takeFirstResult() ([]*Host, Error) {
	refresher.firstMu.Lock()
	res := refresher.firstResult
	refresher.firstMu.Unlock()
	return refresher.resolveStartupSeeds(res.hosts, res.err)
}

// resolveStartupSeeds merges the first discovery result with the constructor
// seeds, stores that list for later overrun restore, and fails only when the
// provider errored and no seed is left.
func (refresher *seedRefresher) resolveStartupSeeds(discovered []*Host, refreshErr error) ([]*Host, Error) {
	merged := refresher.merge(discovered, true)
	if refreshErr != nil && len(merged) == 0 {
		return nil, newError(types.SEED_DISCOVERY_FAILED, "Seed provider "+refresher.providerName+" failed and no seed host is left: "+refreshErr.Error())
	}

	refresher.startupSeeds = copyHosts(merged)
	return merged, nil
}

// refreshLoop is the thread-mode goroutine. The first result is handed to
// start, or published once the client is built if start already gave up
// waiting. The loop closes the provider as it exits, after its last call.
func (refresher *seedRefresher) refreshLoop() {
	defer refresher.loopWg.Done()

	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Logger.Error("Seed refresh goroutine crashed and restarts: %v", recovered)
			refresher.loopWg.Add(1)
			go refresher.refreshLoop()
		}
	}()

	for iteration := int64(0); ; iteration++ {
		select {
		case <-refresher.stopCh:
			refresher.closeProvider()
			return
		default:
		}

		refresher.refreshing.Set(true)
		began := time.Now()
		discovered, err := refresher.refreshOnce(iteration)
		refresher.refreshing.Set(false)
		refresher.noteThreadOverrun(began)

		report := iteration > 0
		if iteration == 0 {
			report = refresher.handOffFirst(discovered, err)
			select {
			case <-refresher.activeCh:
			case <-refresher.stopCh:
				refresher.closeProvider()
				return
			}
		}

		if report {
			if err != nil {
				logger.Logger.Warn("Seed refresh by provider %s failed; keeping the current seeds: %s", refresher.providerName, err.Error())
			} else {
				refresher.publish(refresher.merge(discovered, iteration == 0))
			}
		}

		select {
		case <-refresher.stopCh:
			refresher.closeProvider()
			return
		case <-refresher.urgentCh:
		case <-time.After(refresher.refreshInterval):
		}
	}
}

// handOffFirst gives the first result to start. It returns true when start
// already gave up waiting, so the loop publishes the result itself.
func (refresher *seedRefresher) handOffFirst(discovered []*Host, err error) (late bool) {
	refresher.firstOnce.Do(func() {
		refresher.firstMu.Lock()
		defer refresher.firstMu.Unlock()
		if refresher.firstAbandoned {
			late = true
			return
		}
		refresher.firstResult = firstRefreshResult{hosts: discovered, err: err}
		close(refresher.firstDoneCh)
	})
	return late
}

func (refresher *seedRefresher) noteThreadOverrun(began time.Time) {
	elapsed := time.Since(began)
	if elapsed <= refresher.refreshTimeout {
		return
	}
	refresher.overrunCount++
	logger.Logger.Warn("Seed refresh by provider %s took %s, exceeding refresh timeout %s. Overruns: %d",
		refresher.providerName, elapsed, refresher.refreshTimeout, refresher.overrunCount)
}

// tendTick runs one deadline-measured refresh on the tend goroutine every
// refresh interval worth of tends. Three consecutive overruns disable the
// provider and restore the startup seeds.
func (refresher *seedRefresher) tendTick(tendCount int) {
	if refresher == nil || !refresher.periodic || refresher.executionMode != DiscoveryExecutionTend || refresher.tripped.Get() {
		return
	}
	if tendCount%refresher.refreshCadence != 0 {
		return
	}

	began := time.Now()
	discovered, err := refresher.refreshOnce(int64(tendCount))
	elapsed := time.Since(began)

	if elapsed > refresher.tendDeadline {
		refresher.overrunCount++
		logger.Logger.Warn("Seed refresh by provider %s took %s, exceeding the tend refresh deadline %s. Consecutive overruns: %d", refresher.providerName, elapsed, refresher.tendDeadline, refresher.overrunCount)
		if refresher.overrunCount >= seedRefreshMaxOverruns {
			refresher.tripped.Set(true)
			refresher.publish(copyHosts(refresher.startupSeeds))
			logger.Logger.Error("Seed provider %s exceeded the tend refresh deadline %d consecutive times. The provider is disabled for this client and the initial seeds are restored", refresher.providerName, refresher.overrunCount)
		}
		return
	}

	refresher.overrunCount = 0
	if err != nil {
		logger.Logger.Warn("Seed refresh by provider %s failed; keeping the current seeds: %s", refresher.providerName, err.Error())
		return
	}
	refresher.publish(refresher.merge(discovered, false))
}

// markTending lets the tend goroutine request urgent refreshes. Call it
// when that goroutine starts; the tends during construction request none.
func (refresher *seedRefresher) markTending() {
	refresher.tending.Set(true)
}

// requestUrgentRefresh wakes the thread-mode loop so a client with no nodes
// does not wait out the refresh interval. It is honoured at most once per
// refresh interval and does nothing while a refresh runs. It never blocks;
// the caller reconnects from the seeds already in hand.
func (refresher *seedRefresher) requestUrgentRefresh() {
	if refresher == nil || !refresher.periodic || refresher.executionMode != DiscoveryExecutionThread || !refresher.tending.Get() || refresher.refreshing.Get() {
		return
	}

	// At most one honoured request per refresh interval.
	now := int64(time.Since(refresher.created))
	last := refresher.lastUrgent.Load()
	if last != 0 && now-last < int64(refresher.refreshInterval) {
		return
	}
	if !refresher.lastUrgent.CompareAndSwap(last, now) {
		return
	}

	select {
	case refresher.urgentCh <- struct{}{}:
	default:
	}
}

// close stops the refresher. Without a refresh goroutine it closes the
// provider. Otherwise it waits up to the refresh timeout for the goroutine,
// which closes the provider as it exits, after its last call has returned.
// Safe to call more than once.
func (refresher *seedRefresher) close() {
	refresher.closeOnce.Do(func() {
		close(refresher.stopCh)

		if !refresher.loopStarted.Get() {
			refresher.closeProvider()
			return
		}

		loopDone := make(chan struct{})
		go func() {
			refresher.loopWg.Wait()
			close(loopDone)
		}()
		select {
		case <-loopDone:
		case <-time.After(refresher.refreshTimeout):
			logger.Logger.Warn("A seed refresh by provider %s is still running after %s; the provider is closed when it returns", refresher.providerName, refresher.refreshTimeout)
		}
	})
}

// abort stops the refresher when the client fails to build, without waiting
// for a refresh in flight. A running goroutine closes the provider as it
// exits. A later close does nothing.
func (refresher *seedRefresher) abort() {
	if refresher == nil {
		return
	}
	refresher.closeOnce.Do(func() {
		close(refresher.stopCh)
		if !refresher.loopStarted.Get() {
			refresher.closeProvider()
		}
	})
}

func (refresher *seedRefresher) closeProvider() {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Logger.Warn("Seed provider %s close failed: %v", refresher.providerName, recovered)
		}
	}()
	refresher.provider.Close()
}

// refreshOnce requests one seed list from the provider. The list is copied.
// A panicking provider returns an error instead of crashing the client.
func (refresher *seedRefresher) refreshOnce(tendIteration int64) (discovered []*Host, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			discovered = nil
			err = fmt.Errorf("seed provider panicked: %v", recovered)
		}
	}()

	seedContext := &SeedCandidateContext{
		ClusterName:     refresher.clusterName,
		ConfiguredSeeds: refresher.staticSeeds,
		TendIteration:   tendIteration,
	}

	result, err := refresher.provider.RefreshSeedCandidates(seedContext)
	if err != nil {
		return nil, err
	}
	return copyHosts(result), nil
}

// merge combines the configured seeds with the discovered ones per the merge
// policy, keeping the first occurrence of each host and port in order.
func (refresher *seedRefresher) merge(discovered []*Host, initial bool) []*Host {
	switch refresher.mergePolicy {
	case SeedMergePolicyReplace:
		if !initial {
			return distinctHosts(discovered)
		}
		return distinctHosts(refresher.staticSeeds, discovered)
	case SeedMergePolicyDiscoveryOnly:
		if len(discovered) > 0 {
			return distinctHosts(discovered)
		}
		return distinctHosts(refresher.staticSeeds)
	default:
		return distinctHosts(refresher.staticSeeds, discovered)
	}
}

// distinctHosts keeps the first host for each name and port, in order. The
// TLS name takes no part in the comparison.
func distinctHosts(hostLists ...[]*Host) []*Host {
	total := 0
	for _, hosts := range hostLists {
		total += len(hosts)
	}

	seen := make(map[hostAndPort]struct{}, total)
	distinct := make([]*Host, 0, total)
	for _, hosts := range hostLists {
		for _, host := range hosts {
			if host == nil {
				continue
			}
			key := hostAndPort{host: host.Name, port: host.Port}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			hostCopy := *host
			distinct = append(distinct, &hostCopy)
		}
	}
	return distinct
}

// seedProviderDisplayName names a provider in logs and errors.
func seedProviderDisplayName(provider SeedCandidateProvider) string {
	switch provider.(type) {
	case staticSeedCandidateProvider:
		return seedProviderNameStatic
	case *dnsSeedCandidateProvider:
		return seedProviderNameDns
	case *awsEc2TagSeedCandidateProvider:
		return seedProviderNameAwsTags
	default:
		return fmt.Sprintf("%T", provider)
	}
}
