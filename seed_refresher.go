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
	"time"

	iatomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const seedRefreshMaxOverruns = 3

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

	// startupSeeds is the list published at initialization; the overrun trip
	// restores exactly it.
	startupSeeds []*Host

	// Tend-mode state, touched only on the tend goroutine. tripped is atomic
	// so tests can read it.
	overrunCount int
	tripped      iatomic.Bool

	// Thread-mode loop lifecycle. refreshing is set while the loop is
	// inside a provider call, so an urgent request then does nothing.
	stopCh     chan struct{}
	urgentCh   chan struct{}
	refreshing iatomic.Bool
	loopWg     sync.WaitGroup
	closeOnce  sync.Once
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
	}

	if refresher.refreshInterval <= 0 {
		refresher.refreshInterval = 30 * time.Second
	}
	if refresher.refreshTimeout <= 0 {
		refresher.refreshTimeout = 10 * time.Second
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
	if !refresher.periodic || refresher.executionMode != DiscoveryExecutionTend {
		return nil
	}

	if !refresher.provider.SupportsDeadline() {
		return newError(types.PARAMETER_ERROR, "Seed provider "+refresher.providerName+" does not support a deadline and cannot run in tend execution mode. Use thread execution mode")
	}
	if refresher.refreshInterval < refresher.tendInterval {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Discovery refresh interval %s must be greater or equal to the tend interval %s", refresher.refreshInterval, refresher.tendInterval))
	}
	if refresher.tendDeadline > refresher.tendInterval/2 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid discovery tend refresh deadline %s. Must be greater than zero and at most half the tend interval %s", refresher.tendDeadline, refresher.tendInterval))
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

func (refresher *seedRefresher) startThreadMode() ([]*Host, Error) {
	firstResult := make(chan []*Host, 1)

	refresher.loopWg.Add(1)
	go refresher.refreshLoop(firstResult)

	select {
	case discovered := <-firstResult:
		return refresher.resolveStartupSeeds(discovered, nil)
	case <-time.After(refresher.refreshTimeout):
		logger.Logger.Warn("Seed provider %s did not finish the initial refresh within %s; starting from the configured seeds", refresher.providerName, refresher.refreshTimeout)
		return refresher.resolveStartupSeeds(nil, nil)
	}
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
// start; later results publish directly.
func (refresher *seedRefresher) refreshLoop(firstResult chan<- []*Host) {
	defer refresher.loopWg.Done()

	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Logger.Error("Seed refresh goroutine crashed and restarts: %v", recovered)
			refresher.loopWg.Add(1)
			go refresher.refreshLoop(nil)
		}
	}()

	for iteration := int64(0); ; iteration++ {
		refresher.refreshing.Set(true)
		discovered, err := refresher.refreshOnce(iteration)
		refresher.refreshing.Set(false)

		if firstResult != nil {
			// start merges, checks and publishes the first result itself.
			firstResult <- discovered
			firstResult = nil
		} else if err != nil {
			logger.Logger.Warn("Seed refresh by provider %s failed; keeping the current seeds: %s", refresher.providerName, err.Error())
		} else if merged := refresher.merge(discovered, false); len(merged) > 0 {
			refresher.publish(merged)
		}

		select {
		case <-refresher.stopCh:
			return
		case <-refresher.urgentCh:
		case <-time.After(refresher.refreshInterval):
		}
	}
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
	if merged := refresher.merge(discovered, false); len(merged) > 0 {
		refresher.publish(merged)
	}
}

// requestUrgentRefresh wakes the thread-mode loop so a client with no nodes
// does not wait out the refresh interval. It never blocks; the caller
// reconnects from the seeds already in hand.
func (refresher *seedRefresher) requestUrgentRefresh() {
	if refresher == nil || !refresher.periodic || refresher.executionMode != DiscoveryExecutionThread || refresher.refreshing.Get() {
		return
	}
	select {
	case refresher.urgentCh <- struct{}{}:
	default:
	}
}

// close stops the refresh goroutine, waits for an in-flight refresh up to
// the refresh timeout and closes the provider. Safe to call more than once.
func (refresher *seedRefresher) close() {
	refresher.closeOnce.Do(func() {
		close(refresher.stopCh)

		loopDone := make(chan struct{})
		go func() {
			refresher.loopWg.Wait()
			close(loopDone)
		}()
		select {
		case <-loopDone:
		case <-time.After(refresher.refreshTimeout):
			logger.Logger.Warn("A seed refresh by provider %s is still running after %s; closing the provider anyway", refresher.providerName, refresher.refreshTimeout)
		}

		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Logger.Warn("Seed provider %s close failed: %v", refresher.providerName, recovered)
			}
		}()
		refresher.provider.Close()
	})
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
	if _, isStatic := provider.(staticSeedCandidateProvider); isStatic {
		return seedProviderNameStatic
	}
	return fmt.Sprintf("%T", provider)
}
