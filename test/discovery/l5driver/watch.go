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

package main

import (
	"context"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	watchRoundEvery     = 10 * time.Millisecond
	watchConnectTimeout = 50 * time.Millisecond
	watchServingRounds  = 3
	watchUsableLimit    = 60 * time.Second
)

// watchProbe tracks the TCP rounds of one watch.
type watchProbe struct {
	order            []string
	firstConnectedAt map[string]int64
	connectedRounds  int
	servingAt        *int64
	lastClosedAt     *int64
}

func newWatchProbe() *watchProbe {
	return &watchProbe{firstConnectedAt: map[string]int64{}}
}

// recordRound takes one round started at roundStart over the resolved
// addresses and which of them connected. It returns true once the round
// completes the third consecutive round in which every address connected.
func (probe *watchProbe) recordRound(roundStart int64, addresses []string, connected []bool) bool {
	every := len(addresses) > 0
	for index, address := range addresses {
		if !slices.Contains(probe.order, address) {
			probe.order = append(probe.order, address)
		}
		if connected[index] {
			if _, already := probe.firstConnectedAt[address]; !already {
				probe.firstConnectedAt[address] = roundStart
			}
		} else {
			every = false
		}
	}

	if !every {
		closedAt := roundStart
		probe.lastClosedAt = &closedAt
		probe.connectedRounds = 0
		probe.servingAt = nil
		return false
	}
	if probe.connectedRounds == 0 {
		servingAt := roundStart
		probe.servingAt = &servingAt
	}
	probe.connectedRounds++
	return probe.connectedRounds >= watchServingRounds
}

func (probe *watchProbe) addresses() []line {
	addresses := make([]line, 0, len(probe.order))
	for _, address := range probe.order {
		object := line{"address": address, "firstConnectedAt": nil}
		if connectedAt, connected := probe.firstConnectedAt[address]; connected {
			object["firstConnectedAt"] = connectedAt
		}
		addresses = append(addresses, object)
	}
	return addresses
}

// probeRound resolves host and connects to every address at port at once.
func probeRound(host string, port int) ([]string, []bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	addresses, err := net.DefaultResolver.LookupHost(ctx, host)
	cancel()
	if err != nil {
		return nil, nil
	}

	connected := make([]bool, len(addresses))
	var round sync.WaitGroup
	for index, address := range addresses {
		round.Add(1)
		go func(index int, address string) {
			defer round.Done()
			conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort(address, strconv.Itoa(port)), watchConnectTimeout)
			if dialErr == nil {
				connected[index] = true
				conn.Close()
			}
		}(index, address)
	}
	round.Wait()
	return addresses, connected
}

// startWatch runs a watch on its own goroutine. It returns false when one is
// already running.
func (runner *commandRunner) startWatch(address, host string, port int, nodeName string) bool {
	runner.watchLock.Lock()
	defer runner.watchLock.Unlock()
	if runner.watchDone != nil {
		return false
	}
	done := make(chan struct{})
	runner.watchDone = done
	go func() {
		defer func() {
			runner.watchLock.Lock()
			runner.watchDone = nil
			runner.watchLock.Unlock()
			close(done)
		}()
		runner.watch(address, host, port, nodeName)
	}()
	return true
}

// waitForWatch lets a running watch reply, until deadline.
func (runner *commandRunner) waitForWatch(deadline time.Time) {
	runner.watchLock.Lock()
	done := runner.watchDone
	runner.watchLock.Unlock()
	if done != nil {
		waitUntil(done, deadline)
	}
}

// watch measures how soon the client uses node once address serves.
func (runner *commandRunner) watch(address, host string, port int, nodeName string) {
	probe := newWatchProbe()
	for {
		roundStart := runner.output.elapsed()
		addresses, connected := probeRound(host, port)
		if probe.recordRound(roundStart.Milliseconds(), addresses, connected) {
			break
		}
		runner.output.sleepUntil(roundStart + watchRoundEvery)
	}

	servingAt := *probe.servingAt
	var usableAt *int64
	for {
		checkStart := runner.output.elapsed()
		if runner.monitor.usable(nodeName) {
			at := checkStart.Milliseconds()
			usableAt = &at
			break
		}
		if checkStart.Milliseconds()-servingAt >= watchUsableLimit.Milliseconds() {
			break
		}
		runner.output.sleepUntil(checkStart + watchRoundEvery)
	}

	runner.output.write("watch", func(int64) line {
		reply := line{
			"address":          address,
			"node":             nodeName,
			"servingAt":        servingAt,
			"lastClosedAt":     nullable(probe.lastClosedAt),
			"addresses":        probe.addresses(),
			"usableAt":         nullable(usableAt),
			"usableMs":         nil,
			"ownershipChecked": true,
		}
		if usableAt != nil {
			reply["usableMs"] = *usableAt - servingAt
		}
		return reply
	})
}
