//go:build go1.27

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

package sdk

import (
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Names of the predefined behaviors.
const (
	// BehaviorDefault is the root behavior every other one derives from.
	BehaviorDefault = "DEFAULT"
	// BehaviorReadFast trades retries and timeouts for latency on reads.
	BehaviorReadFast = "READ_FAST"
	// BehaviorStrictlyConsistent linearizes strong-consistency reads.
	BehaviorStrictlyConsistent = "STRICTLY_CONSISTENT"
	// BehaviorFastRackAware prefers the local rack for fast reads.
	BehaviorFastRackAware = "FAST_RACK_AWARE"
)

var (
	registryMu sync.Mutex
	registry   = map[string]*Behavior{}

	predefinedOnce sync.Once

	defaultBehavior            *Behavior
	readFastBehavior           *Behavior
	strictlyConsistentBehavior *Behavior
	fastRackAwareBehavior      *Behavior

	// defaultFactoryPatches is DEFAULT's pristine patch set, captured before
	// any configuration file is applied, so a file DEFAULT block layers onto
	// the factory values instead of replacing them and each reload re-layers
	// from pristine.
	defaultFactoryPatches map[Scope]Settings
)

// registerBehavior adds a behavior to the process-global registry. A name
// collision replaces the mapping; existing holders keep their pointer.
func registerBehavior(b *Behavior) {
	registryMu.Lock()
	registry[b.name] = b
	registryMu.Unlock()
}

// GetBehavior looks a behavior up by name.
func GetBehavior(name string) (*Behavior, bool) {
	ensurePredefined()
	registryMu.Lock()
	defer registryMu.Unlock()
	b, ok := registry[name]
	return b, ok
}

// GetBehaviorOrDefault looks a behavior up by name, falling back to DEFAULT.
func GetBehaviorOrDefault(name string) *Behavior {
	if b, ok := GetBehavior(name); ok {
		return b
	}
	return DefaultBehavior()
}

// AllBehaviors reports every registered behavior, keyed by name.
func AllBehaviors() map[string]*Behavior {
	ensurePredefined()
	registryMu.Lock()
	defer registryMu.Unlock()
	out := make(map[string]*Behavior, len(registry))
	for k, v := range registry {
		out[k] = v
	}
	return out
}

// DefaultBehavior returns the root behavior.
func DefaultBehavior() *Behavior { ensurePredefined(); return defaultBehavior }

// ReadFastBehavior returns the low-latency read behavior.
func ReadFastBehavior() *Behavior { ensurePredefined(); return readFastBehavior }

// StrictlyConsistentBehavior returns the linearized-read behavior.
func StrictlyConsistentBehavior() *Behavior { ensurePredefined(); return strictlyConsistentBehavior }

// FastRackAwareBehavior returns the rack-preferring fast read behavior.
func FastRackAwareBehavior() *Behavior { ensurePredefined(); return fastRackAwareBehavior }

// ensurePredefined builds the predefined behaviors exactly once.
func ensurePredefined() {
	predefinedOnce.Do(func() {
		defaultBehavior = NewBehavior(BehaviorDefault, factoryDefaultPatches(), nil)
		defaultFactoryPatches = defaultBehavior.patchesSnapshot()

		readFastBehavior = NewBehavior(BehaviorReadFast, map[Scope]Settings{
			ScopeReads: {
				TotalTimeout:  DurationPtr(200 * time.Millisecond),
				SocketTimeout: DurationPtr(50 * time.Millisecond),
				MaxRetries:    IntPtr(3),
			},
		}, defaultBehavior)

		strictlyConsistentBehavior = NewBehavior(BehaviorStrictlyConsistent, map[Scope]Settings{
			ScopeReadsSC: {ReadModeSC: ReadModeSCPtr(as.ReadModeSCLinearize)},
		}, defaultBehavior)

		fastRackAwareBehavior = NewBehavior(BehaviorFastRackAware, map[Scope]Settings{
			ScopeReads:   {Replica: ReplicaPtr(as.PREFER_RACK)},
			ScopeReadsSC: {ReadModeSC: ReadModeSCPtr(as.ReadModeSCSession)},
		}, readFastBehavior)
	})
}

// factoryDefaultPatches is the factory patch set for DEFAULT.
func factoryDefaultPatches() map[Scope]Settings {
	return map[Scope]Settings{
		ScopeAll: {
			TotalTimeout:        DurationPtr(30 * time.Second),
			SocketTimeout:       DurationPtr(5 * time.Second),
			MaxRetries:          IntPtr(2),
			RetryDelay:          DurationPtr(0),
			SendKey:             BoolPtr(true),
			Replica:             ReplicaPtr(as.SEQUENCE),
			DurableDelete:       BoolPtr(false),
			MaxConcurrentNodes:  IntPtr(1),
			ReadTouchTTLPercent: Int32Ptr(0),
		},
		ScopeReadsBatch: {
			AllowInline:    BoolPtr(true),
			AllowInlineSSD: BoolPtr(false),
		},
		ScopeReadsQuery: {
			MaxRetries:         IntPtr(5),
			RecordQueueSize:    IntPtr(1024),
			MaxConcurrentNodes: IntPtr(0),
		},
		ScopeWrites: {
			SimulateXDRWrite: BoolPtr(false),
		},
		ScopeWritesNonRetryable: {
			MaxRetries: IntPtr(0),
		},
		ScopeWritesAP: {
			CommitLevel: CommitLevelPtr(as.COMMIT_ALL),
		},
		ScopeWritesSC: {
			DurableDelete: BoolPtr(true),
		},
		ScopeWritesBatch: {
			AllowInline:    BoolPtr(true),
			AllowInlineSSD: BoolPtr(false),
		},
		// The two system scopes carry the transaction defaults.
		ScopeSystemTxnVerify: {
			ReadModeSC:     ReadModeSCPtr(as.ReadModeSCLinearize),
			Replica:        ReplicaPtr(as.MASTER),
			MaxRetries:     IntPtr(5),
			SocketTimeout:  DurationPtr(3 * time.Second),
			TotalTimeout:   DurationPtr(10 * time.Second),
			RetryDelay:     DurationPtr(time.Second),
			AllowInline:    BoolPtr(false),
			AllowInlineSSD: BoolPtr(true),
			SendKey:        BoolPtr(false),
		},
		ScopeSystemTxnRoll: {
			Replica:        ReplicaPtr(as.MASTER),
			MaxRetries:     IntPtr(5),
			SocketTimeout:  DurationPtr(3 * time.Second),
			TotalTimeout:   DurationPtr(10 * time.Second),
			RetryDelay:     DurationPtr(time.Second),
			AllowInline:    BoolPtr(false),
			AllowInlineSSD: BoolPtr(true),
			SendKey:        BoolPtr(false),
		},
	}
}
