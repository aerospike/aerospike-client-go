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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// SeedMergePolicy determines how seeds discovered by a SeedCandidateProvider
// combine with the seeds the client was constructed with.
type SeedMergePolicy int

const (
	// SeedMergePolicyMerge tries the configured seeds first, then the
	// discovered ones. This is the default.
	SeedMergePolicyMerge SeedMergePolicy = iota

	// SeedMergePolicyReplace uses only the discovered seeds after client
	// initialization. During initialization the configured seeds are still
	// tried first.
	SeedMergePolicyReplace

	// SeedMergePolicyDiscoveryOnly uses the configured seeds only when
	// discovery returns no candidates.
	SeedMergePolicyDiscoveryOnly
)

// String implements the Stringer interface.
func (policy SeedMergePolicy) String() string {
	switch policy {
	case SeedMergePolicyReplace:
		return "replace"
	case SeedMergePolicyDiscoveryOnly:
		return "discovery_only"
	default:
		return "merge"
	}
}

// TranslationMissMode determines what happens when the address translator
// has no mapping for a server-advertised endpoint.
type TranslationMissMode int

const (
	// TranslationMissPassThrough connects to the advertised address
	// unchanged. This is the default, and matches how IpMap treats a host
	// with no entry.
	TranslationMissPassThrough TranslationMissMode = iota

	// TranslationMissStrict does not use the advertised address. The
	// affected node is not added to the cluster and the miss is reported;
	// the address is retried on the next cluster tend.
	TranslationMissStrict
)

// String implements the Stringer interface.
func (mode TranslationMissMode) String() string {
	if mode == TranslationMissStrict {
		return "strict"
	}
	return "pass_through"
}

// DiscoveryExecutionMode determines where periodic seed discovery runs.
type DiscoveryExecutionMode int

const (
	// DiscoveryExecutionThread runs periodic seed refreshes on a dedicated
	// goroutine the client owns. The goroutine exists only while a provider
	// that needs periodic refresh is configured. This is the default.
	DiscoveryExecutionThread DiscoveryExecutionMode = iota

	// DiscoveryExecutionTend runs periodic seed refreshes on the cluster
	// tend goroutine, bounded by ClientPolicy.DiscoveryTendRefreshDeadline.
	// Only providers that declare SupportsDeadline may be used in this mode.
	DiscoveryExecutionTend
)

// String implements the Stringer interface.
func (mode DiscoveryExecutionMode) String() string {
	if mode == DiscoveryExecutionTend {
		return "tend"
	}
	return "thread"
}

// Configuration tokens for the discovery and translation settings. These
// spellings are shared across all Aerospike clients and their configuration
// files.
const (
	seedProviderNameStatic  = "static"
	seedProviderNameDns     = "dns"
	seedProviderNameAwsTags = "aws-tags"

	translatorNameStaticMap   = "static-map"
	translatorNamePortMap     = "port-map"
	translatorNamePassThrough = "pass-through"
)

func isStandardSeedProviderName(name string) bool {
	switch name {
	case seedProviderNameStatic, seedProviderNameDns, seedProviderNameAwsTags:
		return true
	}
	return false
}

func isStandardTranslatorName(name string) bool {
	switch name {
	case translatorNameStaticMap, translatorNamePortMap, translatorNamePassThrough:
		return true
	}
	return false
}

func parseSeedMergePolicy(token string) (SeedMergePolicy, Error) {
	switch token {
	case "merge":
		return SeedMergePolicyMerge, nil
	case "replace":
		return SeedMergePolicyReplace, nil
	case "discovery_only":
		return SeedMergePolicyDiscoveryOnly, nil
	}
	return SeedMergePolicyMerge, newError(types.PARAMETER_ERROR, "Invalid merge policy `"+token+"`. Valid values are merge, replace and discovery_only")
}

func parseTranslationMissMode(token string) (TranslationMissMode, Error) {
	switch token {
	case "pass_through":
		return TranslationMissPassThrough, nil
	case "strict":
		return TranslationMissStrict, nil
	}
	return TranslationMissPassThrough, newError(types.PARAMETER_ERROR, "Invalid translation miss mode `"+token+"`. Valid values are pass_through and strict")
}

func parseDiscoveryExecutionMode(token string) (DiscoveryExecutionMode, Error) {
	switch token {
	case "thread":
		return DiscoveryExecutionThread, nil
	case "tend":
		return DiscoveryExecutionTend, nil
	}
	return DiscoveryExecutionThread, newError(types.PARAMETER_ERROR, "Invalid discovery execution mode `"+token+"`. Valid values are thread and tend")
}
