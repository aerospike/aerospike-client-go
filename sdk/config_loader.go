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
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"gopkg.in/yaml.v3"
)

// EnvConfigURL names the environment variable that points at the SDK
// configuration file. It accepts a bare path or a file:// URL.
const EnvConfigURL = "AEROSPIKE_SDK_CONFIG_URL"

// defaultProfileName is the fallback system profile.
const defaultProfileName = "DEFAULT"

// configFile is the on-disk schema. Keys are camelCase, a vocabulary shared
// across the Aerospike SDKs, and never leak into the Go API surface.
type configFile struct {
	System    map[string]systemProfile   `yaml:"system"`
	Behaviors map[string]behaviorProfile `yaml:"behaviors"`
}

type systemProfile struct {
	Connections struct {
		MinimumConnectionsPerNode *int    `yaml:"minimumConnectionsPerNode"`
		MaximumConnectionsPerNode *int    `yaml:"maximumConnectionsPerNode"`
		MaximumSocketIdleTime     *string `yaml:"maximumSocketIdleTime"`
	} `yaml:"connections"`
	CircuitBreaker struct {
		NumTendIntervalsInErrorWindow *int `yaml:"numTendIntervalsInErrorWindow"`
		MaximumErrorsInErrorWindow    *int `yaml:"maximumErrorsInErrorWindow"`
	} `yaml:"circuitBreaker"`
	Refresh struct {
		TendInterval *string `yaml:"tendInterval"`
	} `yaml:"refresh"`
	Transactions struct {
		ImplicitBatchWriteTransactions *bool   `yaml:"implicitBatchWriteTransactions"`
		NumberOfAttempts               *int    `yaml:"numberOfAttempts"`
		SleepBetweenAttempts           *string `yaml:"sleepBetweenAttempts"`
	} `yaml:"transactions"`
}

// behaviorProfile is one entry of the behaviors section. Each selector block
// maps to a [Scope].
type behaviorProfile struct {
	Parent string `yaml:"parent"`

	AllOperations         *settingsBlock `yaml:"allOperations"`
	RetryableWrites       *settingsBlock `yaml:"retryableWrites"`
	NonRetryableWrites    *settingsBlock `yaml:"nonRetryableWrites"`
	ConsistencyModeReads  *settingsBlock `yaml:"consistencyModeReads"`
	AvailabilityModeReads *settingsBlock `yaml:"availabilityModeReads"`
	BatchReads            *settingsBlock `yaml:"batchReads"`
	BatchWrites           *settingsBlock `yaml:"batchWrites"`
	Query                 *settingsBlock `yaml:"query"`
	SystemTxnVerify       *settingsBlock `yaml:"systemTxnVerify"`
	SystemTxnRoll         *settingsBlock `yaml:"systemTxnRoll"`
}

// scopeBlocks pairs each selector block with its scope.
func (b behaviorProfile) scopeBlocks() []struct {
	scope Scope
	block *settingsBlock
} {
	return []struct {
		scope Scope
		block *settingsBlock
	}{
		{ScopeAll, b.AllOperations},
		{ScopeWritesRetryable, b.RetryableWrites},
		{ScopeWritesNonRetryable, b.NonRetryableWrites},
		{ScopeReadsSC, b.ConsistencyModeReads},
		{ScopeReadsAP, b.AvailabilityModeReads},
		{ScopeReadsBatch, b.BatchReads},
		{ScopeWritesBatch, b.BatchWrites},
		{ScopeReadsQuery, b.Query},
		{ScopeSystemTxnVerify, b.SystemTxnVerify},
		{ScopeSystemTxnRoll, b.SystemTxnRoll},
	}
}

// settingsBlock is the on-disk vocabulary for a [Settings] patch.
type settingsBlock struct {
	AbandonCallAfter            *string `yaml:"abandonCallAfter"`
	WaitForCallToComplete       *string `yaml:"waitForCallToComplete"`
	DelayBetweenRetries         *string `yaml:"delayBetweenRetries"`
	MaximumNumberOfCallAttempts *int    `yaml:"maximumNumberOfCallAttempts"`
	ReplicaOrder                *string `yaml:"replicaOrder"`
	SendKey                     *bool   `yaml:"sendKey"`
	UseCompression              *bool   `yaml:"useCompression"`
	UseDurableDelete            *bool   `yaml:"useDurableDelete"`
	ResetTTLOnReadAtPercent     *int32  `yaml:"resetTtlOnReadAtPercent"`
	ReadConsistency             *string `yaml:"readConsistency"`
	Consistency                 *string `yaml:"consistency"`
	MigrationReadConsistency    *string `yaml:"migrationReadConsistency"`
	MaxConcurrentServers        *int    `yaml:"maxConcurrentServers"`
	AllowInlineMemoryAccess     *bool   `yaml:"allowInlineMemoryAccess"`
	AllowInlineSSDAccess        *bool   `yaml:"allowInlineSsdAccess"`
	RecordQueueSize             *int    `yaml:"recordQueueSize"`
	ErrorDetailVerbosity        *uint8  `yaml:"errorDetailVerbosity"`
}

// toSettings converts an on-disk block into a [Settings] patch. Parsing is
// fail-soft: a bad value is logged and skipped rather than failing the load.
func (b *settingsBlock) toSettings(where string) Settings {
	var s Settings
	if b == nil {
		return s
	}
	if d, ok := parseConfigDuration(b.AbandonCallAfter, where, "abandonCallAfter"); ok {
		s.TotalTimeout = &d
	}
	if d, ok := parseConfigDuration(b.WaitForCallToComplete, where, "waitForCallToComplete"); ok {
		s.SocketTimeout = &d
	}
	if d, ok := parseConfigDuration(b.DelayBetweenRetries, where, "delayBetweenRetries"); ok {
		s.RetryDelay = &d
	}
	if b.MaximumNumberOfCallAttempts != nil {
		// Attempts, not retries: one attempt means zero retries.
		if *b.MaximumNumberOfCallAttempts < 1 {
			logger.Logger.Warn("sdk config: %s: maximumNumberOfCallAttempts must be at least 1", where)
		} else {
			s.MaxRetries = IntPtr(*b.MaximumNumberOfCallAttempts - 1)
		}
	}
	if b.ReplicaOrder != nil {
		if r, ok := parseReplica(*b.ReplicaOrder); ok {
			s.Replica = &r
		} else {
			logger.Logger.Warn("sdk config: %s: unknown replicaOrder %q", where, *b.ReplicaOrder)
		}
	}
	if b.SendKey != nil {
		s.SendKey = b.SendKey
	}
	if b.UseCompression != nil {
		s.UseCompression = b.UseCompression
	}
	if b.UseDurableDelete != nil {
		s.DurableDelete = b.UseDurableDelete
	}
	if b.ResetTTLOnReadAtPercent != nil {
		s.ReadTouchTTLPercent = b.ResetTTLOnReadAtPercent
	}
	rc := b.ReadConsistency
	if rc == nil {
		rc = b.Consistency
	}
	if rc != nil {
		if m, ok := parseReadModeSC(*rc); ok {
			s.ReadModeSC = &m
		} else {
			logger.Logger.Warn("sdk config: %s: unknown readConsistency %q", where, *rc)
		}
	}
	if b.MigrationReadConsistency != nil {
		if m, ok := parseReadModeAP(*b.MigrationReadConsistency); ok {
			s.ReadModeAP = &m
		} else {
			logger.Logger.Warn("sdk config: %s: unknown migrationReadConsistency %q", where, *b.MigrationReadConsistency)
		}
	}
	if b.MaxConcurrentServers != nil {
		s.MaxConcurrentNodes = b.MaxConcurrentServers
	}
	if b.AllowInlineMemoryAccess != nil {
		s.AllowInline = b.AllowInlineMemoryAccess
	}
	if b.AllowInlineSSDAccess != nil {
		s.AllowInlineSSD = b.AllowInlineSSDAccess
	}
	if b.RecordQueueSize != nil {
		s.RecordQueueSize = b.RecordQueueSize
	}
	if b.ErrorDetailVerbosity != nil {
		s.ErrorDetailVerbosity = b.ErrorDetailVerbosity
	}
	return s
}

func parseReplica(v string) (as.ReplicaPolicy, bool) {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "MASTER":
		return as.MASTER, true
	case "MASTER_PROLES":
		return as.MASTER_PROLES, true
	case "SEQUENCE":
		return as.SEQUENCE, true
	case "PREFER_RACK":
		return as.PREFER_RACK, true
	case "RANDOM":
		return as.RANDOM, true
	}
	return as.SEQUENCE, false
}

func parseReadModeSC(v string) (as.ReadModeSC, bool) {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "SESSION":
		return as.ReadModeSCSession, true
	case "LINEARIZE":
		return as.ReadModeSCLinearize, true
	case "ALLOW_REPLICA":
		return as.ReadModeSCAllowReplica, true
	case "ALLOW_UNAVAILABLE":
		return as.ReadModeSCAllowUnavailable, true
	}
	return as.ReadModeSCSession, false
}

func parseReadModeAP(v string) (as.ReadModeAP, bool) {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "ONE":
		return as.ReadModeAPOne, true
	case "ALL":
		return as.ReadModeAPAll, true
	}
	return as.ReadModeAPOne, false
}

// durationPattern matches the configuration duration grammar. Multi-character
// units are listed first so "ms" is never eaten as "m".
var durationPattern = regexp.MustCompile(
	`^\s*(\d+(?:\.\d+)?)\s*(nanoseconds|nanosecond|nanos|nano|ns|` +
		`microseconds|microsecond|micros|micro|us|` +
		`milliseconds|millisecond|millis|milli|ms|` +
		`seconds|second|secs|sec|s|` +
		`minutes|minute|mins|min|m|` +
		`hours|hour|hrs|hr|h|` +
		`days|day|d)\s*$`)

// parseConfigDuration parses a configuration duration such as "250ms" or "1s".
func parseConfigDuration(v *string, where, field string) (time.Duration, bool) {
	if v == nil {
		return 0, false
	}
	m := durationPattern.FindStringSubmatch(*v)
	if m == nil {
		logger.Logger.Warn("sdk config: %s: cannot parse %s duration %q", where, field, *v)
		return 0, false
	}
	value, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		logger.Logger.Warn("sdk config: %s: cannot parse %s duration %q", where, field, *v)
		return 0, false
	}
	var unit time.Duration
	switch m[2] {
	case "nanoseconds", "nanosecond", "nanos", "nano", "ns":
		unit = time.Nanosecond
	case "microseconds", "microsecond", "micros", "micro", "us":
		unit = time.Microsecond
	case "milliseconds", "millisecond", "millis", "milli", "ms":
		unit = time.Millisecond
	case "seconds", "second", "secs", "sec", "s":
		unit = time.Second
	case "minutes", "minute", "mins", "min", "m":
		unit = time.Minute
	case "hours", "hour", "hrs", "hr", "h":
		unit = time.Hour
	case "days", "day", "d":
		unit = 24 * time.Hour
	}
	return time.Duration(value * float64(unit)), true
}

// loadedConfig is the outcome of one configuration load.
type loadedConfig struct {
	path      string
	raw       []byte
	system    map[string]systemProfile
	behaviors map[string]behaviorProfile
}

// configPathFromEnv resolves the configured path, accepting a bare path or a
// file:// URL. Any other scheme is logged and ignored.
func configPathFromEnv() string {
	v := strings.TrimSpace(os.Getenv(EnvConfigURL))
	if v == "" {
		return ""
	}
	if strings.HasPrefix(v, "file://") {
		return strings.TrimPrefix(v, "file://")
	}
	if m := regexp.MustCompile(`^([A-Za-z][A-Za-z0-9+.-]*)://`).FindStringSubmatch(v); m != nil {
		logger.Logger.Warn("sdk config: unsupported URL scheme %q in %s; ignoring", m[1], EnvConfigURL)
		return ""
	}
	return v
}

// readConfigFile reads and parses the configuration file. Parsing is
// fail-soft: an unreadable or unparsable file yields an empty result.
func readConfigFile(path string) loadedConfig {
	out := loadedConfig{path: path}
	if path == "" {
		return out
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		logger.Logger.Warn("sdk config: cannot read %q: %s", path, err)
		return out
	}
	out.raw = raw

	var cf configFile
	if err := yaml.Unmarshal(raw, &cf); err != nil {
		logger.Logger.Warn("sdk config: cannot parse %q: %s", path, err)
		return out
	}
	out.system = cf.System
	out.behaviors = cf.Behaviors
	return out
}

// resolveSystemSettings applies the documented precedence:
//
//	file cluster-name profile > file DEFAULT profile > programmatic > hard defaults
func resolveSystemSettings(loaded loadedConfig, clusterName string, programmatic SystemSettings) SystemSettings {
	var fileLayer SystemSettings
	if loaded.system != nil {
		if def, ok := loaded.system[defaultProfileName]; ok {
			fileLayer = mergeSystemSettings(systemProfileToSettings(def, defaultProfileName), fileLayer)
		}
		if clusterName != "" {
			if named, ok := loaded.system[clusterName]; ok {
				fileLayer = mergeSystemSettings(systemProfileToSettings(named, clusterName), fileLayer)
			}
		}
	}
	return mergeSystemSettings(fileLayer, programmatic)
}

// systemProfileToSettings converts one on-disk system profile.
func systemProfileToSettings(p systemProfile, where string) SystemSettings {
	var s SystemSettings
	s.MinConnectionsPerNode = p.Connections.MinimumConnectionsPerNode
	s.MaxConnectionsPerNode = p.Connections.MaximumConnectionsPerNode
	if d, ok := parseConfigDuration(p.Connections.MaximumSocketIdleTime, where, "maximumSocketIdleTime"); ok {
		s.MaxSocketIdleTime = &d
	}
	s.NumTendIntervalsInErrorWindow = p.CircuitBreaker.NumTendIntervalsInErrorWindow
	s.MaxErrorsInErrorWindow = p.CircuitBreaker.MaximumErrorsInErrorWindow
	if d, ok := parseConfigDuration(p.Refresh.TendInterval, where, "tendInterval"); ok {
		s.TendInterval = &d
	}
	s.Transactions.ImplicitBatchWriteTransactions = p.Transactions.ImplicitBatchWriteTransactions
	s.Transactions.NumberOfAttempts = p.Transactions.NumberOfAttempts
	if d, ok := parseConfigDuration(p.Transactions.SleepBetweenAttempts, where, "sleepBetweenAttempts"); ok {
		s.Transactions.SleepBetweenAttempts = &d
	}
	return s
}

var (
	appliedBehaviorsMu sync.Mutex
	appliedBehaviors   = map[string]behaviorProfile{}
)

// applyBehaviors registers or updates the behaviors declared in the file.
//
// A DEFAULT block layers onto the pristine factory patches rather than
// replacing them, and each reload re-layers from pristine, so a removed key
// reverts to the factory value.
func applyBehaviors(profiles map[string]behaviorProfile) {
	if len(profiles) == 0 {
		return
	}
	ensurePredefined()

	appliedBehaviorsMu.Lock()
	defer appliedBehaviorsMu.Unlock()

	for _, name := range topoOrder(profiles) {
		profile := profiles[name]
		if prev, ok := appliedBehaviors[name]; ok && equalBehaviorProfile(prev, profile) {
			continue
		}

		patches := map[Scope]Settings{}
		for _, sb := range profile.scopeBlocks() {
			if sb.block == nil {
				continue
			}
			patches[sb.scope] = sb.block.toSettings("behaviors." + name)
		}

		if name == BehaviorDefault {
			merged := map[Scope]Settings{}
			for s, v := range defaultFactoryPatches {
				merged[s] = v
			}
			for s, v := range patches {
				merged[s] = mergeSettings(merged[s], v)
			}
			DefaultBehavior().ReloadPatches(merged)
			appliedBehaviors[name] = profile
			continue
		}

		parent := DefaultBehavior()
		if profile.Parent != "" {
			if p, ok := GetBehavior(profile.Parent); ok {
				parent = p
			} else {
				logger.Logger.Warn("sdk config: behavior %q names unknown parent %q; using DEFAULT", name, profile.Parent)
			}
		}

		if existing, ok := GetBehavior(name); ok && existing.Parent() == parent {
			existing.ReloadPatches(patches)
		} else {
			NewBehavior(name, patches, parent)
		}
		appliedBehaviors[name] = profile
	}
}

// topoOrder orders behaviors so a parent declared in the same file is applied
// before its children. A cycle is reported and the file order is used.
func topoOrder(profiles map[string]behaviorProfile) []string {
	names := make([]string, 0, len(profiles))
	for n := range profiles {
		names = append(names, n)
	}
	// Deterministic starting order.
	sortStrings(names)

	var ordered []string
	placed := map[string]bool{}
	for len(ordered) < len(names) {
		progressed := false
		for _, n := range names {
			if placed[n] {
				continue
			}
			parent := profiles[n].Parent
			if parent != "" && !placed[parent] {
				if _, inFile := profiles[parent]; inFile {
					continue
				}
			}
			ordered = append(ordered, n)
			placed[n] = true
			progressed = true
		}
		if !progressed {
			logger.Logger.Warn("sdk config: behavior parent cycle detected; applying in file order")
			for _, n := range names {
				if !placed[n] {
					ordered = append(ordered, n)
					placed[n] = true
				}
			}
		}
	}
	return ordered
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// equalBehaviorProfile compares two profiles for the change gate.
func equalBehaviorProfile(a, b behaviorProfile) bool {
	return fmt.Sprintf("%+v", a) == fmt.Sprintf("%+v", b)
}

// loadConfigAtConnect runs the full pipeline at connect time.
func loadConfigAtConnect(clusterName string, programmatic SystemSettings) (loadedConfig, SystemSettings) {
	path := configPathFromEnv()
	loaded := readConfigFile(path)
	applyBehaviors(loaded.behaviors)
	settings := resolveSystemSettings(loaded, clusterName, programmatic)

	if path != "" {
		// A breadcrumb naming what was loaded; never the values.
		logger.Logger.Info("sdk config: loaded %q (%d system profiles, %d behaviors)",
			path, len(loaded.system), len(loaded.behaviors))
	}
	return loaded, settings
}
