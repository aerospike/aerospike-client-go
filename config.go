// Copyright 2014-2022 Aerospike, Inc.
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
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	registry "github.com/aerospike/aerospike-client-go/v8/config/registry"
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

var supportedVersions = map[string]struct{}{
	"1.0.0": {},
}

type DynConfig struct {
	lock sync.RWMutex

	config *dynconfig.Config
	cancel context.CancelFunc

	configInitialized *atomic.Bool
	client            *Client // Reference to the client to use for callbacks and cached policies.
	configProvider    dynconfig.ConfigProvider

	metricsCallback func(config *dynconfig.Config, client *Client)

	scheme    string
	dsn       string
	logUpdate *atomic.Bool
}

func parseDsn(inputDsn string) map[string]string {
	// ^\s*                                    skip any leading spaces or tabs
	// ([A-Za-z][A-Za-z0-9+.-]*://)?           optionally capture scheme:// (RFC-style)
	// (.*)                                    capture the rest of the line verbatim
	re := regexp.MustCompile(registry.DSN_REGEX_PATTERN)
	match := re.FindStringSubmatch(inputDsn)
	if match == nil {
		return nil
	}

	result := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && name != "" {
			result[name] = match[i]
		}
	}
	return result
}

func newDynConfigWithCallBack(policy *ClientPolicy, fn func(config *dynconfig.Config, client *Client)) *DynConfig {
	// Dynamic configuration is not enabled if the config URL is empty.
	if strings.TrimSpace(AEROSPIKE_CLIENT_CONFIG_URL) == "" {
		return nil
	}
	if policy == nil {
		policy = NewClientPolicy()
	}

	parts := parseDsn(AEROSPIKE_CLIENT_CONFIG_URL)
	if len(parts) < 2 {
		logger.Logger.Warn("Invalid config URL %s. Expected format: [scheme://dsn] | [file path]", AEROSPIKE_CLIENT_CONFIG_URL)
		return nil
	}

	var schema string
	if parts[registry.DSN_SCHEME] == "" {
		schema = registry.DEFAULT_SCHEME
	} else {
		schema = parts[registry.DSN_SCHEME]
	}
	urlPath := parts[registry.DSN_PATH]

	// At this point in time we should have at least one configuration provider in the registry.
	provider, _ := registry.Get(schema)
	if provider == nil {
		logger.Logger.Warn("No configuration provider found for scheme %s.", schema)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	dynConfig := &DynConfig{
		configInitialized: &atomic.Bool{},
		metricsCallback:   fn,
		scheme:            schema,
		dsn:               urlPath,
		configProvider:    provider,
		cancel:            cancel,
	}
	dynConfig.logUpdate.Store(false)
	dynConfig.initConfig()

	go dynConfig.watchConfig(ctx, policy.ConfigInterval)

	return dynConfig
}

// Close closes the dynamic configuration watcher.
func (dc *DynConfig) Close() {
	if dc != nil && dc.cancel != nil {
		dc.cancel()
	}
}

// ----------------------------------------------------------------
// Functions used to manage the configuration state
// ----------------------------------------------------------------

func (dc *DynConfig) loadConfig() {
	dc.lock.Lock()
	defer dc.lock.Unlock()

	if !dc.configInitialized.Load() && dc.configProvider != nil {
		dc.initConfig()
	} else {
		dc.providerLoadConfig()
	}

	// Invoke the callback if it is set.
	dc.runCallBack()
}

func (dc *DynConfig) runCallBack() {
	// Atomically load config for safe access
	currentConfig := dc.config
	if dc.metricsCallback != nil && currentConfig != nil && currentConfig.Dynamic != nil && currentConfig.Dynamic.Metrics != nil {
		dc.metricsCallback(currentConfig, dc.client)
	}
}

// providerLoadConfig loads the config from the provider and hydrates
// the dynamic policies. It also clears the cache for dynamic configuration to ensure that the new
// config is used.
func (dc *DynConfig) providerLoadConfig() {
	loadedConfig := dc.configProvider.LoadConfig(dc.dsn)
	if loadedConfig != nil {
		// If the config is updated we need to log changes
		dc.logUpdate.Store(true)

		// Atomically load current config
		currentConfig := dc.config
		if currentConfig != nil && currentConfig.Dynamic == nil {
			logger.Logger.Warn("Dynamic configuration is enabled and configuration is empty. Configuration will load default policy values.")
		}

		// Create new config with updated Dynamic section
		if currentConfig != nil {
			newConfig := &dynconfig.Config{
				Static:  currentConfig.Static, // Keep existing static config
				Dynamic: loadedConfig.Dynamic, // Use new dynamic config
			}
			dc.config = newConfig // Atomically store new config
		} else {
			// If no current config, store the loaded config directly
			dc.config = loadedConfig
		}

		dc.hydrateDynamicPolicyFromConfig()
		// Once policies are hydrated we can turn logging off
		dc.logUpdate.Store(false)
		logger.Logger.Info("Dynamic configuration updated internal state from provider.")
	}
}

// initConfig is called only once on startup. It loads the config and
// hydrates the static and dynamic policies. It also clears the cache to ensure that the new config is used.
func (dc *DynConfig) initConfig() {
	loadedConfig := dc.configProvider.LoadConfig(dc.dsn)
	if loadedConfig != nil {
		dc.config = loadedConfig // Atomically store the config

		if dc.client != nil {
			dc.hydrateStaticPolicyFromConfig()
			dc.hydrateDynamicPolicyFromConfig()
		}

		dc.configInitialized.Store(true)
		dc.logUpdate.Store(false)
		logger.Logger.Info("Dynamic configuration initialized...")
	}
}

func (dc *DynConfig) updateCachedPolicies() {
	// This function is called to update the cached policies in the client.
	// It is used to ensure that the policies are updated when the config changes.

	if dc.client != nil {
		dc.hydrateStaticPolicyFromConfig()
		dc.hydrateDynamicPolicyFromConfig()
	} else {
		panic(fmt.Errorf("Client is not set in DynConfig, cannot update cached policies"))
	}
}

func (dc *DynConfig) hydrateStaticPolicyFromConfig() {
	(*dc.client.dynDefaultClientPolicy).Store(dc.generateStaticClientPolicy())
}

func (dc *DynConfig) hydrateDynamicPolicyFromConfig() {
	(*dc.client.dynDefaultClientPolicy).Store(dc.generateDynamicClientPolicy())
	dc.client.dynDefaultPolicy.Store(dc.generateDynamicReadPolicy())
	dc.client.dynDefaultWritePolicy.Store(dc.generateDynamicWritePolicy())
	dc.client.dynDefaultQueryPolicy.Store(dc.generateDynamicQueryPolicy())
	dc.client.dynDefaultScanPolicy.Store(dc.generateDynamicScanPolicy())
	dc.client.dynDefaultBatchPolicy.Store(dc.generateDynamicBatchPolicy())
	dc.client.dynDefaultBatchReadPolicy.Store(dc.generateDynamicBatchReadPolicy())
	dc.client.dynDefaultBatchWritePolicy.Store(dc.generateDynamicBatchWritePolicy())
	dc.client.dynDefaultBatchUDFPolicy.Store(dc.generateDynamicBatchUdfPolicy())
	dc.client.dynDefaultBatchDeletePolicy.Store(dc.generateDynamicBatchDeletePolicy())
	dc.client.dynDefaultTxnRollPolicy.Store(dc.generateDynamicTxnRollPolicy())
	dc.client.dynDefaultTxnVerifyPolicy.Store(dc.generateDynamicTxnVerifyPolicy())
	dc.client.dynDefaultMetricsPolicy.Store(dc.generateDynamicMetricsPolicy())
	dc.client.dynDefaultBatchReadBasePolicy.Store(dc.generateDynamicBatchReadBasePolicy())
	dc.client.dynDefaultBatchWriteBasePolicy.Store(dc.generateDynamicBatchWriteBasePolicy())
}

func (dc *DynConfig) generateStaticClientPolicy() *ClientPolicy {
	policy := NewClientPolicy()

	policy = policy.mapStatic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicClientPolicy() *ClientPolicy {
	// Loading current client policy since static fields are set at init time
	// We need to merge and preserve static and dynamic values.
	policy := (*dc.client.dynDefaultClientPolicy).Load()
	if policy == nil {
		policy = NewClientPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicWritePolicy() *WritePolicy {
	var policy *WritePolicy
	if dc.client != nil && dc.client.DefaultWritePolicy != nil {
		// Not going go make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultWritePolicy.copy()
	} else {
		policy = NewWritePolicy(0, 0)
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchReadBasePolicy() *BasePolicy {
	var policy *BasePolicy
	if dc.client != nil && dc.client.DefaultBatchReadPolicy != nil {
		// Not going go make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchPolicy.BasePolicy.copy()
	} else {
		policy = NewPolicy()
	}

	policy = policy.mapDynamicBatchRead(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchWriteBasePolicy() *BasePolicy {
	var policy *BasePolicy
	if dc.client != nil && dc.client.DefaultBatchWritePolicy != nil {
		// Not going go make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchPolicy.BasePolicy.copy()
	} else {
		policy = NewPolicy()
	}

	policy = policy.mapDynamicBatchWrite(dc)

	return policy
}

func (dc *DynConfig) generateDynamicReadPolicy() *BasePolicy {
	var policy *BasePolicy
	if dc.client != nil && dc.client.DefaultPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultPolicy.copy()
	} else {
		policy = NewPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicQueryPolicy() *QueryPolicy {
	var policy *QueryPolicy
	if dc.client != nil && dc.client.DefaultQueryPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultQueryPolicy.copy()
	} else {
		// If no default query policy is set, create a new one.
		policy = NewQueryPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicScanPolicy() *ScanPolicy {
	var policy *ScanPolicy
	if dc.client != nil && dc.client.DefaultScanPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultScanPolicy.copy()
	} else {
		// If no default scan policy is set, create a new one.
		policy = NewScanPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchWritePolicy() *BatchWritePolicy {
	var policy *BatchWritePolicy
	if dc.client != nil && dc.client.DefaultBatchWritePolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchWritePolicy.copy()
	} else {
		// If no default batch write policy is set, create a new one.
		policy = NewBatchWritePolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchReadPolicy() *BatchReadPolicy {
	var policy *BatchReadPolicy
	if dc.client != nil && dc.client.DefaultBatchReadPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchReadPolicy.copy()
	} else {
		// If no default batch read policy is set, create a new one.
		policy = NewBatchReadPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicTxnRollPolicy() *TxnRollPolicy {
	var policy *TxnRollPolicy
	if dc.client != nil && dc.client.DefaultTxnRollPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultTxnRollPolicy.copy()
	} else {
		// If no default txn roll policy is set, create a new one.
		policy = NewTxnRollPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicTxnVerifyPolicy() *TxnVerifyPolicy {
	var policy *TxnVerifyPolicy
	if dc.client != nil && dc.client.DefaultTxnVerifyPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultTxnVerifyPolicy.copy()
	} else {
		// If no default txn verify policy is set, create a new one.
		policy = NewTxnVerifyPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchDeletePolicy() *BatchDeletePolicy {
	var policy *BatchDeletePolicy
	if dc.client != nil && dc.client.DefaultBatchDeletePolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchDeletePolicy.copy()
	} else {
		// If no default batch delete policy is set, create a new one.
		policy = NewBatchDeletePolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchUdfPolicy() *BatchUDFPolicy {
	var policy *BatchUDFPolicy
	if dc.client != nil && dc.client.DefaultBatchUDFPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchUDFPolicy.copy()
	} else {
		// If no default batch udf policy is set, create a new one.
		policy = NewBatchUDFPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchPolicy() *BatchPolicy {
	var policy *BatchPolicy
	if dc.client != nil && dc.client.DefaultBatchPolicy != nil {
		// Not going to make changes to policy user has set but will create a copy of it
		// and apply dynamic configuration to it. The copy of the merged policy will be returned
		policy = dc.client.DefaultBatchPolicy.copy()
	} else {
		// If no default batch policy is set, create a new one.
		policy = NewBatchPolicy()
	}

	policy = policy.mapDynamic(dc)

	return policy
}

func (dc *DynConfig) generateDynamicMetricsPolicy() *MetricsPolicy {
	policy := DefaultMetricsPolicy()

	policy = policy.mapDynamic(dc)

	return policy
}

// ----------------------------------------------------------------
// Main watch goroutine for the config provider
// ----------------------------------------------------------------
func (dc *DynConfig) watchConfig(ctx context.Context, interval time.Duration) {
	logger.Logger.Info("Starting the config watch goroutine...")

	// If the config is not loaded, we will use the default interval.
	// If the config is loaded, we will use the interval from the config.
	// This allows the config to be updated dynamically without restarting the client.
	var mergedConfigInterval time.Duration
	// Handle the condition where dynamic config is enabled but config was not loaded because
	// the file could not be found or the url is not valid. In that case we will use the interval passed
	// in or use the default interval of 1 second.
	currentConfig := dc.config // Atomically load config
	if currentConfig == nil {
		mergedConfigInterval = interval
	} else {
		// If the config is already loaded, use the interval from the config.
		if currentConfig.Static != nil && currentConfig.Static.Client != nil && currentConfig.Static.Client.ConfigInterval != nil {
			mergedConfigInterval = time.Duration(*currentConfig.Static.Client.ConfigInterval) * time.Millisecond
		} else {
			mergedConfigInterval = interval
		}
	}

	defer func() {
		// TODO: Add exponential backoff here to resource starvation
		if r := recover(); r != nil {
			logger.Logger.Warn("Watch config goroutine crashed: %s", debug.Stack())
			go dc.watchConfig(ctx, mergedConfigInterval)
		}
	}()

	configInterval := max(mergedConfigInterval, 1*time.Second)
Loop:
	for {
		select {
		case <-ctx.Done():
			logger.Logger.Debug("Stopping config watch goroutine.")
			break Loop
		case <-time.After(configInterval):
			tm := time.Now()
			dc.loadConfig()
			if configDuration := time.Since(tm); configDuration > interval {
				logger.Logger.Warn("Watching took %s.", configDuration)
			}
		}
	}
}

// getConfigIfNotLoadedOrInitialized is used to get the config if it is not initialized yet.
func (dc *DynConfig) getConfigIfNotLoadedOrInitialized() *dynconfig.Config {
	config := dc.config

	if config == nil && !dc.configInitialized.Load() {
		// On initial load it is possible that the config is not yet loaded. This will kick things off to make sure
		// config is loaded.
		dc.loadConfig()
		config = dc.config
	}

	return config
}
