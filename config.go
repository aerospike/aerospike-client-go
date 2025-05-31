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
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	registry "github.com/aerospike/aerospike-client-go/v8/config/registry"
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

type DynConfig struct {
	lock sync.RWMutex

	config   *dynconfig.Config
	wgConfig sync.WaitGroup

	configInitialized  *atomic.Bool
	client             *Client // Reference to the client to use for callbacks and cached policies.
	configProvider     dynconfig.ConfigProvider
	configWatchChannel chan struct{}

	metricsCallback func(config *dynconfig.Config, client *Client)

	scheme string
	dsn    string
}

// TODO: not used consider removing
func newDynConfig(policy *ClientPolicy) *DynConfig {
	dynConfig := newDynConfigWithCallBack(policy, nil)

	return dynConfig
}

func newDynConfigWithCallBack(policy *ClientPolicy, fn func(config *dynconfig.Config, client *Client)) *DynConfig {
	// Dynamic configuration is not enabled if the config URL is empty.
	if strings.TrimSpace(AEROSPIKE_CLIENT_CONFIG_URL) == "" {
		return nil
	}
	if policy == nil {
		policy = NewClientPolicy()
	}

	parsedUrl, err := url.Parse(AEROSPIKE_CLIENT_CONFIG_URL)
	if err != nil {
		logger.Logger.Error("Failed to parse config URL %s. Error: %v", AEROSPIKE_CLIENT_CONFIG_URL, err)
	}

	// At this point in time we should have at least one configuration provider in the registry.
	provider, _ := registry.Get(parsedUrl.Scheme)

	dynConfig := &DynConfig{
		configWatchChannel: make(chan struct{}),
		configInitialized:  &atomic.Bool{},
		metricsCallback:    fn,
		scheme:             parsedUrl.Scheme,
		dsn:                parsedUrl.Path,
		configProvider:     provider,
	}
	dynConfig.wgConfig.Add(1)
	go dynConfig.watchConfig(policy.ConfigInterval)

	return dynConfig
}

// ----------------------------------------------------------------
// Functions used to manage the configuration state
// ----------------------------------------------------------------

func (dc *DynConfig) loadConfig() {
	dc.lock.Lock()
	defer dc.lock.Unlock()
	registry.ConfigProvidersMu.Lock()
	defer registry.ConfigProvidersMu.Unlock()

	if !dc.configInitialized.Load() && dc.configProvider != nil {
		logger.Logger.Debug("Initializing configuration...")
		dc.initConfig()
		dc.configInitialized.Store(true)
	} else {
		dc.providerLoadConfig()
	}

	// Invoke the callback if it is set.
	dc.runCallBack()
}

func (dc *DynConfig) runCallBack() {
	if dc.metricsCallback != nil && dc.config != nil && dc.config.Dynamic != nil && dc.config.Dynamic.Metrics != nil {
		dc.metricsCallback(dc.config, dc.client)
	}
}

// providerLoadConfig loads the config from the provider and hydrates
// the dynamic policies. It also clears the cache for dynamic configuration to ensure that the new
// config is used.
func (dc *DynConfig) providerLoadConfig() {
	loadedConfig := dc.configProvider.LoadConfig(dc.dsn)
	if loadedConfig != nil {
		dc.config.Dynamic = loadedConfig.Dynamic // This is updating the entire dynamic config object

		if dc.config.Dynamic == nil {
			logger.Logger.Warn("Dynamic configuration is enabled and configuration is empty. Configuration will load default policy values.")
		}

		dc.hydrateDynamicPolicyFromConfig()
		logger.Logger.Debug("Dynamic configuration updated internal state from provider.")
	}
}

// initConfig is called only once on startup. It loads the config and
// hydrates the static and dynamic policies. It also clears the cache to ensure that the new config is used.
func (dc *DynConfig) initConfig() {
	loadedConfig := dc.configProvider.LoadConfig(dc.dsn)
	if loadedConfig != nil {
		dc.config = loadedConfig // This is updating the entire config object
	}

	dc.hydrateStaticPolicyFromConfig()
	dc.hydrateDynamicPolicyFromConfig()
}

func (dc *DynConfig) hydrateStaticPolicyFromConfig() {
	dc.client.dynDefaultClientPolicy.Store(dc.generateStaticClientPolicy())
}

func (dc *DynConfig) hydrateDynamicPolicyFromConfig() {
	dc.client.dynDefaultClientPolicy.Store(dc.generateDynamicClientPolicy())
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
}

func (dc *DynConfig) generateStaticClientPolicy() *ClientPolicy {
	policy := NewClientPolicy()

	policy = mapStaticClientPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicClientPolicy() *ClientPolicy {
	// Loading current client policy since static fields are set at init time
	// We need to merge and preserve static and dynamic values.
	policy := dc.client.dynDefaultClientPolicy.Load()
	if policy == nil {
		policy = NewClientPolicy()
	}

	policy = mapDynamicClientPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicWritePolicy() *WritePolicy {
	policy := NewWritePolicy(0, 0)

	policy = mapDynamicWritePolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicReadPolicy() *BasePolicy {
	policy := NewPolicy()

	policy = mapDynamicReadPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicQueryPolicy() *QueryPolicy {
	policy := NewQueryPolicy()

	policy = mapDynamicQueryPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicScanPolicy() *ScanPolicy {
	policy := NewScanPolicy()

	policy = mapDynamicScanPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchWritePolicy() *BatchWritePolicy {
	policy := NewBatchWritePolicy()

	policy = mapDynamicBatchWritePolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchReadPolicy() *BatchReadPolicy {
	policy := NewBatchReadPolicy()

	policy = mapDynamicBatchReadPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicTxnRollPolicy() *TxnRollPolicy {
	policy := NewTxnRollPolicy()

	policy = mapDynamicTxnRollPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicTxnVerifyPolicy() *TxnVerifyPolicy {
	policy := NewTxnVerifyPolicy()

	policy = mapDynamicTxnVerifyPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchDeletePolicy() *BatchDeletePolicy {
	policy := NewBatchDeletePolicy()

	policy = mapDynamicBatchDeletePolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchUdfPolicy() *BatchUDFPolicy {
	policy := NewBatchUDFPolicy()

	policy = mapDynamicBatchUdfPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicBatchPolicy() *BatchPolicy {
	policy := NewBatchPolicy()

	policy = mapDynamicBatchPolicy(policy, dc)

	return policy
}

func (dc *DynConfig) generateDynamicMetricsPolicy() *MetricsPolicy {
	policy := DefaultMetricsPolicy()

	policy = mapDynamicMetricsPolicy(policy, dc)

	return policy
}

// ----------------------------------------------------------------
// Main watch goroutine for the config provider
// ----------------------------------------------------------------
func (dc *DynConfig) watchConfig(interval time.Duration) {
	logger.Logger.Info("Starting the config watch goroutine...")

	defer func() {
		// TODO: Add exponential backoff here to resource starvation
		if r := recover(); r != nil {
			logger.Logger.Error("Watch config goroutine crashed: %s", debug.Stack())
			go dc.watchConfig(interval)
		}
	}()

	defer dc.wgConfig.Done()

	configInterval := max(interval, 10*time.Millisecond)
Loop:
	for {
		// If the config is not initialized, load it once. This is
		// important for the first time the config is loaded.
		if !dc.configInitialized.Load() {
			logger.Logger.Debug("Initializing configuration...")
			tm := time.Now()
			dc.loadConfig()
			if configDuration := time.Since(tm); configDuration > interval {
				logger.Logger.Warn("Reload took %s, but your requested ConfigInterval is %s. "+
					"Reload is slower than the interval and may fall behind changes.",
					configDuration, interval)
			}
		}

		select {
		case <-dc.configWatchChannel:
			logger.Logger.Debug("Watch config channel closed. Stopping watch goroutine.")
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

// getConfigIfNotInitialized is used to get the config if it is not initialized yet.
func (dc *DynConfig) getConfigIfNotInitialized() *dynconfig.Config {
	config := dc.config

	if config == nil && !dc.configInitialized.Load() {
		// On initial load it is possible that the config is not yet loaded. This will kick things off to make sure
		// config is loaded.
		dc.loadConfig()
		config = dc.config
	}

	return config
}

// ----------------------------------------------------------------
// Testing functions
// ----------------------------------------------------------------

func NewDynConfigForTest(config *dynconfig.Config) *DynConfig {
	return &DynConfig{
		config: config,
	}
}
