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
	pc "github.com/aerospike/aerospike-client-go/v8/internal/cache"
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

type DynConfig struct {
	lock sync.RWMutex

	config   *dynconfig.Config
	wgConfig sync.WaitGroup

	configInitialized  *atomic.Bool
	clientPolicy       *ClientPolicy
	client             *Client
	configProvider     dynconfig.ConfigProvider
	configWatchChannel chan struct{}
	mappedPolicies     *pc.PolicyCache

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
		clientPolicy:       policy,
		configWatchChannel: make(chan struct{}),
		configInitialized:  &atomic.Bool{},
		metricsCallback:    fn,
		scheme:             parsedUrl.Scheme,
		dsn:                parsedUrl.Path,
		configProvider:     provider,
		mappedPolicies:     pc.NewPolicyCache(),
	}
	dynConfig.wgConfig.Add(1)
	go dynConfig.watchConfig()

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
		dc.mappedPolicies.PruneDynamic()
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
	if dc.mappedPolicies.Static == nil {
		dc.mappedPolicies.Static = make(map[pc.PolicyT]any, 0)
	}
	dc.mappedPolicies.Static[pc.CLIENT_POLICY] = dc.generateStaticClientPolicy()
}

func (dc *DynConfig) hydrateDynamicPolicyFromConfig() {
	if dc.mappedPolicies.Dynamic == nil {
		dc.mappedPolicies.Dynamic = make(map[pc.PolicyT]any, 0)
	}
	dc.mappedPolicies.Dynamic[pc.CLIENT_POLICY] = dc.generateDynamicClientPolicy()
	dc.mappedPolicies.Dynamic[pc.READ_POLICY] = dc.generateDynamicReadPolicy()
	dc.mappedPolicies.Dynamic[pc.WRITE_POLICY] = dc.generateDynamicWritePolicy()
	dc.mappedPolicies.Dynamic[pc.QUERY_POLICY] = dc.generateDynamicQueryPolicy()
	dc.mappedPolicies.Dynamic[pc.SCAN_POLICY] = dc.generateDynamicScanPolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_POLICY] = dc.generateDynamicBatchPolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_READ_POLICY] = dc.generateDynamicBatchReadPolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_WRITE_POLICY] = dc.generateDynamicBatchWritePolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_PARENT_WRITE_POLICY] = dc.generateDynamicBatchPolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_UDF_POLICY] = dc.generateDynamicBatchUdfPolicy()
	dc.mappedPolicies.Dynamic[pc.BATCH_DELETE_POLICY] = dc.generateDynamicBatchDeletePolicy()
	dc.mappedPolicies.Dynamic[pc.TXN_ROLL_POLICY] = dc.generateDynamicTxnRollPolicy()
	dc.mappedPolicies.Dynamic[pc.TXN_VERIFY_POLICY] = dc.generateDynamicTxnVerifyPolicy()
	dc.mappedPolicies.Dynamic[pc.METRICS_POLICY] = dc.generateDynamicMetricsPolicy()
}

func (dc *DynConfig) generateStaticClientPolicy() *ClientPolicy {
	policy := NewClientPolicy()

	if dc.config != nil && dc.config.Static != nil {
		if dc.config.Static.Client != nil {
			policy = mapStaticClientPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicClientPolicy() *ClientPolicy {
	policy := NewClientPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Client != nil {
			// Need to apply static and dynamic values
			policy = mapStaticClientPolicy(policy, dc)
			policy = mapDynamicClientPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicWritePolicy() *WritePolicy {
	policy := NewWritePolicy(0, 0)

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Write != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicWritePolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicReadPolicy() *BasePolicy {
	policy := NewPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Read != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicReadPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicQueryPolicy() *QueryPolicy {
	policy := NewQueryPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Query != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicQueryPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicScanPolicy() *ScanPolicy {
	policy := NewScanPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Scan != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicScanPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicBatchWritePolicy() *BatchWritePolicy {
	policy := NewBatchWritePolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.BatchWrite != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicBatchWritePolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicBatchReadPolicy() *BatchReadPolicy {
	policy := NewBatchReadPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.BatchRead != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicBatchReadPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicTxnRollPolicy() *TxnRollPolicy {
	policy := NewTxnRollPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.TxnRoll != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicTxnRollPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicTxnVerifyPolicy() *TxnVerifyPolicy {
	policy := NewTxnVerifyPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.TxnVerify != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicTxnVerifyPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicBatchDeletePolicy() *BatchDeletePolicy {
	policy := NewBatchDeletePolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.BatchDelete != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicBatchDeletePolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicBatchUdfPolicy() *BatchUDFPolicy {
	policy := NewBatchUDFPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.BatchUdf != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicBatchUdfPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicBatchPolicy() *BatchPolicy {
	policy := NewBatchPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.BatchRead != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicBatchPolicy(policy, dc)
		}
	}

	return policy
}

func (dc *DynConfig) generateDynamicMetricsPolicy() *MetricsPolicy {
	policy := DefaultMetricsPolicy()

	if dc.config != nil && dc.config.Dynamic != nil {
		if dc.config.Dynamic.Metrics != nil {
			// Need to apply static and dynamic values
			policy = mapDynamicMetricsPolicy(policy, dc)
		}
	}

	return policy
}

// ----------------------------------------------------------------
// Main watch goroutine for the config provider. This will
// ----------------------------------------------------------------
func (dc *DynConfig) watchConfig() {
	logger.Logger.Info("Starting the config watch goroutine...")

	defer func() {
		// TODO: Add exponential backoff here to resource starvation
		if r := recover(); r != nil {
			logger.Logger.Error("Watch config goroutine crashed: %s", debug.Stack())
			go dc.watchConfig()
		}
	}()

	defer dc.wgConfig.Done()

	configInterval := max(dc.clientPolicy.ConfigInterval, 10*time.Millisecond)
Loop:
	for {
		// If the config is not initialized, load it once. This is
		// important for the first time the config is loaded.
		if !dc.configInitialized.Load() {
			logger.Logger.Debug("Initializing configuration...")
			tm := time.Now()
			dc.loadConfig()
			if configDuration := time.Since(tm); configDuration > dc.clientPolicy.ConfigInterval {
				logger.Logger.Warn("Reload took %s, but your requested ConfigInterval is %s. "+
					"Reload is slower than the interval and may fall behind changes.",
					configDuration, dc.clientPolicy.ConfigInterval)
			}
		}

		select {
		case <-dc.configWatchChannel:
			logger.Logger.Debug("Watch config channel closed. Stopping watch goroutine.")
			break Loop
		case <-time.After(configInterval):
			tm := time.Now()
			dc.loadConfig()
			if configDuration := time.Since(tm); configDuration > dc.clientPolicy.ConfigInterval {
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

func NewDynConfigForTest(mapped *pc.PolicyCache, config *dynconfig.Config) *DynConfig {
	return &DynConfig{
		mappedPolicies: mapped,
		config:         config,
	}
}
