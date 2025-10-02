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
	"crypto/tls"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

// ClientPolicy encapsulates parameters for client policy command.
type ClientPolicy struct {
	// AuthMode specifies authentication mode used when user/password is defined. It is set to AuthModeInternal by default.
	AuthMode AuthMode

	// User authentication to cluster. Leave empty for clusters running without restricted access.
	User string

	// Password authentication to cluster. The password will be stored by the client and sent to server
	// in hashed format. Leave empty for clusters running without restricted access.
	Password string

	// ClusterName sets the expected cluster ID. If not nil, server nodes must return this cluster ID in order to
	// join the client's view of the cluster. Should only be set when connecting to servers that
	// support the "cluster-name" info command. (v3.10+)
	ClusterName string //=""

	// Initial host connection timeout duration. The timeout when opening a connection
	// to the server host for the first time.
	Timeout time.Duration //= 30 seconds

	// Connection idle timeout. Every time a connection is used, its idle
	// deadline will be extended by this duration. When this deadline is reached,
	// the connection will be closed and discarded from the connection pool.
	// The value is limited to 24 hours (86400s).
	//
	// It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
	// (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
	// that has already been reaped by the server.
	//
	// Connection pools are now implemented by a LIFO stack. Connections at the tail of the
	// stack will always be the least used. These connections are checked for IdleTimeout
	// on every tend (usually 1 second).
	//
	// Default: 0 seconds
	IdleTimeout time.Duration //= 0 seconds

	// LoginTimeout specifies the timeout for login operation for external authentication such as LDAP.
	LoginTimeout time.Duration //= 10 seconds

	// ConnectionQueueCache specifies the size of the Connection Queue cache PER NODE.
	// Note: One connection per node is reserved for tend operations and is not used for transactions.
	ConnectionQueueSize int //= 100

	// MinConnectionsPerNode specifies the minimum number of synchronous connections allowed per server node.
	// Preallocate min connections on client node creation.
	// The client will periodically allocate new connections if count falls below min connections.
	//
	// Server proto-fd-idle-ms may also need to be increased substantially if min connections are defined.
	// The proto-fd-idle-ms default directs the server to close connections that are idle for 60 seconds
	// which can defeat the purpose of keeping connections in reserve for a future burst of activity.
	//
	// If server proto-fd-idle-ms is changed, client ClientPolicy.IdleTimeout should also be
	// changed to be a few seconds less than proto-fd-idle-ms.
	//
	// Default: 0
	MinConnectionsPerNode int

	// MaxErrorRate defines the maximum number of errors allowed per node per ErrorRateWindow before
	// the circuit-breaker algorithm returns MAX_ERROR_RATE on database commands to that node.
	// If MaxErrorRate is zero, there is no error limit and
	// the exception will never be thrown.
	//
	// The counted error types are any error that causes the connection to close (socket errors
	// and client timeouts) and types.ResultCode.DEVICE_OVERLOAD.
	//
	// Default: 100
	MaxErrorRate int

	// ErrorRateWindow defined the number of cluster tend iterations that defines the window for MaxErrorRate.
	// One tend iteration is defined as TendInterval plus the time to tend all nodes.
	// At the end of the window, the error count is reset to zero and backoff state is removed
	// on all nodes.
	//
	// Default: 1
	ErrorRateWindow int //= 1

	// If set to true, will not create a new connection
	// to the node if there are already `ConnectionQueueSize` active connections.
	// Note: One connection per node is reserved for tend operations and is not used for transactions.
	LimitConnectionsToQueueSize bool //= true

	// Number of connections allowed to established at the same time.
	// This value does not limit the number of connections. It just
	// puts a threshold on the number of parallel opening connections.
	// By default, there are no limits.
	OpeningConnectionThreshold int // 0

	// Throw exception if host connection fails during addHost().
	FailIfNotConnected bool //= true

	// TendInterval determines interval for checking for cluster state changes.
	// Minimum possible interval is 10 Milliseconds.
	TendInterval time.Duration //= 1 second

	// A IP translation table is used in cases where different clients
	// use different server IP addresses. This may be necessary when
	// using clients from both inside and outside a local area
	// network. Default is no translation.
	// The key is the IP address returned from friend info requests to other servers.
	// The value is the real IP address used to connect to the server.
	IpMap map[string]string

	// UseServicesAlternate determines if the client should use "services-alternate" instead of "services"
	// in info request during cluster tending.
	//"services-alternate" returns server configured external IP addresses that client
	// uses to talk to nodes. "services-alternate" can be used in place of providing a client "ipMap".
	// This feature is recommended instead of using the client-side IpMap above.
	//
	// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
	//
	// Info command to use whether UserServicesAlternate is true or false:
	//
	// If false, use:
	// IP address: service-clear-std
	// TLS IP address: service-tls-std
	// Peers addresses: peers-clear-std
	// Peers TLS addresses: peers-tls-std
	// If true, use:
	// IP address: service-clear-alt
	// TLS IP address: service-tls-alt
	// Peers addresses: peers-clear-alt
	// Peers TLS addresses: peers-tls-alt
	UseServicesAlternate bool // false

	// RackAware directs the client to update rack information on intervals.
	// When this feature is enabled, the client will prefer to use nodes which reside
	// on the same rack as the client for read transactions. The application should also set the RackId, and
	// use the ReplicaPolicy.PREFER_RACK for reads.
	// This feature is in particular useful if the cluster is in the cloud and the cloud provider
	// is charging for network bandwidth out of the zone. Keep in mind that the node on the same rack
	// may not be the Master, and as such the data may be stale. This setting is particularly usable
	// for clusters that are read heavy.
	RackAware bool // false

	// RackIds defines the list of acceptable racks in order of preference. Nodes in RackIds[0] are chosen first.
	// If a node is not found in rackIds[0], then nodes in rackIds[1] are searched, and so on.
	// If rackIds is set, ClientPolicy.RackId is ignored.
	//
	// ClientPolicy.RackAware, ReplicaPolicy.PREFER_RACK and server rack
	// configuration must also be set to enable this functionality.
	RackIds []int // nil

	// TlsConfig specifies TLS secure connection policy for TLS enabled servers.
	// For better performance, we suggest preferring the server-side ciphers by
	// setting PreferServerCipherSuites = true.
	TlsConfig *tls.Config //= nil

	// IgnoreOtherSubnetAliases helps to ignore aliases that are outside main subnet
	IgnoreOtherSubnetAliases bool //= false

	// SeedOnlyCluster enforces the client to use only the seed addresses.
	// Peers nodes for the cluster are not discovered and seed nodes are
	// retained despite connection failures.
	SeedOnlyCluster bool // = false

	// Application id is used to identify application so that client operations can be correlated
	// with server side metrics.
	ApplicationId string

	// Determianes the interval for checking for configuration changes using configProvider.
	ConfigInterval time.Duration // = 5 second
}

// NewClientPolicy generates a new ClientPolicy with default values.
func NewClientPolicy() *ClientPolicy {
	return &ClientPolicy{
		AuthMode:                    AuthModeInternal,
		Timeout:                     30 * time.Second,
		IdleTimeout:                 0 * time.Second,
		LoginTimeout:                10 * time.Second,
		ConnectionQueueSize:         100,
		OpeningConnectionThreshold:  0,
		FailIfNotConnected:          true,
		TendInterval:                time.Second,
		LimitConnectionsToQueueSize: true,
		IgnoreOtherSubnetAliases:    false,
		MaxErrorRate:                100,
		ErrorRateWindow:             1,
		SeedOnlyCluster:             false,
		ConfigInterval:              time.Second * 5,
	}
}

// RequiresAuthentication returns true if a User or Password is set for ClientPolicy.
func (cp *ClientPolicy) RequiresAuthentication() bool {
	return (cp.User != "") || (cp.Password != "") || (cp.AuthMode == AuthModePKI)
}

func (cp *ClientPolicy) servicesString() string {
	if cp.UseServicesAlternate {
		return "services-alternate"
	}
	return "services"
}

func (cp *ClientPolicy) serviceString() string {
	if cp.TlsConfig == nil {
		if cp.UseServicesAlternate {
			return "service-clear-alt"
		}
		return "service-clear-std"
	}

	if cp.UseServicesAlternate {
		return "service-tls-alt"
	}
	return "service-tls-std"
}

func (cp *ClientPolicy) peersString() string {
	if cp.TlsConfig != nil {
		if cp.UseServicesAlternate {
			return "peers-tls-alt"
		}
		return "peers-tls-std"
	}

	if cp.UseServicesAlternate {
		return "peers-clear-alt"
	}
	return "peers-clear-std"
}

// copyClientPolicy creates a new BasePolicy instance and copies the values from the source policy.
func (cp *ClientPolicy) copy() *ClientPolicy {
	if cp == nil {
		return nil
	}

	response := *cp
	return &response
}

func (cp *ClientPolicy) mapDynamic(dynConfig *DynConfig) *ClientPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Dynamic == nil {
		return cp
	}

	if currentConfig.Dynamic.Client != nil {
		if currentConfig.Dynamic.Client.IdleTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Client.IdleTimeout) * time.Second
			cp.IdleTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("IdleTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Client.Timeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Client.Timeout) * time.Millisecond
			cp.Timeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("Timeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Client.ErrorRateWindow != nil {
			configValue := *currentConfig.Dynamic.Client.ErrorRateWindow
			cp.ErrorRateWindow = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ErrorRateWindow set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.Client.MaxErrorRate != nil {
			configValue := *currentConfig.Dynamic.Client.MaxErrorRate
			cp.MaxErrorRate = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("MaxErrorRate set to %d", configValue)
			}
		}
		if currentConfig.Dynamic.Client.LoginTimeout != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Client.LoginTimeout) * time.Millisecond
			cp.LoginTimeout = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("LoginTimeout set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Client.RackAware != nil {
			configValue := *currentConfig.Dynamic.Client.RackAware
			cp.RackAware = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("RackAware set to %t", configValue)
			}
		}
		if currentConfig.Dynamic.Client.RackIds != nil {
			configValue := *currentConfig.Dynamic.Client.RackIds
			cp.RackIds = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("RackIds set to %v", configValue)
			}
		}
		if currentConfig.Dynamic.Client.TendInterval != nil {
			configValue := time.Duration(*currentConfig.Dynamic.Client.TendInterval) * time.Millisecond
			cp.TendInterval = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("TendInterval set to %s", configValue.String())
			}
		}
		if currentConfig.Dynamic.Client.UseServiceAlternate != nil {
			configValue := *currentConfig.Dynamic.Client.UseServiceAlternate
			cp.UseServicesAlternate = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("UseServicesAlternate set to %t", configValue)
			}
		}
		if currentConfig.Dynamic.Client.ApplicationId != nil {
			configValue := *currentConfig.Dynamic.Client.ApplicationId
			cp.ApplicationId = configValue
			if dynConfig.logUpdate.Load() {
				logger.Logger.Info("ApplicationId set to %s", configValue)
			}
		}
	}

	return cp
}

func (cp *ClientPolicy) mapStatic(dynConfig *DynConfig) *ClientPolicy {
	// Atomically load config to avoid race conditions
	currentConfig := dynConfig.config
	if currentConfig == nil || currentConfig.Static == nil {
		return cp
	}

	if currentConfig.Static.Client != nil {
		if currentConfig.Static.Client.ConfigInterval != nil {
			cp.ConfigInterval = time.Duration(*currentConfig.Static.Client.ConfigInterval) * time.Second
		}
		if currentConfig.Static.Client.ConnectionQueueSize != nil {
			cp.ConnectionQueueSize = *currentConfig.Static.Client.ConnectionQueueSize
		}
		if currentConfig.Static.Client.MinConnectionsPerNode != nil {
			cp.MinConnectionsPerNode = *currentConfig.Static.Client.MinConnectionsPerNode
		}
	}

	return cp
}

// patchDynamic applies the dynamic configuration and generates a new policy.
func (policy *ClientPolicy) patchDynamic(dynConfig *DynConfig) *ClientPolicy {
	if dynConfig == nil {
		return policy
	}

	config := dynConfig.getConfigIfNotLoadedOrInitialized()

	if config != nil && ((config.Dynamic != nil && config.Dynamic.Client != nil) || (config.Static != nil && config.Static.Client != nil)) {
		// User has provided a custom policy. We need to apply the dynamic configuration.
		if policy != nil {
			return policy.copy().mapStatic(dynConfig).mapDynamic(dynConfig)
		} else {
			// Passed in policy is nil, fetch mapped default policy from cache.
			return (*dynConfig.client.dynDefaultClientPolicy).Load()
		}
	} else {
		return policy
	}
}

// maxErrorRate is the maximum number of errors allowed in a window
// errorRateWindow is the time window in which the errors are counted
// The value for maxErrorRate has to fall within the ratio of errorRateWindow:maxErrorRate, where the ratio is set to be 1:100.
// Returning calling policy to support chaining.
func (cp *ClientPolicy) ensureErrorRates() *ClientPolicy {
	// Returning a copy to avoid modifying the original policy and avoiding potential race conditions.
	returnPolicy := cp.copy()
	var errorRateWindow, maxErrorRate int
	errorRateWindow = max(cp.ErrorRateWindow, ERROR_RATE_MIN_VALUE)

	// MaxErrorRate set by user must be within the ratio of 1:100 of ErrorRateWindow to MaxErrorRate.
	if errorRateWindow*MAX_ERROR_RATE_MIN_VALUE >= cp.MaxErrorRate {
		maxErrorRate = cp.MaxErrorRate
	} else {
		logger.Logger.Warn(
			"Invalid circuit breaker configuration: MaxErrorRate: %d, ErrorRateWindow: %d, ratio: %.2f. The ratio (MaxErrorRate/ErrorRateWindow) must be between 1 and 100. Resetting to defaults - MaxErrorRate: %d and ErrorRateWindow: %d",
			cp.MaxErrorRate, errorRateWindow, float64(cp.MaxErrorRate)/float64(errorRateWindow), MAX_ERROR_RATE_MIN_VALUE, ERROR_RATE_MIN_VALUE)
		maxErrorRate = MAX_ERROR_RATE_MIN_VALUE
	}

	returnPolicy.ErrorRateWindow = errorRateWindow
	returnPolicy.MaxErrorRate = maxErrorRate

	return returnPolicy
}
