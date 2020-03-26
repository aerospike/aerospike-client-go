// Copyright 2013-2019 Aerospike, Inc.
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
	"net"
	"time"
)

const defaultIdleTimeout = 14 * time.Second

// ClientPolicy encapsulates parameters for client policy command.
type ClientPolicy struct {
	// AuthMode specifies authentication mode used when user/password is defined. It is set to AuthModeInternal by default.
	AuthMode AuthMode

	// User authentication to cluster. Leave empty for clusters running without restricted access.
	User string

	// Password authentication to cluster. The password will be stored by the client and sent to server
	// in hashed format. Leave empty for clusters running without restricted access.
	Password string

	// ClusterName sets the expected cluster ID.  If not null, server nodes must return this cluster ID in order to
	// join the client's view of the cluster. Should only be set when connecting to servers that
	// support the "cluster-name" info command. (v3.10+)
	ClusterName string //=""

	// Initial host connection timeout duration.  The timeout when opening a connection
	// to the server host for the first time.
	Timeout time.Duration //= 30 seconds

	// Connection idle timeout. Every time a connection is used, its idle
	// deadline will be extended by this duration. When this deadline is reached,
	// the connection will be closed and discarded from the connection pool.
	IdleTimeout time.Duration //= 14 seconds

	// LoginTimeout specifies the timeout for login operation for external authentication such as LDAP.
	LoginTimeout time.Duration //= 10 seconds

	// ConnectionQueueCache specifies the size of the Connection Queue cache PER NODE.
	// Note: One connection per node is reserved for tend operations and is not used for transactions.
	ConnectionQueueSize int //= 256

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
	// use different server IP addresses.  This may be necessary when
	// using clients from both inside and outside a local area
	// network. Default is no translation.
	// The key is the IP address returned from friend info requests to other servers.
	// The value is the real IP address used to connect to the server.
	IpMap map[string]string

	// UseServicesAlternate determines if the client should use "services-alternate" instead of "services"
	// in info request during cluster tending.
	//"services-alternate" returns server configured external IP addresses that client
	// uses to talk to nodes.  "services-alternate" can be used in place of providing a client "ipMap".
	// This feature is recommended instead of using the client-side IpMap above.
	//
	// "services-alternate" is available with Aerospike Server versions >= 3.7.1.
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

	// RackId defines the Rack the application is on. This will only influence reads if Rackaware is enabled on the client,
	// and configured on the server.
	RackId int // 0

	// TlsConfig specifies TLS secure connection policy for TLS enabled servers.
	// For better performance, we suggest preferring the server-side ciphers by
	// setting PreferServerCipherSuites = true.
	TlsConfig *tls.Config //= nil

	// IgnoreOtherSubnetAliases helps to ignore aliases that are outside main subnet
	IgnoreOtherSubnetAliases bool //= false

	// DialFunc specifies a custom dial function for creating tcp connections,
	// otherwise net.DialTimeout is used. it overrides Timeout.
	DialFunc func(net, addr string) (net.Conn, error)
}

// NewClientPolicy generates a new ClientPolicy with default values.
func NewClientPolicy() *ClientPolicy {
	return &ClientPolicy{
		AuthMode:                    AuthModeInternal,
		Timeout:                     30 * time.Second,
		IdleTimeout:                 defaultIdleTimeout,
		LoginTimeout:                10 * time.Second,
		ConnectionQueueSize:         256,
		OpeningConnectionThreshold:  0,
		FailIfNotConnected:          true,
		TendInterval:                time.Second,
		LimitConnectionsToQueueSize: true,
		IgnoreOtherSubnetAliases:    false,
	}
}

// RequiresAuthentication returns true if a User or Password is set for ClientPolicy.
func (cp *ClientPolicy) RequiresAuthentication() bool {
	return (cp.User != "") || (cp.Password != "")
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
