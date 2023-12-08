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

import "github.com/aerospike/aerospike-client-go/v7/types"

// ClientType determines the type of client to build.
type ClientType int

const (
	// CTNative means: Create a native client.
	CTNative ClientType = iota

	// CTProxy means: Create a proxy client.
	CTProxy
)

// CreateClientWithPolicyAndHost generates a new Client of the specified type
// with the specified ClientPolicy and sets up the cluster using the provided hosts.
// If the policy is nil, the default relevant policy will be used.
func CreateClientWithPolicyAndHost(typ ClientType, policy *ClientPolicy, hosts ...*Host) (ClientIfc, Error) {
	if len(hosts) == 0 {
		return nil, newError(types.SERVER_NOT_AVAILABLE, "No hosts were provided")
	}

	switch typ {
	case CTNative:
		return NewClientWithPolicyAndHost(policy, hosts...)
	case CTProxy:
		if len(hosts) > 1 {
			return nil, newError(types.GRPC_ERROR, "Only one proxy host is acceptable")
		}
		return NewProxyClientWithPolicyAndHost(policy, hosts[0])
	}
	return nil, newError(types.SERVER_NOT_AVAILABLE, "Invalid client type")
}
