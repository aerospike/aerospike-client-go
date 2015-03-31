// Copyright 2013-2015 Aerospike, Inc.
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
	"time"
)

// ClientPolicy encapsulates parameters for client policy command.
type ClientPolicy struct {
	// User authentication to cluster. Leave empty for clusters running without restricted access.
	User string

	// Password authentication to cluster. The password will be stored by the client and sent to server
	// in hashed format. Leave empty for clusters running without restricted access.
	Password string

	// Initial host connection timeout in milliseconds.  The timeout when opening a connection
	// to the server host for the first time.
	Timeout time.Duration //= 1 second

	// Size of the Connection Queue cache.
	ConnectionQueueSize int //= 256

	// Throw exception if host connection fails during addHost().
	FailIfNotConnected bool //= true

	// TendInterval determines interval for checking for cluster state changes.
	// Minimum possible interval is 10 Miliseconds.
	TendInterval time.Duration //= 1 second
}

// NewClientPolicy generates a new ClientPolicy with default values.
func NewClientPolicy() *ClientPolicy {
	return &ClientPolicy{
		Timeout:             time.Second,
		ConnectionQueueSize: 256,
		FailIfNotConnected:  true,
		TendInterval:        time.Second,
	}
}

// RequiresAuthentication returns true if a USer or Password is set for ClientPolicy.
func (cp *ClientPolicy) RequiresAuthentication() bool {
	return (cp.User != "") || (cp.Password != "")
}
