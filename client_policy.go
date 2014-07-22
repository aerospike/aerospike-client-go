// Copyright 2013-2014 Aerospike, Inc.
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

// Container object for client policy Command.
type ClientPolicy struct {
	// Initial host connection timeout in milliseconds.  The timeout when opening a connection
	// to the server host for the first time.
	Timeout time.Duration //= 1000 milliseconds

	// Size of the Connection Queue cache.
	ConnectionQueueSize int //= 64

	// Throw exception if host connection fails during addHost().
	FailIfNotConnected bool //= true
}

// Generates a new ClientPolicy with default values
func NewClientPolicy() *ClientPolicy {
	return &ClientPolicy{
		Timeout:             1 * time.Second,
		ConnectionQueueSize: 64,
		FailIfNotConnected:  true,
	}
}
