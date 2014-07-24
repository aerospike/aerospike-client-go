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
	"net"
	"time"

	. "github.com/citrusleaf/aerospike-client-go/logger"
)

// Connection represents a connection with a timeout
type Connection struct {
	// timeout
	timeout time.Duration

	// connection object
	conn net.Conn
}

// NewConnection creates a connection on the network and returns the pointer
// A minimum timeout of 2 seconds will always be applied.
// If the connection is not established in the specified timeout,
// an error will be returned
func NewConnection(address string, timeout time.Duration) (*Connection, error) {
	newConn := &Connection{}
	newConn.SetTimeout(timeout)

	if conn, err := net.DialTimeout("tcp", address, newConn.timeout); err != nil {
		Logger.Error("Connection to address `%s` failed to establish: %s", address, err.Error())
		return nil, err
	} else {
		newConn.conn = conn
		return newConn, nil
	}
}

// Writes the slice to the connection buffer.
func (this *Connection) Write(buf []byte) (int, error) {
	if this.timeout > 0 {
		this.conn.SetWriteDeadline(time.Now().Add(this.timeout))
	}
	return this.conn.Write(buf)
}

// Reads from connection buffer to the slice
func (this *Connection) Read(buf []byte, length int) (int, error) {
	if this.timeout > 0 {
		this.conn.SetReadDeadline(time.Now().Add(this.timeout))
	}
	r, err := this.conn.Read(buf[:length])
	return r, err
}

// Returns true if the connection is not closed
func (this *Connection) IsConnected() bool {
	return this.conn != nil
}

// sets connection timeout
func (this *Connection) SetTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	this.timeout = timeout
}

// Closes the connection
func (this *Connection) Close() {
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
}
