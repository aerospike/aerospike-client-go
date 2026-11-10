//go:build unix && !aix && !app_engine

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

package aerospike

import (
	"net"
	"syscall"
)

// socketProbe peeks at a socket to find out if the peer closed it.
// It never blocks and never consumes data.
type socketProbe struct {
	rawConn    syscall.RawConn
	peek       func(fd uintptr)
	peekedByte [1]byte
	peekedLen  int
	peekErr    error
}

// newSocketProbe returns nil for connections that are not TCP sockets.
func newSocketProbe(conn net.Conn) *socketProbe {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil
	}

	// The probe must not reference the Connection, or its finalizer would never run.
	probe := &socketProbe{rawConn: rawConn}
	probe.peek = func(fd uintptr) {
		probe.peekedLen, _, probe.peekErr = syscall.Recvfrom(int(fd), probe.peekedByte[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	}
	return probe
}

// peerClosed returns true if the peer closed or reset the socket.
// Unread bytes also count as closed, unless pendingBytesAllowed is set.
func (probe *socketProbe) peerClosed(pendingBytesAllowed bool) bool {
	if probe == nil {
		return false
	}

	// Control is used because Read fails on the expired deadline of the last command.
	if err := probe.rawConn.Control(probe.peek); err != nil {
		return true
	}

	if probe.peekErr == nil {
		return probe.peekedLen == 0 || !pendingBytesAllowed
	}
	return probe.peekErr != syscall.EAGAIN && probe.peekErr != syscall.EWOULDBLOCK && probe.peekErr != syscall.EINTR
}
