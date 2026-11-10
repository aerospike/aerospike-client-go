//go:build (linux || darwin || windows) && !app_engine

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
	peekSocket func(fd uintptr)
	peekedByte [1]byte
	outcome    peekOutcome
}

// newSocketProbe returns nil for connections that cannot be probed.
func newSocketProbe(conn net.Conn) *socketProbe {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil
	}

	var prepareErr error
	err = rawConn.Control(func(fd uintptr) {
		prepareErr = prepareSocketForPeek(fd)
	})
	if err != nil || prepareErr != nil {
		return nil
	}

	// The probe must not reference the Connection, or its finalizer would never run.
	probe := &socketProbe{rawConn: rawConn}
	probe.peekSocket = func(fd uintptr) {
		probe.outcome = peekSocket(fd, &probe.peekedByte)
	}
	return probe
}

// peek reports what is waiting on the socket.
func (probe *socketProbe) peek() peekOutcome {
	if probe == nil {
		return peekFoundNothing
	}

	// Control is used because Read fails on the expired deadline of the last command.
	if err := probe.rawConn.Control(probe.peekSocket); err != nil {
		return peekFoundClosed
	}
	return probe.outcome
}
