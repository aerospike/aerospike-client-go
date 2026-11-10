//go:build windows && !app_engine

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

const (
	_FIONBIO        = 0x8004667e
	_MSG_PEEK       = 0x2
	_WSAEWOULDBLOCK = syscall.Errno(10035)
)

type peekOutcome uint8

const (
	peekFoundNothing peekOutcome = iota
	peekFoundBytes
	peekFoundClosed
)

// socketProbe peeks at a socket to find out if the peer closed it.
// It never blocks and never consumes data.
type socketProbe struct {
	rawConn    syscall.RawConn
	peek       func(fd uintptr)
	peekedByte [1]byte
	outcome    peekOutcome
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

	// A peek only returns at once on a non-blocking socket.
	// The runtime's own reads and writes are overlapped and do not depend on this mode.
	var modeErr error
	nonBlocking := [4]byte{1}
	var bytesReturned uint32
	err = rawConn.Control(func(fd uintptr) {
		modeErr = syscall.WSAIoctl(syscall.Handle(fd), _FIONBIO, &nonBlocking[0], uint32(len(nonBlocking)), nil, 0, &bytesReturned, nil, 0)
	})
	if err != nil || modeErr != nil {
		return nil
	}

	// The probe must not reference the Connection, or its finalizer would never run.
	probe := &socketProbe{rawConn: rawConn}
	probe.peek = func(fd uintptr) {
		buffer := syscall.WSABuf{Len: 1, Buf: &probe.peekedByte[0]}
		flags := uint32(_MSG_PEEK)
		var peekedLen uint32
		err := syscall.WSARecv(syscall.Handle(fd), &buffer, 1, &peekedLen, &flags, nil, nil)
		switch {
		case err == _WSAEWOULDBLOCK:
			probe.outcome = peekFoundNothing
		case err == nil && peekedLen > 0:
			probe.outcome = peekFoundBytes
		default:
			probe.outcome = peekFoundClosed
		}
	}
	return probe
}

// peerClosed returns true if the peer closed or reset the socket.
// Unread bytes also count as closed, unless pendingBytesAllowed is set.
func (probe *socketProbe) peerClosed(pendingBytesAllowed bool) bool {
	if probe == nil {
		return false
	}

	if err := probe.rawConn.Control(probe.peek); err != nil {
		return true
	}

	switch probe.outcome {
	case peekFoundNothing:
		return false
	case peekFoundBytes:
		return !pendingBytesAllowed
	}
	return true
}
