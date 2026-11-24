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

import "syscall"

const (
	_FIONBIO        = 0x8004667e
	_MSG_PEEK       = 0x2
	_WSAEWOULDBLOCK = syscall.Errno(10035)
)

// A peek only returns at once on a non-blocking socket.
// The runtime's own reads and writes are overlapped and do not depend on this mode.
func prepareSocketForPeek(fd uintptr) error {
	nonBlocking := [4]byte{1}
	var bytesReturned uint32
	return syscall.WSAIoctl(syscall.Handle(fd), _FIONBIO, &nonBlocking[0], uint32(len(nonBlocking)), nil, 0, &bytesReturned, nil, 0)
}

func peekSocket(fd uintptr, peekedByte *[1]byte) peekOutcome {
	buffer := syscall.WSABuf{Len: 1, Buf: &peekedByte[0]}
	flags := uint32(_MSG_PEEK)
	var peekedLen uint32
	err := syscall.WSARecv(syscall.Handle(fd), &buffer, 1, &peekedLen, &flags, nil, nil)
	switch {
	case err == _WSAEWOULDBLOCK:
		return peekFoundNothing
	case err == nil && peekedLen > 0:
		return peekFoundBytes
	}
	return peekFoundClosed
}
