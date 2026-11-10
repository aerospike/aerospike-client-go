//go:build (linux || darwin) && !app_engine

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

// Sockets are already non-blocking.
func prepareSocketForPeek(fd uintptr) error {
	return nil
}

func peekSocket(fd uintptr, peekedByte *[1]byte) peekOutcome {
	peekedLen, _, err := syscall.Recvfrom(int(fd), peekedByte[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
	switch {
	case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINTR:
		return peekFoundNothing
	case err == nil && peekedLen > 0:
		return peekFoundBytes
	}
	return peekFoundClosed
}
