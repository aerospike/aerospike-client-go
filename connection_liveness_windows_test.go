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
	"testing"
	"time"
)

// Experiment: does a peek on a healthy idle socket wait when the socket was
// not switched to non-blocking mode?
func TestPeekWithoutNonBlockingMode(t *testing.T) {
	server := newLivenessServer(t)

	// A plain dial, so no probe has touched the socket's mode.
	dialed, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer dialed.Close()
	serverSide := server.nextAccepted(t)
	defer serverSide.Close()

	rawConn, err := dialed.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	var peekedByte [1]byte
	var outcome peekOutcome
	returned := make(chan time.Duration, 1)
	peekStart := time.Now()
	go func() {
		_ = rawConn.Control(func(fd uintptr) {
			outcome = peekSocket(fd, &peekedByte)
		})
		returned <- time.Since(peekStart)
	}()

	select {
	case elapsed := <-returned:
		t.Logf("RESULT: peek returned after %s with outcome %d (0 nothing, 1 bytes, 2 closed): it does NOT block", elapsed, outcome)
		return
	case <-time.After(2 * time.Second):
		t.Log("RESULT: peek had not returned after 2s on a healthy idle socket: it BLOCKS")
	}

	// Release the blocked peek by giving it a byte to see.
	if _, err := serverSide.Write([]byte{0x7f}); err != nil {
		t.Fatalf("server write: %v", err)
	}
	select {
	case elapsed := <-returned:
		t.Logf("RESULT: peek returned %s after it started, once the server sent a byte, with outcome %d", elapsed, outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("peek stayed blocked after the server sent a byte")
	}
}
