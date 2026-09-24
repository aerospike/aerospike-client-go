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

// Unit tests for the mark-roll-forward response parsing. MRT_COMMITTED means a
// previous attempt already told the server the transaction will be rolled
// forward, so it is a success answer, and the commit path still has to know it
// arrived to report CommitStatusAlreadyCommitted.

import (
	"io"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// markRollForwardWire builds a header-only write response carrying resultCode.
func markRollForwardWire(resultCode byte) []byte {
	return []byte{
		0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x16, // proto: version, type, size = 22
		0x16,                   // header length
		0x00, 0x00, 0x00, 0x00, // info1..info4
		resultCode,
		0x00, 0x00, 0x00, 0x01, // generation
		0x00, 0x00, 0x00, 0x00, // record ttl
		0x00, 0x00, 0x00, 0x00, // transaction ttl
		0x00, 0x00, // field count
		0x00, 0x00, // op count
	}
}

func markRollForwardCommandForWire(t *testing.T, resultCode byte) *txnMarkRollForwardCommand {
	t.Helper()

	buf := buffPool.Get(DefaultBufferSize)
	t.Cleanup(func() { buffPool.Put(buf) })

	conn := &Connection{
		conn:           &bytesConn{buf: markRollForwardWire(resultCode)},
		limitReader:    &io.LimitedReader{R: nil, N: 0},
		dataBuffer:     buf,
		origDataBuffer: buf,
	}
	conn.limitReader.R = conn.conn

	key, err := NewKey("test", "<ERO~MRT", 1)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	cmd := &txnMarkRollForwardCommand{}
	cmd.policy = NewWritePolicy(0, 0)
	cmd.key = key
	cmd.node = &Node{cluster: &Cluster{}}
	cmd.conn = conn
	cmd.dataBuffer = buf

	return cmd
}

func TestTxnMarkRollForwardParseResultSuccessCodes(t *testing.T) {
	tests := []struct {
		name                 string
		resultCode           byte
		wantAlreadyCommitted bool
	}{
		{"OK", byte(types.OK), false},
		{"MRT_COMMITTED", byte(types.MRT_COMMITTED), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := markRollForwardCommandForWire(t, tc.resultCode)

			if err := cmd.parseResult(cmd, cmd.conn); err != nil {
				t.Fatalf("parseResult(%d): got %v, want nil", tc.resultCode, err)
			}
			if cmd.alreadyCommitted != tc.wantAlreadyCommitted {
				t.Fatalf("parseResult(%d): alreadyCommitted got %v, want %v", tc.resultCode, cmd.alreadyCommitted, tc.wantAlreadyCommitted)
			}
		})
	}
}

func TestTxnMarkRollForwardParseResultRejectsEveryOtherCode(t *testing.T) {
	for rc := 1; rc <= 255; rc++ {
		if rc == int(types.MRT_COMMITTED) {
			continue
		}

		cmd := markRollForwardCommandForWire(t, byte(rc))

		err := cmd.parseResult(cmd, cmd.conn)
		if err == nil {
			t.Fatalf("parseResult(%d): got nil, want an error", rc)
		}
		if !err.Matches(types.ResultCode(rc)) {
			t.Fatalf("parseResult(%d): error %v does not match result code %d", rc, err, rc)
		}
		if cmd.alreadyCommitted {
			t.Fatalf("parseResult(%d): alreadyCommitted must stay false", rc)
		}
	}
}

// The command is retried in place, so a MRT_COMMITTED seen by an earlier
// attempt must not survive into the parse that actually succeeds.
func TestTxnMarkRollForwardParseResultClearsStaleAlreadyCommitted(t *testing.T) {
	cmd := markRollForwardCommandForWire(t, byte(types.OK))
	cmd.alreadyCommitted = true

	if err := cmd.parseResult(cmd, cmd.conn); err != nil {
		t.Fatalf("parseResult(OK): got %v, want nil", err)
	}
	if cmd.alreadyCommitted {
		t.Fatal("parseResult(OK): alreadyCommitted must be cleared")
	}
}
