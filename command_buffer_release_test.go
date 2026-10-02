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

// Buffer handling at the end of a successful executeAt. Under a UseCompression
// policy sizeBufferSz keeps a second reference to the connection's live buffer
// in dataBufferCompress, and compress() only clears it once the request exceeds
// _COMPRESS_THRESHOLD. Both sides of that threshold are covered here.

import (
	"io"
	"strings"
	"testing"
	"time"
)

// bufferProbeCommand drives a real writeCommand through executeAt against an
// in-memory connection, and snapshots the command buffers at the point the
// buffer release used to happen: parseResult is the last thing executeAt calls
// before it, and putConnection the first thing after.
type bufferProbeCommand struct {
	writeCommand

	fakeConn *Connection

	requestSize           int
	preDataBuffer         []byte
	preDataBufferCompress []byte
	connPutBack           bool
}

func (cmd *bufferProbeCommand) getNode(ifc command) (*Node, Error) {
	return cmd.node, nil
}

func (cmd *bufferProbeCommand) getConnection(policy Policy) (*Connection, Error) {
	return cmd.fakeConn, nil
}

func (cmd *bufferProbeCommand) putConnection(conn *Connection) {
	cmd.connPutBack = true
}

func (cmd *bufferProbeCommand) writeBuffer(ifc command) Error {
	err := cmd.writeCommand.writeBuffer(ifc)
	cmd.requestSize = cmd.dataOffset
	return err
}

func (cmd *bufferProbeCommand) parseResult(ifc command, conn *Connection) Error {
	err := cmd.writeCommand.parseResult(ifc, conn)
	cmd.preDataBuffer = cmd.dataBuffer
	cmd.preDataBufferCompress = cmd.dataBufferCompress
	return err
}

func newBufferProbeCommand(t *testing.T, payload string, useCompression bool) *bufferProbeCommand {
	t.Helper()

	key, err := NewKey("test", "buffer-release", 1)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	policy := NewWritePolicy(0, 0)
	policy.UseCompression = useCompression

	bwc, err := newBaseWriteCommand(nil, policy, key)
	if err != nil {
		t.Fatalf("newBaseWriteCommand: %v", err)
	}

	buf := make([]byte, DefaultBufferSize)
	conn := &Connection{
		conn:                 &bytesConn{buf: markRollForwardWire(0)},
		limitReader:          &io.LimitedReader{R: nil, N: 0},
		dataBuffer:           buf,
		origDataBuffer:       buf,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	conn.limitReader.R = conn.conn

	node := &Node{cluster: &Cluster{}}
	node.active.Set(true)

	cmd := &bufferProbeCommand{
		writeCommand: writeCommand{
			baseWriteCommand: bwc,
			binMap:           BinMap{"bin": payload},
			operation:        _WRITE,
		},
		fakeConn: conn,
	}
	cmd.node = node

	return cmd
}

// sharesArray reports whether both slices start at the same address.
func sharesArray(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

func TestExecuteAtKeepsConnectionBufferUnderCompressThreshold(t *testing.T) {
	cmd := newBufferProbeCommand(t, "small", true)
	conn := cmd.fakeConn

	if err := cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1); err != nil {
		t.Fatalf("executeAt: %v", err)
	}

	if cmd.requestSize > _COMPRESS_THRESHOLD {
		t.Fatalf("request size got %d, want <= %d", cmd.requestSize, _COMPRESS_THRESHOLD)
	}

	// compress() left the request uncompressed, so dataBufferCompress still
	// referenced the connection's live buffer at the release point.
	if !sharesArray(cmd.preDataBufferCompress, conn.origDataBuffer) {
		t.Fatal("dataBufferCompress did not reference the connection buffer before cleanup")
	}

	assertConnectionBufferIntact(t, cmd)
}

func TestExecuteAtKeepsConnectionBufferOverCompressThreshold(t *testing.T) {
	cmd := newBufferProbeCommand(t, strings.Repeat("x", 4*_COMPRESS_THRESHOLD), true)
	conn := cmd.fakeConn

	if err := cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1); err != nil {
		t.Fatalf("executeAt: %v", err)
	}

	if cmd.requestSize <= _COMPRESS_THRESHOLD {
		t.Fatalf("request size got %d, want > %d", cmd.requestSize, _COMPRESS_THRESHOLD)
	}

	// compress() cleared dataBufferCompress and moved the connection's buffer
	// back to dataBuffer, which is what the release point saw here.
	if cmd.preDataBufferCompress != nil {
		t.Fatal("dataBufferCompress was not cleared by compress()")
	}
	if !sharesArray(cmd.preDataBuffer, conn.origDataBuffer) {
		t.Fatal("dataBuffer did not reference the connection buffer before cleanup")
	}

	assertConnectionBufferIntact(t, cmd)
}

// TestExecuteAtDoesNotDonateConnectionBufferToPool is the regression guard.
// A request under _COMPRESS_THRESHOLD takes the identical code path with and
// without UseCompression -- sizeBufferSz only re-slices, and compress() returns
// early -- so the two must allocate the same amount. Handing the connection's
// buffer to buffPool costs one extra allocation to box the slice into sync.Pool's
// any parameter, which makes that donation observable without depending on what
// a later buffPool.Get returns.
func TestExecuteAtDoesNotDonateConnectionBufferToPool(t *testing.T) {
	compressed := allocsPerExecuteAt(t, true)
	plain := allocsPerExecuteAt(t, false)

	if compressed != plain {
		t.Errorf("UseCompression executeAt allocated %v objects, uncompressed %v; the difference is the connection buffer being handed to buffPool", compressed, plain)
	}
}

func allocsPerExecuteAt(t *testing.T, useCompression bool) float64 {
	t.Helper()

	return testing.AllocsPerRun(100, func() {
		cmd := newBufferProbeCommand(t, "small", useCompression)
		if err := cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1); err != nil {
			t.Fatalf("executeAt: %v", err)
		}
		if cmd.requestSize > _COMPRESS_THRESHOLD {
			t.Fatalf("request size got %d, want <= %d", cmd.requestSize, _COMPRESS_THRESHOLD)
		}
	})
}

// assertConnectionBufferIntact checks that the command dropped its references
// and left the connection with its own buffer, ready to be used again by the
// next command that borrows the connection.
func assertConnectionBufferIntact(t *testing.T, cmd *bufferProbeCommand) {
	t.Helper()

	conn := cmd.fakeConn
	if cmd.dataBuffer != nil {
		t.Error("cmd.dataBuffer was not cleared")
	}
	if cmd.dataBufferCompress != nil {
		t.Error("cmd.dataBufferCompress was not cleared")
	}
	if !sharesArray(conn.dataBuffer, conn.origDataBuffer) {
		t.Error("connection buffer was not reset to the original buffer")
	}
	if len(conn.dataBuffer) != DefaultBufferSize {
		t.Errorf("connection buffer length got %d, want %d", len(conn.dataBuffer), DefaultBufferSize)
	}
	if !cmd.connPutBack {
		t.Error("connection was not put back")
	}
}
