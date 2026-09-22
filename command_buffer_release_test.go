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
	"net"
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

	wc, err := newWriteCommand(nil, policy, key, nil, BinMap{"bin": payload}, _WRITE)
	if err != nil {
		t.Fatalf("newWriteCommand: %v", err)
	}

	// v7's executeAt writes and compresses the request twice, so the slice
	// executeAt used to hand to buffPool is the connection buffer minus one
	// compression pad. TieredBufferPool.Put drops buffers whose capacity is not
	// a power of two, so the pad is added here to keep that slice pool sized and
	// the donation observable.
	buf := make([]byte, DefaultBufferSize+msgHeaderPad+zlibHeaderPad)
	conn := &Connection{
		conn:                 &bytesConn{buf: writeResponseWire(0)},
		limitReader:          &io.LimitedReader{R: nil, N: 0},
		dataBuffer:           buf,
		origDataBuffer:       buf,
		bufferAdjustDeadline: time.Now().Add(time.Hour),
	}
	conn.limitReader.R = conn.conn

	node := &Node{cluster: &Cluster{}}
	node.active.Set(true)

	cmd := &bufferProbeCommand{
		writeCommand: wc,
		fakeConn:     conn,
	}
	cmd.node = node

	return cmd
}

// writeResponseWire builds a header-only write response carrying resultCode.
func writeResponseWire(resultCode byte) []byte {
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

type bytesConn struct {
	buf []byte
	off int
}

func (c *bytesConn) Read(p []byte) (int, error) {
	if c.off >= len(c.buf) {
		return 0, io.EOF
	}
	n := copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}

func (c *bytesConn) Write(p []byte) (int, error)        { return len(p), nil }
func (c *bytesConn) Close() error                       { return nil }
func (c *bytesConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (c *bytesConn) RemoteAddr() net.Addr               { return c.LocalAddr() }
func (c *bytesConn) SetDeadline(t time.Time) error      { return nil }
func (c *bytesConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *bytesConn) SetWriteDeadline(t time.Time) error { return nil }

// sharesArray reports whether both slices start at the same address.
func sharesArray(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

// aliases reports whether inner is a trailing sub-slice of outer's array.
// v7's executeAt writes and compresses the request twice, so the compression
// padding is applied twice and dataBufferCompress starts inside the connection
// buffer rather than at its first byte.
func aliases(inner, outer []byte) bool {
	off := cap(outer) - cap(inner)
	return off >= 0 && off < len(outer) && len(inner) > 0 && &outer[off] == &inner[0]
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
	if !aliases(cmd.preDataBufferCompress, conn.origDataBuffer) {
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
	if !aliases(cmd.preDataBuffer, conn.origDataBuffer) {
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
	if len(conn.dataBuffer) != len(conn.origDataBuffer) {
		t.Errorf("connection buffer length got %d, want %d", len(conn.dataBuffer), len(conn.origDataBuffer))
	}
	if !cmd.connPutBack {
		t.Error("connection was not put back")
	}
}
