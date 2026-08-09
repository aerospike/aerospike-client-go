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

// Unit tests for single-command read response parsing (record_parser).
// Covers the v8 regression where the compressed path skipped the inflated
// message header read and passed the original-size field to validateHeader().

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

const largePayloadLen = _COMPRESS_THRESHOLD * 512 // 64 KiB

// 64 KiB uncompressed read response (header + bin op prefix; payload is all 'p').
var fixtureUncompressedLargeHeader = []byte{
	0x02, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x21, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
	0x00, 0x07, 0x00, 0x03, 0x00, 0x03, 0x62, 0x69, 0x6e,
}

// 64 KiB compressed read response (outer envelope + zlib body).
var fixtureCompressedLarge = []byte{
	0x02, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x29,
	0x78, 0x9c, 0xec, 0xc0, 0xa1, 0x0d, 0x80, 0x50, 0x0c, 0x05, 0xc0, 0xd7, 0xdf, 0x10, 0x36, 0xc0,
	0xb3, 0x1a, 0x0e, 0x43, 0xba, 0xbf, 0xc2, 0x90, 0xb0, 0xc4, 0xdd, 0x5a, 0x49, 0x2a, 0xe7, 0x91,
	0xcf, 0x96, 0x5f, 0xa5, 0xb2, 0xa7, 0xd3, 0xd7, 0xfd, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xcc, 0xcc, 0x1b, 0x00, 0x00, 0xff, 0xff,
	0x57, 0x07, 0x08, 0x1a,
}

func fixtureUncompressedLarge() []byte {
	wire := make([]byte, len(fixtureUncompressedLargeHeader)+largePayloadLen)
	copy(wire, fixtureUncompressedLargeHeader)
	copy(wire[len(fixtureUncompressedLargeHeader):], strings.Repeat("p", largePayloadLen))
	return wire
}

func TestNewRecordParserUncompressedWire(t *testing.T) {
	rp := parseRecordParserFromWire(t, fixtureUncompressedLarge())
	assertRecordParserRoundTrip(t, largePayloadLen, 5, rp, "bin")
}

func TestNewRecordParserCompressedWire(t *testing.T) {
	rp := parseRecordParserFromWire(t, fixtureCompressedLarge)
	assertRecordParserRoundTrip(t, largePayloadLen, 5, rp, "bin")
}

func assertRecordParserRoundTrip(t *testing.T, wantLen int, wantGen uint32, rp *recordParser, binName string) {
	t.Helper()
	if rp == nil {
		t.Fatal("record parser is nil")
	}
	if rp.resultCode != types.OK {
		t.Fatalf("resultCode got %v, want OK", rp.resultCode)
	}
	if rp.opCount != 1 {
		t.Fatalf("opCount got %d, want 1", rp.opCount)
	}
	if rp.generation != wantGen {
		t.Fatalf("generation got %d, want %d", rp.generation, wantGen)
	}

	key := &Key{namespace: "test", setName: "set"}
	record, err := rp.parseRecord(key, false)
	if err != nil {
		t.Fatalf("parseRecord: %v", err)
	}
	got, ok := record.Bins[binName].(string)
	if !ok {
		t.Fatalf("bin %q unexpected type %T", binName, record.Bins[binName])
	}
	if len(got) != wantLen {
		t.Fatalf("bin %q got len=%d, want len=%d", binName, len(got), wantLen)
	}
	if got != strings.Repeat("p", wantLen) {
		t.Fatalf("bin %q payload mismatch", binName)
	}
}

func parseRecordParserFromWire(t *testing.T, wire []byte) *recordParser {
	t.Helper()

	buf := buffPool.Get(len(wire) + DefaultBufferSize)
	t.Cleanup(func() { buffPool.Put(buf) })

	conn := &Connection{
		conn:           &bytesConn{buf: wire},
		limitReader:    &io.LimitedReader{R: nil, N: 0},
		dataBuffer:     buf,
		origDataBuffer: buf,
	}
	conn.limitReader.R = conn.conn

	cmd := &baseCommand{conn: conn}
	cmd.dataBuffer = buf
	rp, err := newRecordParser(cmd)
	if err != nil {
		t.Fatalf("newRecordParser: %v", err)
	}
	return rp
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
