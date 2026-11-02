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

package dns

import (
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
)

const (
	stubTypeA    = 1
	stubTypeAAAA = 28
	stubTypeSRV  = 33
	stubNOERROR  = 0
	stubSERVFAIL = 2
	stubNXDOMAIN = 3
	stubNoAnswer = -1
)

type stubDNSServer struct {
	conn      *net.UDPConn
	addr      string
	lock      sync.Mutex
	answers   map[string][][]byte
	rcodes    map[string]int
	queries   []string
	silent    bool
	truncated bool
	tcp       net.Listener
	done      chan struct{}
}

func newStubDNSServer(t *testing.T) *stubDNSServer {
	t.Helper()
	server, err := startStubDNSServer(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}

func startStubDNSServer(silent bool) (*stubDNSServer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	server := &stubDNSServer{
		conn:    conn,
		addr:    conn.LocalAddr().String(),
		answers: map[string][][]byte{},
		rcodes:  map[string]int{},
		silent:  silent,
		done:    make(chan struct{}),
	}
	go server.serve()
	return server, nil
}

func (server *stubDNSServer) Address() string {
	return server.addr
}

func (server *stubDNSServer) Queries() []string {
	server.lock.Lock()
	defer server.lock.Unlock()
	out := make([]string, len(server.queries))
	copy(out, server.queries)
	return out
}

func (server *stubDNSServer) a(name string, addresses ...string) *stubDNSServer {
	for _, address := range addresses {
		ip := net.ParseIP(address).To4()
		server.addAnswer(name, stubTypeA, ip)
	}
	return server
}

func (server *stubDNSServer) aaaa(name string, addresses ...string) *stubDNSServer {
	for _, address := range addresses {
		ip := net.ParseIP(address).To16()
		server.addAnswer(name, stubTypeAAAA, ip)
	}
	return server
}

func (server *stubDNSServer) srv(name string, priority, weight, port int, target string) *stubDNSServer {
	rdata := make([]byte, 6)
	binary.BigEndian.PutUint16(rdata[0:2], uint16(priority))
	binary.BigEndian.PutUint16(rdata[2:4], uint16(weight))
	binary.BigEndian.PutUint16(rdata[4:6], uint16(port))
	rdata = append(rdata, encodeDNSName(target)...)
	server.addAnswer(name, stubTypeSRV, rdata)
	return server
}

func (server *stubDNSServer) rcode(name string, qtype, code int) *stubDNSServer {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.rcodes[stubKey(name, qtype)] = code
	return server
}

func (server *stubDNSServer) truncate() *stubDNSServer {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.truncated = true
	return server
}

func (server *stubDNSServer) hangTCP() *stubDNSServer {
	listener, err := net.Listen("tcp", server.addr)
	if err != nil {
		panic(err)
	}
	server.tcp = listener
	return server
}

func (server *stubDNSServer) addAnswer(name string, qtype int, rdata []byte) {
	server.lock.Lock()
	defer server.lock.Unlock()
	key := stubKey(name, qtype)
	server.answers[key] = append(server.answers[key], stubRecord(name, qtype, rdata))
}

func (server *stubDNSServer) serve() {
	defer close(server.done)
	buf := make([]byte, 2048)
	for {
		n, remote, err := server.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		msg := append([]byte(nil), buf[:n]...)
		qname, qtype, qstart, qend, ok := parseDNSQuestion(msg)
		if !ok {
			continue
		}
		key := stubKey(qname, qtype)
		server.lock.Lock()
		server.queries = append(server.queries, key)
		silent := server.silent
		truncated := server.truncated
		answers := append([][]byte(nil), server.answers[key]...)
		rcode, hasRcode := server.rcodes[key]
		if !hasRcode {
			if len(answers) == 0 && !server.hasNameLocked(qname) {
				rcode = stubNXDOMAIN
			} else {
				rcode = stubNOERROR
			}
		}
		server.lock.Unlock()

		if silent || rcode == stubNoAnswer {
			continue
		}
		if rcode != stubNOERROR || truncated {
			answers = nil
		}

		header := make([]byte, 12)
		copy(header[0:2], msg[0:2])
		flags := uint16(0x8400 | 0x0080 | rcode)
		if truncated {
			flags |= 0x0200
		}
		if len(msg) >= 4 {
			flags |= binary.BigEndian.Uint16(msg[2:4]) & 0x0100
		}
		binary.BigEndian.PutUint16(header[2:4], flags)
		binary.BigEndian.PutUint16(header[4:6], 1)
		binary.BigEndian.PutUint16(header[6:8], uint16(len(answers)))

		out := append(header, msg[qstart:qend]...)
		for _, rr := range answers {
			out = append(out, rr...)
		}
		_, _ = server.conn.WriteToUDP(out, remote)
	}
}

func (server *stubDNSServer) hasNameLocked(name string) bool {
	prefix := strings.ToLower(absolute(name))
	for key := range server.answers {
		if strings.HasPrefix(key, prefix+"/") {
			return true
		}
	}
	return false
}

func (server *stubDNSServer) Close() {
	_ = server.conn.Close()
	if server.tcp != nil {
		_ = server.tcp.Close()
	}
	<-server.done
}

func stubKey(name string, qtype int) string {
	n := strings.ToLower(name)
	if !strings.HasSuffix(n, ".") {
		n += "."
	}
	return n + "/" + itoa(qtype)
}

func stubRecord(name string, qtype int, rdata []byte) []byte {
	out := encodeDNSName(name)
	meta := make([]byte, 10)
	binary.BigEndian.PutUint16(meta[0:2], uint16(qtype))
	binary.BigEndian.PutUint16(meta[2:4], 1)
	binary.BigEndian.PutUint32(meta[4:8], 60)
	binary.BigEndian.PutUint16(meta[8:10], uint16(len(rdata)))
	out = append(out, meta...)
	return append(out, rdata...)
}

func encodeDNSName(name string) []byte {
	var out []byte
	for _, label := range strings.Split(name, ".") {
		if label == "" {
			continue
		}
		out = append(out, byte(len(label)))
		out = append(out, []byte(label)...)
	}
	return append(out, 0)
}

func parseDNSQuestion(msg []byte) (qname string, qtype int, qstart, qend int, ok bool) {
	if len(msg) < 12 {
		return "", 0, 0, 0, false
	}
	qstart = 12
	name, offset, err := decodeDNSName(msg, 12)
	if err != nil || offset+4 > len(msg) {
		return "", 0, 0, 0, false
	}
	qtype = int(binary.BigEndian.Uint16(msg[offset : offset+2]))
	return name, qtype, qstart, offset + 4, true
}

func decodeDNSName(msg []byte, offset int) (string, int, error) {
	var labels []string
	jumped := false
	end := offset
	for hops := 0; hops < 20; hops++ {
		if offset >= len(msg) {
			return "", 0, errEmptyAnswer
		}
		length := int(msg[offset])
		if length == 0 {
			offset++
			if !jumped {
				end = offset
			}
			return strings.Join(labels, ".") + ".", end, nil
		}
		if length&0xC0 == 0xC0 {
			if offset+1 >= len(msg) {
				return "", 0, errEmptyAnswer
			}
			if !jumped {
				end = offset + 2
				jumped = true
			}
			offset = int(binary.BigEndian.Uint16(msg[offset:offset+2]) & 0x3FFF)
			continue
		}
		offset++
		if offset+length > len(msg) {
			return "", 0, errEmptyAnswer
		}
		labels = append(labels, string(msg[offset:offset+length]))
		offset += length
	}
	return "", 0, errEmptyAnswer
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [12]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}
