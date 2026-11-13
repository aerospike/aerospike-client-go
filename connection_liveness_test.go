//go:build (linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || windows) && !app_engine

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
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// livenessServer accepts connections on loopback and hands the server side to the test.
type livenessServer struct {
	listener net.Listener
	host     *Host
	accepted chan *net.TCPConn
	// write commands answered by serveWrites
	commandsServed atomic.Int64
}

func newLivenessServer(t *testing.T) *livenessServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	server := &livenessServer{
		listener: listener,
		host:     NewHost("127.0.0.1", listener.Addr().(*net.TCPAddr).Port),
		accepted: make(chan *net.TCPConn, 16),
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				close(server.accepted)
				return
			}
			server.accepted <- conn.(*net.TCPConn)
		}
	}()
	return server
}

// connect opens a plain client connection and returns it with its server side.
func (server *livenessServer) connect(t *testing.T) (*Connection, *net.TCPConn) {
	t.Helper()

	conn, err := NewConnection(NewClientPolicy(), server.host)
	if err != nil {
		t.Fatalf("NewConnection: %v", err)
	}
	t.Cleanup(conn.Close)

	serverSide := server.nextAccepted(t)
	t.Cleanup(func() { _ = serverSide.Close() })
	return conn, serverSide
}

// connectTLS opens a TLS client connection and returns it with the server's socket and TLS session.
func (server *livenessServer) connectTLS(t *testing.T) (*Connection, *net.TCPConn, *tls.Conn) {
	t.Helper()

	policy := NewClientPolicy()
	policy.TlsConfig = &tls.Config{InsecureSkipVerify: true}

	type dialResult struct {
		conn *Connection
		err  Error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, err := NewConnection(policy, server.host)
		dialed <- dialResult{conn, err}
	}()

	serverSide := server.nextAccepted(t)
	t.Cleanup(func() { _ = serverSide.Close() })
	tlsServerSide := tls.Server(serverSide, &tls.Config{Certificates: []tls.Certificate{livenessServerCertificate()}})
	if err := tlsServerSide.Handshake(); err != nil {
		t.Fatalf("server handshake: %v", err)
	}

	result := <-dialed
	if result.err != nil {
		t.Fatalf("NewConnection: %v", result.err)
	}
	t.Cleanup(result.conn.Close)
	return result.conn, serverSide, tlsServerSide
}

func (server *livenessServer) nextAccepted(t *testing.T) *net.TCPConn {
	t.Helper()

	select {
	case conn := <-server.accepted:
		return conn
	case <-time.After(5 * time.Second):
		t.Fatal("server accepted no connection")
		return nil
	}
}

var livenessServerCertificate = sync.OnceValue(newSelfSignedCertificate)

// serveWrites answers every write command on the connection with a successful response.
func (server *livenessServer) serveWrites(conn net.Conn) {
	defer conn.Close()

	var header [8]byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		bodySize := int64(binary.BigEndian.Uint64(header[:]) & 0xFFFFFFFFFFFF)
		if _, err := io.CopyN(io.Discard, conn, bodySize); err != nil {
			return
		}
		server.commandsServed.Add(1)
		if _, err := conn.Write(markRollForwardWire(0)); err != nil {
			return
		}
	}
}

func newSelfSignedCertificate() tls.Certificate {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "liveness-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		panic(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}
}

// resetByPeer closes the server side so the client receives a TCP reset.
func resetByPeer(t *testing.T, serverSide *net.TCPConn) {
	t.Helper()

	if err := serverSide.SetLinger(0); err != nil {
		t.Fatalf("SetLinger: %v", err)
	}
	_ = serverSide.Close()
}

func becomesTrue(condition func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return condition()
}

func TestIdleConnectionIsNotReportedClosed(t *testing.T) {
	server := newLivenessServer(t)
	conn, _ := server.connect(t)

	if conn.closedByPeer() {
		t.Fatal("an open idle connection was reported closed")
	}

	// A pooled connection still carries the deadline of its last command.
	if err := conn.conn.SetDeadline(time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	for range 3 {
		if conn.closedByPeer() {
			t.Fatal("an open idle connection with an expired deadline was reported closed")
		}
	}
}

func TestConnectionClosedByPeerIsDetected(t *testing.T) {
	closures := map[string]func(t *testing.T, serverSide *net.TCPConn){
		"graceful close": func(t *testing.T, serverSide *net.TCPConn) { _ = serverSide.Close() },
		"reset":          resetByPeer,
	}
	for name, closeServerSide := range closures {
		t.Run(name, func(t *testing.T) {
			server := newLivenessServer(t)
			conn, serverSide := server.connect(t)

			closeServerSide(t, serverSide)

			if !becomesTrue(conn.closedByPeer) {
				t.Fatal("the closed connection was still reported open")
			}
			if !conn.IsConnected() {
				t.Fatal("the probe closed the connection itself")
			}
		})
	}
}

func TestUnreadBytesOnPlainConnectionCountAsClosed(t *testing.T) {
	server := newLivenessServer(t)
	conn, serverSide := server.connect(t)

	if _, err := serverSide.Write([]byte{0x7f}); err != nil {
		t.Fatalf("server write: %v", err)
	}

	if !becomesTrue(conn.closedByPeer) {
		t.Fatal("a plain connection with unread bytes was reported open")
	}

	if !conn.closedByPeer() {
		t.Fatal("a second probe of the same unread bytes reported the connection open")
	}

	// The probe only peeks: the byte is still there to read.
	if err := conn.SetTimeout(time.Now().Add(5*time.Second), 0); err != nil {
		t.Fatalf("SetTimeout: %v", err)
	}
	unread := make([]byte, 1)
	if _, err := conn.Read(unread, len(unread)); err != nil || unread[0] != 0x7f {
		t.Fatalf("read after probe got %#x, err %v; want 0x7f", unread[0], err)
	}
	if conn.closedByPeer() {
		t.Fatal("the connection was still reported closed after the bytes were read")
	}
}

func TestTLSConnectionWithPendingRecordStaysOpenUntilReset(t *testing.T) {
	server := newLivenessServer(t)
	conn, serverSide, tlsServerSide := server.connectTLS(t)

	if conn.closedByPeer() {
		t.Fatal("an open idle TLS connection was reported closed")
	}

	// A record arrives while the connection is idle, as session tickets and key updates do.
	if _, err := tlsServerSide.Write([]byte("ping")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if !becomesTrue(func() bool { return conn.peerProbe.peek() == peekFoundBytes }) {
		t.Fatal("the TLS record did not reach the client socket")
	}
	if conn.closedByPeer() {
		t.Fatal("a TLS connection with a pending record was reported closed")
	}

	// The probe only peeks: the record is intact and the session still decrypts it.
	if err := conn.SetTimeout(time.Now().Add(5*time.Second), 0); err != nil {
		t.Fatalf("SetTimeout: %v", err)
	}
	received := make([]byte, 4)
	if _, err := conn.Read(received, len(received)); err != nil || string(received) != "ping" {
		t.Fatalf("read over TLS after probe got %q, err %v; want \"ping\"", received, err)
	}

	resetByPeer(t, serverSide)
	if !becomesTrue(conn.closedByPeer) {
		t.Fatal("a reset TLS connection was still reported open")
	}
}

func TestProbedConnectionStillBlocksUntilDataOrDeadline(t *testing.T) {
	server := newLivenessServer(t)
	conn, serverSide := server.connect(t)

	// More than the socket buffers hold, so the write and the read both have to wait.
	payload := make([]byte, 4<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	go func() {
		received := make([]byte, len(payload))
		if _, err := io.ReadFull(serverSide, received); err != nil {
			return
		}
		_, _ = serverSide.Write(received)
	}()

	if conn.closedByPeer() {
		t.Fatal("an open idle connection was reported closed")
	}
	if err := conn.SetTimeout(time.Now().Add(time.Minute), 0); err != nil {
		t.Fatalf("SetTimeout: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	echoed := make([]byte, len(payload))
	if _, err := conn.Read(echoed, len(echoed)); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(payload, echoed) {
		t.Fatal("the payload was not echoed back intact")
	}

	// A read with nothing to read waits for its deadline and reports a timeout.
	if conn.closedByPeer() {
		t.Fatal("an open idle connection was reported closed")
	}
	readTimeout := 200 * time.Millisecond
	if err := conn.SetTimeout(time.Now().Add(readTimeout), 0); err != nil {
		t.Fatalf("SetTimeout: %v", err)
	}
	readStart := time.Now()
	_, err := conn.Read(echoed, 1)
	if err == nil || !err.Matches(types.TIMEOUT) {
		t.Fatalf("read without data got err %v, want a timeout", err)
	}
	if waited := time.Since(readStart); waited < readTimeout/2 {
		t.Fatalf("read gave up after %s, before its %s deadline", waited, readTimeout)
	}
	if conn.closedByPeer() {
		t.Fatal("the connection was reported closed after a read timeout")
	}
}

//go:noinline
func abandonConnection(t *testing.T, server *livenessServer) *net.TCPConn {
	t.Helper()

	conn, err := NewConnection(NewClientPolicy(), server.host)
	if err != nil {
		t.Fatalf("NewConnection: %v", err)
	}
	if conn.peerProbe == nil {
		t.Fatal("connection has no liveness probe")
	}
	return server.nextAccepted(t)
}

func TestAbandonedConnectionIsStillClosedByGarbageCollector(t *testing.T) {
	server := newLivenessServer(t)
	serverSide := abandonConnection(t, server)
	defer serverSide.Close()

	clientClosed := make(chan struct{})
	go func() {
		_, _ = serverSide.Read(make([]byte, 1))
		close(clientClosed)
	}()

	closed := becomesTrue(func() bool {
		runtime.GC()
		select {
		case <-clientClosed:
			return true
		default:
			return false
		}
	})
	if !closed {
		t.Fatal("a connection that was never closed stayed open after garbage collection")
	}
}

// stalePoolWriteCommand runs a real write command against one fixed node.
type stalePoolWriteCommand struct {
	writeCommand
	targetNode *Node
}

func (cmd *stalePoolWriteCommand) getNode(ifc command) (*Node, Error) {
	return cmd.targetNode, nil
}

func (cmd *stalePoolWriteCommand) prepareRetry(ifc command, isTimeout bool) bool {
	return true
}

// newLivenessNode returns a node with an empty pool that dials the server.
func newLivenessNode(t *testing.T, server *livenessServer) *Node {
	t.Helper()

	cluster := &Cluster{clientPolicy: &atomic.Pointer[ClientPolicy]{}}
	cluster.clientPolicy.Store(NewClientPolicy())
	node := &Node{
		cluster:     cluster,
		name:        "stale-pool",
		host:        server.host,
		connections: *newConnectionHeap(0, 8),
	}
	node.active.Set(true)
	t.Cleanup(func() {
		for conn := node.connections.Poll(0); conn != nil; conn = node.connections.Poll(0) {
			conn.Close()
		}
	})
	return node
}

// pooledConnectionResetByServer dials a node connection that the server resets at once.
func pooledConnectionResetByServer(t *testing.T, node *Node, server *livenessServer) *Connection {
	t.Helper()

	conn, err := node.newConnection(true)
	if err != nil {
		t.Fatalf("newConnection: %v", err)
	}
	resetByPeer(t, server.nextAccepted(t))
	if !becomesTrue(conn.closedByPeer) {
		t.Fatal("the reset did not reach the connection")
	}
	return conn
}

func TestCheckoutReturnsTheLiveConnectionAndClosesTheDeadOnes(t *testing.T) {
	server := newLivenessServer(t)
	node := newLivenessNode(t, server)

	liveConnection, err := node.newConnection(true)
	if err != nil {
		t.Fatalf("newConnection: %v", err)
	}
	liveServerSide := server.nextAccepted(t)
	t.Cleanup(func() { _ = liveServerSide.Close() })
	deadConnections := []*Connection{
		pooledConnectionResetByServer(t, node, server),
		pooledConnectionResetByServer(t, node, server),
	}

	for _, conn := range append([]*Connection{liveConnection}, deadConnections...) {
		if !node.putConnectionWithHint(conn, 0) {
			t.Fatal("pool rejected the connection")
		}
	}

	checkedOut, err := node.getConnectionWithHint(0, 0, 0, 0)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	if checkedOut != liveConnection {
		t.Fatal("checkout returned a connection the server had reset")
	}
	if !liveConnection.IsConnected() {
		t.Fatal("checkout closed the live connection")
	}

	// Only dead connections are left, so the next checkout finds none to use.
	if _, err := node.getConnectionWithHint(0, 0, 0, 0); !errors.Is(err, ErrConnectionPoolEmpty) {
		t.Fatalf("checkout from a pool of dead connections got err %v, want pool empty", err)
	}
	for _, conn := range deadConnections {
		if conn.IsConnected() {
			t.Error("a reset pooled connection was not closed")
		}
	}
	if closed := node.stats.ConnectionsClosed.Get(); closed != len(deadConnections) {
		t.Errorf("node closed %d connections, want %d", closed, len(deadConnections))
	}
}

func newStalePoolWriteCommand(t *testing.T, node *Node, policy *WritePolicy, key *Key) *stalePoolWriteCommand {
	t.Helper()

	bwc, err := newBaseWriteCommand(nil, policy, key)
	if err != nil {
		t.Fatalf("newBaseWriteCommand: %v", err)
	}
	return &stalePoolWriteCommand{
		writeCommand: writeCommand{
			baseWriteCommand: bwc,
			binMap:           BinMap{"bin": "value"},
			operation:        _WRITE,
		},
		targetNode: node,
	}
}

func TestExecuteAtUsesNoRetryForDeadPooledConnections(t *testing.T) {
	server := newLivenessServer(t)
	node := newLivenessNode(t, server)

	key, err := NewKey("test", "stale-pool", 1)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	// Every pooled connection is reset by the server while it sits idle.
	const stalePooled = 3
	staleConnections := make([]*Connection, 0, stalePooled)
	for range stalePooled {
		staleConnections = append(staleConnections, pooledConnectionResetByServer(t, node, server))
	}
	for _, conn := range staleConnections {
		if !node.putConnectionWithHint(conn, key.digest[0]) {
			t.Fatal("pool rejected the connection")
		}
	}
	if pooled := node.connections.LenAll(); pooled != stalePooled {
		t.Fatalf("pool holds %d connections, want %d", pooled, stalePooled)
	}

	go func() {
		for serverSide := range server.accepted {
			go server.serveWrites(serverSide)
		}
	}()

	policy := NewWritePolicy(0, 0)
	policy.MaxRetries = 0
	policy.TotalTimeout = 10 * time.Second
	cmd := newStalePoolWriteCommand(t, node, policy, key)

	if err := cmd.executeAt(cmd, policy.GetBasePolicy(), time.Now().Add(policy.TotalTimeout), -1); err != nil {
		t.Fatalf("write failed on a pool of reset connections: %v", err)
	}

	if cmd.commandSentCounter != 1 {
		t.Errorf("command used %d attempts, want 1", cmd.commandSentCounter)
	}
	if served := server.commandsServed.Load(); served != 1 {
		t.Errorf("server received %d commands, want 1", served)
	}
	for _, conn := range staleConnections {
		if conn.IsConnected() {
			t.Error("a reset pooled connection was not discarded")
		}
	}
	if closed := node.stats.ConnectionsClosed.Get(); closed != stalePooled {
		t.Errorf("node closed %d connections, want %d", closed, stalePooled)
	}
	if failed := node.stats.ConnectionsFailed.Get(); failed != 0 {
		t.Errorf("node counted %d failed connections, want 0", failed)
	}
	// An empty pool can start more than one replacement before the first is ready.
	if pooled := node.connections.LenAll(); pooled < 1 {
		t.Error("the replacement connection was not returned to the pool")
	}

	// The healthy replacement is reused, not discarded.
	cmd.commandSentCounter = 0
	if err := cmd.executeAt(cmd, policy.GetBasePolicy(), time.Now().Add(policy.TotalTimeout), -1); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if cmd.commandSentCounter != 1 {
		t.Errorf("second write used %d attempts, want 1", cmd.commandSentCounter)
	}
	if closed := node.stats.ConnectionsClosed.Get(); closed != stalePooled {
		t.Errorf("node closed %d connections after the second write, want still %d", closed, stalePooled)
	}
}
