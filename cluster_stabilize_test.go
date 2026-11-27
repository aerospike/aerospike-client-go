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
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Cluster start with a flapping node count", gg.Label("discovery"), func() {
	gg.It("returns before the timeout when a peer sits behind a round-robin load balancer", func() {
		const localhost = "127.0.0.1"
		loopback := net.ParseIP(localhost)
		startNode := func(name string) *stubNodeServer {
			server, err := newStubNodeServer(loopback, 0, name)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gg.DeferCleanup(server.close)
			return server
		}

		// nodeB is reachable only through the balancer, which alternates between nodeB and nodeC.
		seedNode := startNode("BB9A00000000000A")
		nodeB := startNode("BB9B00000000000B")
		nodeC := startNode("BB9C00000000000C")
		balancer := newRoundRobinProxy(loopback,
			net.JoinHostPort(localhost, strconv.Itoa(nodeB.port())),
			net.JoinHostPort(localhost, strconv.Itoa(nodeC.port())))
		gg.DeferCleanup(balancer.close)

		peers := peersFor(localhost,
			[]string{"BB9A00000000000A", "BB9B00000000000B", "BB9C00000000000C"},
			[]int{seedNode.port(), balancer.port(), nodeC.port()}, -1)
		for _, server := range []*stubNodeServer{seedNode, nodeB, nodeC} {
			server.setPeers(peers)
		}

		policy := NewClientPolicy()
		policy.Timeout = 3 * time.Second
		policy.TendInterval = 250 * time.Millisecond

		logs := captureWarnLogs()
		began := time.Now()
		client, err := NewClientWithPolicyAndHost(policy, NewHost(localhost, seedNode.port()))
		elapsed := time.Since(began)
		if client != nil {
			gg.DeferCleanup(client.Close)
		}

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(elapsed).To(gm.BeNumerically("<", policy.Timeout))
		gm.Expect(logs.snapshot()).To(gm.ContainElement(gm.ContainSubstring("Node count did not settle during cluster start")))
	})
})

// roundRobinProxy sends each new connection to the next target in turn.
type roundRobinProxy struct {
	listener net.Listener
	targets  []string

	mu    sync.Mutex
	next  int
	conns []net.Conn
}

func newRoundRobinProxy(ip net.IP, targets ...string) *roundRobinProxy {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	gm.Expect(err).ToNot(gm.HaveOccurred())
	proxy := &roundRobinProxy{listener: listener, targets: targets}
	go proxy.serve()
	return proxy
}

func (proxy *roundRobinProxy) port() int {
	return proxy.listener.Addr().(*net.TCPAddr).Port
}

func (proxy *roundRobinProxy) serve() {
	for {
		in, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		proxy.mu.Lock()
		target := proxy.targets[proxy.next%len(proxy.targets)]
		proxy.next++
		proxy.mu.Unlock()
		out, err := net.Dial("tcp", target)
		if err != nil {
			in.Close()
			continue
		}
		proxy.mu.Lock()
		proxy.conns = append(proxy.conns, in, out)
		proxy.mu.Unlock()
		go func() { io.Copy(out, in); out.Close() }()
		go func() { io.Copy(in, out); in.Close() }()
	}
}

func (proxy *roundRobinProxy) close() {
	proxy.listener.Close()
	proxy.mu.Lock()
	conns := proxy.conns
	proxy.conns = nil
	proxy.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}
