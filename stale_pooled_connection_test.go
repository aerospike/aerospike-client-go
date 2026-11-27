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

package aerospike_test

import (
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// cuttingProxy forwards connections to a node and cuts all of them on demand.
type cuttingProxy struct {
	listener net.Listener
	upstream string
	accepted atomic.Int64

	mu          sync.Mutex
	clientSides []*net.TCPConn
	nodeSides   []net.Conn
}

func newCuttingProxy(upstream string) *cuttingProxy {
	// listen on the local interface that routes to the upstream
	probe, err := net.Dial("tcp", upstream)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	local := probe.LocalAddr().(*net.TCPAddr).IP
	probe.Close()

	listener, err := net.Listen("tcp", net.JoinHostPort(local.String(), "0"))
	gm.Expect(err).ToNot(gm.HaveOccurred())

	proxy := &cuttingProxy{listener: listener, upstream: upstream}
	go proxy.serve()
	return proxy
}

func (proxy *cuttingProxy) host(tlsName string) *as.Host {
	addr := proxy.listener.Addr().(*net.TCPAddr)
	return &as.Host{Name: addr.IP.String(), Port: addr.Port, TLSName: tlsName}
}

func (proxy *cuttingProxy) port() int {
	return proxy.listener.Addr().(*net.TCPAddr).Port
}

func (proxy *cuttingProxy) serve() {
	for {
		accepted, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		clientSide := accepted.(*net.TCPConn)
		nodeSide, err := net.Dial("tcp", proxy.upstream)
		if err != nil {
			clientSide.Close()
			continue
		}

		proxy.mu.Lock()
		proxy.clientSides = append(proxy.clientSides, clientSide)
		proxy.nodeSides = append(proxy.nodeSides, nodeSide)
		proxy.mu.Unlock()
		proxy.accepted.Add(1)

		go func() {
			_, _ = io.Copy(nodeSide, clientSide)
			nodeSide.Close()
		}()
		go func() {
			_, _ = io.Copy(clientSide, nodeSide)
			clientSide.Close()
		}()
	}
}

// cutAll closes every connection the client holds through the proxy,
// with a TCP reset or with a graceful close.
func (proxy *cuttingProxy) cutAll(withReset bool) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()

	for _, clientSide := range proxy.clientSides {
		if withReset {
			_ = clientSide.SetLinger(0)
		}
		clientSide.Close()
	}
	for _, nodeSide := range proxy.nodeSides {
		nodeSide.Close()
	}
	proxy.clientSides = nil
	proxy.nodeSides = nil
}

func (proxy *cuttingProxy) close() {
	proxy.listener.Close()
	proxy.cutAll(false)
}

var _ = gg.Describe("Pooled connections cut while idle", func() {

	const pooledConnections = 3
	const followUps = 10

	var ns = *namespace
	var set = ""
	var proxy *cuttingProxy
	var pclient *as.Client
	var proxied *as.Node
	var key *as.Key

	noRetryPolicy := func() *as.BasePolicy {
		p := as.NewPolicy()
		p.TotalTimeout = 5 * time.Second
		p.SocketTimeout = 2 * time.Second
		p.MaxRetries = 0
		p.ReadModeSC = client.DefaultPolicy.ReadModeSC
		return p
	}

	expectReadWithoutRetry := func() {
		rec, err := pclient.Get(noRetryPolicy(), key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["idx"]).To(gm.Equal(1))
	}

	// parks connections in the proxied node's pool, cuts them, then reads
	expectReadsAfterCut := func(withReset bool) {
		pooled := make([]*as.Connection, 0, pooledConnections)
		for range pooledConnections {
			conn, err := proxied.GetConnection(time.Second)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			pooled = append(pooled, conn)
		}
		for _, conn := range pooled {
			proxied.PutConnection(conn)
		}

		acceptedBeforeCut := proxy.accepted.Load()
		proxy.cutAll(withReset)
		// let the close reach the client's sockets
		time.Sleep(200 * time.Millisecond)

		expectReadWithoutRetry()

		for _, conn := range pooled {
			gm.Expect(conn.IsConnected()).To(gm.BeFalse(), "a cut pooled connection was not discarded")
		}
		gm.Expect(proxy.accepted.Load()).To(gm.BeNumerically(">", acceptedBeforeCut), "no replacement connection was opened")

		for range followUps {
			expectReadWithoutRetry()
		}
	}

	gg.BeforeEach(func() {
		seed := dbHosts[0]
		proxy = newCuttingProxy(net.JoinHostPort(seed.Name, strconv.Itoa(seed.Port)))

		cp := *clientPolicy
		cp.MinConnectionsPerNode = 0
		cp.FailIfNotConnected = true
		// peers advertise addresses the test host can't reach, so seed the other nodes directly
		seeds := append([]*as.Host{proxy.host(seed.TLSName)}, dbHosts[1:]...)
		var err error
		pclient, err = as.NewClientWithPolicyAndHost(&cp, seeds...)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(pclient.GetNodes()).To(gm.HaveLen(len(client.GetNodes())))

		proxied = nil
		for _, n := range pclient.GetNodes() {
			if n.GetHost().Port == proxy.port() {
				proxied = n
			}
		}
		gm.Expect(proxied).ToNot(gm.BeNil())

		// pick a key the proxied node serves
		for key = nil; key == nil; {
			candidate, err := as.NewKey(ns, set, randString(20))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			ptn, err := as.PartitionForRead(pclient.Cluster(), noRetryPolicy(), candidate)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			node, err := ptn.GetNodeRead(pclient.Cluster())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			if node.GetName() == proxied.GetName() {
				key = candidate
			}
		}
		gm.Expect(client.PutBins(nil, key, as.NewBin("idx", 1))).To(gm.Succeed())

		expectReadWithoutRetry()
	})

	gg.AfterEach(func() {
		pclient.Close()
		proxy.close()
		_, _ = client.Delete(nil, key)
	})

	gg.It("must read without a retry after every pooled connection was reset", func() {
		expectReadsAfterCut(true)
	})

	gg.It("must read without a retry after every pooled connection was closed gracefully", func() {
		expectReadsAfterCut(false)
	})
})
