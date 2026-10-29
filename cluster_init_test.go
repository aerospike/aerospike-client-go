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
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	ast "github.com/aerospike/aerospike-client-go/v7/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// delayProxy forwards TCP connections to a server and delays server responses,
// so that the initial cluster tend outlasts ClientPolicy.Timeout.
//
// The first connection is the seed validation connection, which has to finish all of its
// requests within ClientPolicy.Timeout / 2, so its responses are not delayed.
type delayProxy struct {
	listener net.Listener
	target   string
	delay    time.Duration

	accepted atomic.Int64

	// Number of connections currently open to the server through the proxy.
	open atomic.Int64
}

func newDelayProxy(target string, delay time.Duration) *delayProxy {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	gm.Expect(err).NotTo(gm.HaveOccurred())

	proxy := &delayProxy{listener: listener, target: target, delay: delay}
	go proxy.serve()
	return proxy
}

func (p *delayProxy) serve() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.forward(clientConn)
	}
}

func (p *delayProxy) forward(clientConn net.Conn) {
	serverConn, err := net.Dial("tcp", p.target)
	if err != nil {
		clientConn.Close()
		return
	}

	p.open.Add(1)
	defer p.open.Add(-1)

	delay := p.delay
	if p.accepted.Add(1) == 1 {
		delay = 0
	}

	done := make(chan struct{}, 2)
	go func() {
		io.Copy(serverConn, clientConn)
		done <- struct{}{}
	}()
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := serverConn.Read(buf)
			if n > 0 {
				time.Sleep(delay)
				if _, werr := clientConn.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	<-done
	clientConn.Close()
	serverConn.Close()
	<-done
}

func (p *delayProxy) host() *as.Host {
	addr := p.listener.Addr().(*net.TCPAddr)
	return as.NewHost(addr.IP.String(), addr.Port)
}

var _ = gg.Describe("Cluster initialization", func() {

	const timeout = 500 * time.Millisecond

	var proxy *delayProxy
	var cpolicy as.ClientPolicy

	gg.BeforeEach(func() {
		target := dbHosts[0].String()
		// Each info request on the tend connection still finishes within ClientPolicy.Timeout,
		// but the initial tend needs at least two of them in sequence.
		responseDelay := timeout * 3 / 5
		proxy = newDelayProxy(target, responseDelay)

		cpolicy = *clientPolicy
		cpolicy.Timeout = timeout
		cpolicy.MinConnectionsPerNode = 5
		// Keep every connection on the proxy instead of the addresses the server advertises.
		cpolicy.SeedOnlyCluster = true
	})

	gg.AfterEach(func() {
		proxy.listener.Close()
	})

	expectNoOpenConnections := func() {
		gm.Eventually(proxy.open.Load, 5*time.Second, 10*time.Millisecond).Should(gm.BeZero())
		gm.Consistently(proxy.open.Load, time.Second, 10*time.Millisecond).Should(gm.BeZero())
	}

	newProxiedClient := func() (*as.Client, as.Error) {
		host := proxy.host()
		host.TLSName = dbHosts[0].TLSName
		return as.NewClientWithPolicyAndHost(&cpolicy, host)
	}

	gg.It("must not leave connections open when FailIfNotConnected is true and the initial tend times out", func() {
		cpolicy.FailIfNotConnected = true

		nclient, err := newProxiedClient()
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.TIMEOUT)).To(gm.BeTrue())
		gm.Expect(nclient).To(gm.BeNil())

		expectNoOpenConnections()
	})

	gg.It("must close all connections on Close when FailIfNotConnected is false and the initial tend times out", func() {
		cpolicy.FailIfNotConnected = false

		nclient, err := newProxiedClient()
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.TIMEOUT)).To(gm.BeTrue())
		gm.Expect(nclient).NotTo(gm.BeNil())

		nclient.Close()

		expectNoOpenConnections()
	})

	gg.It("must finish connecting in the background when FailIfNotConnected is false and the initial tend times out", func() {
		cpolicy.FailIfNotConnected = false

		nclient, err := newProxiedClient()
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.TIMEOUT)).To(gm.BeTrue())
		gm.Expect(nclient).NotTo(gm.BeNil())

		gm.Eventually(nclient.IsConnected, 10*time.Second, 10*time.Millisecond).Should(gm.BeTrue())
		nodes := nclient.GetNodes()
		gm.Expect(nodes).To(gm.HaveLen(1))
		minOpen := int64(cpolicy.MinConnectionsPerNode)
		gm.Eventually(proxy.open.Load, 10*time.Second, 10*time.Millisecond).Should(gm.BeNumerically(">=", minOpen))

		nclient.Close()

		expectNoOpenConnections()
	})
})
