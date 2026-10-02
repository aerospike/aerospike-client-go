// Copyright 2014-2022 Aerospike, Inc.
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
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// holdProxy forwards client->server bytes immediately. When armed, it holds the
// next server->client response on a data connection after forwarding passBytes of it.
type holdProxy struct {
	ln       net.Listener
	upstream string

	mu          sync.Mutex
	armed       bool
	passBytes   int
	release     chan struct{}
	releaseOnce *sync.Once
	heldClosed  chan struct{}
	held        atomic.Bool
	conns       []net.Conn
}

func newHoldProxy(upstream string) *holdProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	gm.Expect(err).ToNot(gm.HaveOccurred())

	p := &holdProxy{ln: ln, upstream: upstream}
	go p.serve()
	return p
}

func (p *holdProxy) port() int {
	return p.ln.Addr().(*net.TCPAddr).Port
}

func (p *holdProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		s, err := net.Dial("tcp", p.upstream)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, s)
		p.mu.Unlock()
		go p.pipe(c, s)
	}
}

func (p *holdProxy) pipe(c, s net.Conn) {
	var isData atomic.Bool
	var closedOnce sync.Once
	var heldClosed chan struct{}
	var heldMu sync.Mutex

	go func() {
		defer s.Close()
		buf := make([]byte, 64*1024)
		for {
			n, err := c.Read(buf)
			if n >= 2 && (buf[1] == 3 || buf[1] == 4) {
				isData.Store(true)
			}
			if n > 0 {
				if _, werr := s.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				heldMu.Lock()
				if heldClosed != nil {
					closedOnce.Do(func() { close(heldClosed) })
				}
				heldMu.Unlock()
				return
			}
		}
	}()

	defer c.Close()
	buf := make([]byte, 64*1024)
	for {
		n, err := s.Read(buf)
		if n > 0 {
			out := buf[:n]
			if isData.Load() {
				p.mu.Lock()
				armed, pass, release := p.armed, p.passBytes, p.release
				if armed {
					p.armed = false
					heldMu.Lock()
					heldClosed = p.heldClosed
					heldMu.Unlock()
				}
				p.mu.Unlock()

				if armed {
					if pass > len(out) {
						pass = len(out)
					}
					if _, werr := c.Write(out[:pass]); werr != nil {
						return
					}
					out = out[pass:]
					p.held.Store(true)
					<-release
				}
			}
			if _, werr := c.Write(out); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// holdNext arms the proxy and returns the channel closed once the client side of
// the held connection closes.
func (p *holdProxy) holdNext(passBytes int) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
	p.passBytes = passBytes
	p.release = make(chan struct{})
	p.releaseOnce = new(sync.Once)
	p.heldClosed = make(chan struct{})
	p.held.Store(false)
	return p.heldClosed
}

func (p *holdProxy) releaseHeld() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.release != nil {
		p.releaseOnce.Do(func() { close(p.release) })
	}
}

func (p *holdProxy) close() {
	p.ln.Close()
	p.releaseHeld()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = false
	for _, c := range p.conns {
		c.Close()
	}
}

var _ = gg.Describe("Connection salvage after a timeout", func() {

	const followUps = 6

	var ns = *namespace
	var set = ""
	var proxy *holdProxy
	var pclient *as.Client
	var keys []*as.Key

	readPolicy := func(timeoutDelay time.Duration) *as.BasePolicy {
		p := as.NewPolicy()
		p.TotalTimeout = 5 * time.Second
		p.SocketTimeout = 2 * time.Second
		p.TimeoutDelay = timeoutDelay
		p.MaxRetries = 2
		p.ReadModeSC = client.DefaultPolicy.ReadModeSC
		return p
	}

	timeoutPolicy := func(timeoutDelay time.Duration, maxRetries int) *as.BasePolicy {
		p := as.NewPolicy()
		p.TotalTimeout = 300 * time.Millisecond
		p.SocketTimeout = 100 * time.Millisecond
		p.TimeoutDelay = timeoutDelay
		p.MaxRetries = maxRetries
		p.ReadModeSC = client.DefaultPolicy.ReadModeSC
		return p
	}

	recovered := func() float64 {
		stats, err := pclient.Stats()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return stats["cluster-aggregated-stats"].(map[string]any)["connections-recovered"].(float64)
	}

	isClosed := func(ch <-chan struct{}) func() bool {
		return func() bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		}
	}

	expectOwnRecords := func(timeoutDelay time.Duration) {
		for i := 1; i <= followUps; i++ {
			rec, err := pclient.Get(readPolicy(timeoutDelay), keys[i])
			if err != nil {
				continue
			}
			gm.Expect(rec.Bins["idx"]).To(gm.Equal(i), "read %d returned the record of another command", i)
		}
	}

	timeOutRead := func(policy *as.BasePolicy) {
		_, err := pclient.Get(policy, keys[0])
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.TIMEOUT)).To(gm.BeTrue(), err.Error())
		gm.Eventually(proxy.held.Load).Should(gm.BeTrue())
	}

	gg.BeforeEach(func() {
		seed := dbHosts[0]
		proxy = newHoldProxy(net.JoinHostPort(seed.Name, strconv.Itoa(seed.Port)))

		cp := *clientPolicy
		// one slot for the tend connection, one for commands
		cp.ConnectionQueueSize = 2
		cp.MinConnectionsPerNode = 0
		cp.FailIfNotConnected = true
		var err error
		pclient, err = as.NewClientWithPolicyAndHost(&cp, &as.Host{Name: "127.0.0.1", Port: proxy.port(), TLSName: seed.TLSName})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		pclient.EnableMetrics(nil)

		var proxied *as.Node
		for _, n := range pclient.GetNodes() {
			if n.GetHost().Port == proxy.port() {
				proxied = n
			}
		}
		gm.Expect(proxied).ToNot(gm.BeNil())

		keys = keys[:0]
		for i := 0; len(keys) <= followUps; i++ {
			key, err := as.NewKey(ns, set, randString(20))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			ptn, err := as.PartitionForRead(pclient.Cluster(), readPolicy(0), key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			node, err := ptn.GetNodeRead(pclient.Cluster())
			gm.Expect(err).ToNot(gm.HaveOccurred())
			if node.GetName() != proxied.GetName() {
				continue
			}
			gm.Expect(client.PutBins(nil, key, as.NewBin("idx", len(keys)), as.NewBin("pad", randString(512)))).To(gm.Succeed())
			keys = append(keys, key)
		}

		rec, err := pclient.Get(readPolicy(0), keys[0])
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["idx"]).To(gm.Equal(0))
	})

	gg.AfterEach(func() {
		pclient.Close()
		proxy.close()
	})

	gg.It("must close, not pool, a connection whose read timed out before the response header arrived", func() {
		before := recovered()

		closed := proxy.holdNext(0)
		timeOutRead(timeoutPolicy(2*time.Second, 0))
		proxy.releaseHeld()
		time.Sleep(300 * time.Millisecond)

		expectOwnRecords(2 * time.Second)

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})

	gg.It("must salvage a connection when the outstanding response is fully drained", func() {
		before := recovered()

		closed := proxy.holdNext(8)
		timeOutRead(timeoutPolicy(3*time.Second, 1))
		proxy.releaseHeld()

		gm.Eventually(recovered, 2*time.Second).Should(gm.Equal(before + 1))
		gm.Expect(isClosed(closed)()).To(gm.BeFalse())

		expectOwnRecords(3 * time.Second)
		gm.Expect(isClosed(closed)()).To(gm.BeFalse())
	})

	gg.It("must close the connection when the drain deadline expires", func() {
		before := recovered()

		closed := proxy.holdNext(8)
		timeOutRead(timeoutPolicy(300*time.Millisecond, 0))

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		proxy.releaseHeld()

		expectOwnRecords(300 * time.Millisecond)
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})

	gg.It("must close the connection without salvaging when TimeoutDelay is 0", func() {
		before := recovered()

		closed := proxy.holdNext(0)
		timeOutRead(timeoutPolicy(0, 0))

		gm.Eventually(isClosed(closed), 2*time.Second).Should(gm.BeTrue())
		proxy.releaseHeld()

		expectOwnRecords(0)
		gm.Consistently(recovered, 500*time.Millisecond).Should(gm.Equal(before))
	})
})
