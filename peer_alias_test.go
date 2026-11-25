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
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Peer address answering as another node", gg.Label("discovery"), func() {
	const (
		tendInterval = 250 * time.Millisecond
		joinBound    = 5 * time.Second
		settle       = 12 * tendInterval
		nodeA        = "BB9A00000000000A"
		nodeB        = "BB9B00000000000B"
		nodeC        = "BB9C00000000000C"
		nodeD        = "BB9D00000000000D"
		nodeX        = "BB9E00000000000E"
	)

	var (
		loopback   = net.ParseIP("127.0.0.1")
		servers    []*stubNodeServer
		proxies    []*switchProxy
		client     *Client
		origLookup = lookupHost
		hosts      map[string][]string
		lookupMu   sync.Mutex
		logs       *warnLogCapture
		ipMap      map[string]string
	)

	gg.BeforeEach(func() {
		servers = nil
		proxies = nil
		client = nil
		hosts = map[string][]string{}
		ipMap = nil
		lookupHost = func(name string) ([]string, error) {
			lookupMu.Lock()
			addrs := hosts[name]
			lookupMu.Unlock()
			if addrs != nil {
				return append([]string(nil), addrs...), nil
			}
			return origLookup(name)
		}
		logs = captureWarnLogs()
	})

	gg.AfterEach(func() {
		lookupHost = origLookup
		if client != nil {
			client.Close()
		}
		for _, proxy := range proxies {
			proxy.close()
		}
		for _, server := range servers {
			server.close()
		}
	})

	alias := func(prefix string) string {
		name := fmt.Sprintf("%s-%d.alias.test", prefix, time.Now().UnixNano())
		lookupMu.Lock()
		hosts[name] = []string{loopback.String()}
		lookupMu.Unlock()
		return name
	}

	start := func(name string) *stubNodeServer {
		server, err := newStubNodeServer(loopback, 0, name)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		servers = append(servers, server)
		return server
	}

	addr := func(host string, port int) string {
		return host + ":" + strconv.Itoa(port)
	}

	local := func(server *stubNodeServer) string {
		return addr(loopback.String(), server.port())
	}

	type entry struct {
		name  string
		hosts []string
	}

	peersOf := func(self string, entries ...entry) string {
		var parts []string
		for _, e := range entries {
			if e.name != self {
				parts = append(parts, "["+e.name+",,["+strings.Join(e.hosts, ",")+"]]")
			}
		}
		return "1,3000,[" + strings.Join(parts, ",") + "]"
	}

	advertise := func(entries ...entry) {
		for _, server := range servers {
			server.setPeers(peersOf(server.nodeName, entries...))
		}
	}

	connect := func(seed *stubNodeServer) *Client {
		policy := NewClientPolicy()
		policy.Timeout = 500 * time.Millisecond
		policy.TendInterval = tendInterval
		policy.IpMap = ipMap
		opened, err := NewClientWithPolicyAndHost(policy, NewHost(loopback.String(), seed.port()))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client = opened
		return opened
	}

	names := func(c *Client) []string {
		var out []string
		for _, node := range c.GetNodes() {
			out = append(out, node.GetName())
		}
		sort.Strings(out)
		return out
	}

	warned := func(parts ...string) bool {
		logs.mu.Lock()
		defer logs.mu.Unlock()
		for _, msg := range logs.msgs {
			all := true
			for _, part := range parts {
				if !strings.Contains(msg, part) {
					all = false
					break
				}
			}
			if all {
				return true
			}
		}
		return false
	}

	gg.It("(a) creates both nodes when a peer lists an alias of a known node first", func() {
		a := start(nodeA)
		b := start(nodeB)
		bAlias := start(nodeB)
		d := start(nodeD)
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeD, []string{local(bAlias), local(d)}},
		)

		opened := connect(a)
		gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
		gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
		gm.Expect(nodeNamed(opened, nodeD).host.Port).To(gm.Equal(d.port()))
		gm.Expect(warned("Skip address")).To(gm.BeFalse(), "an untranslated shared address logs at INFO: %v", logs.msgs)
		gm.Expect(warned("TRANSLATION_COLLISION")).To(gm.BeFalse(), "warnings: %v", logs.msgs)
	})

	gg.It("(b) stops a peer that lists a known node's socket under another name first", func() {
		a := start(nodeA)
		b := start(nodeB)
		d := start(nodeD)
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
		)

		opened := connect(a)
		gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB}))
		live := nodeNamed(opened, nodeB)

		bAlias := addr(alias("b"), b.port())
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeD, []string{bAlias, local(d)}},
		)

		gm.Eventually(func() bool {
			return warned("TRANSLATION_COLLISION", "left node "+nodeD, bAlias+" is used by "+nodeB, "Not tried: "+local(d))
		}, joinBound, 20*time.Millisecond).Should(gm.BeTrue(), "warnings: %v", logs.msgs)
		gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB}))
		gm.Expect(nodeNamed(opened, nodeB)).To(gm.BeIdenticalTo(live))
		gm.Expect(live.IsActive()).To(gm.BeTrue())
	})

	gg.It("(c) keeps the known node and does not create a peer whose only address answers as it", func() {
		a := start(nodeA)
		b := start(nodeB)
		bAlias := start(nodeB)
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeD, []string{local(bAlias)}},
		)

		opened := connect(a)
		gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB}))
		gm.Consistently(func() []string { return names(opened) }, settle, 50*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB}))
		gm.Expect(nodeNamed(opened, nodeA).IsActive()).To(gm.BeTrue())
		gm.Expect(nodeNamed(opened, nodeB).IsActive()).To(gm.BeTrue())
		gm.Expect(warned("TRANSLATION_COLLISION", nodeD, nodeB, local(bAlias), "Address translator static-map")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
		gm.Expect(warned("Skip address")).To(gm.BeFalse(), "an untranslated shared address logs at INFO: %v", logs.msgs)
	})

	gg.It("(d) creates an unadvertised node under the name it answers with", func() {
		a := start(nodeA)
		x := start(nodeX)
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeD, []string{local(x)}},
		)

		opened := connect(a)
		gm.Eventually(func() *Node { return nodeNamed(opened, nodeX) }, joinBound, 20*time.Millisecond).
			ShouldNot(gm.BeNil())
		gm.Expect(nodeNamed(opened, nodeD)).To(gm.BeNil())
		gm.Expect(warned("Peer node `"+nodeD+"` is different than actual node `"+nodeX+"`")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
	})

	gg.It("(f) warns when the translator produced the shared address", func() {
		a := start(nodeA)
		b := start(nodeB)
		bAlias := start(nodeB)
		d := start(nodeD)
		ipMap = map[string]string{"10.255.0.4": loopback.String()}
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeD, []string{addr("10.255.0.4", bAlias.port()), local(d)}},
		)

		opened := connect(a)
		gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeB, nodeD}))
		gm.Expect(warned("Skip address "+local(bAlias)+" of node "+nodeD+" because node "+nodeB+" also has it. Address translator static-map")).To(gm.BeTrue(), "warnings: %v", logs.msgs)
		gm.Expect(warned("TRANSLATION_COLLISION")).To(gm.BeFalse(), "warnings: %v", logs.msgs)
	})

	gg.It("(g) reports a collision naming the translator when a translated peer has no address left", func() {
		a := start(nodeA)
		b := start(nodeB)
		bAlias := start(nodeB)
		ipMap = map[string]string{"10.255.0.4": loopback.String()}
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeD, []string{addr("10.255.0.4", bAlias.port())}},
		)

		opened := connect(a)
		gm.Eventually(func() bool {
			return warned("TRANSLATION_COLLISION", nodeD, nodeB, local(bAlias), "Address translator static-map")
		}, joinBound, 20*time.Millisecond).Should(gm.BeTrue(), "warnings: %v", logs.msgs)
		gm.Expect(names(opened)).To(gm.Equal([]string{nodeA, nodeB}))
	})

	gg.It("(e) keeps a collision when the socket already holds a node with another name", func() {
		a := start(nodeA)
		b := start(nodeB)
		c := start(nodeC)
		d := start(nodeD)
		lb := newSwitchProxy(loopback, local(c))
		proxies = append(proxies, lb)
		lbAddr := addr(loopback.String(), lb.port())
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeC, []string{lbAddr}},
		)

		opened := connect(a)
		gm.Eventually(func() []string { return names(opened) }, joinBound, 20*time.Millisecond).
			Should(gm.Equal([]string{nodeA, nodeC}))
		live := nodeNamed(opened, nodeC)
		gm.Expect(live.host.Port).To(gm.Equal(lb.port()))

		lb.switchTo(local(b))
		advertise(
			entry{nodeA, []string{local(a)}},
			entry{nodeB, []string{local(b)}},
			entry{nodeC, []string{lbAddr}},
			entry{nodeD, []string{addr(alias("lb"), lb.port()), local(d)}},
		)

		gm.Eventually(func() *Node { return nodeNamed(opened, nodeB) }, joinBound, 20*time.Millisecond).
			ShouldNot(gm.BeNil())
		gm.Consistently(func() *Node { return nodeNamed(opened, nodeD) }, settle, 50*time.Millisecond).
			Should(gm.BeNil())
		gm.Expect(nodeNamed(opened, nodeC)).To(gm.BeIdenticalTo(live))
		gm.Expect(live.IsActive()).To(gm.BeTrue())
	})
})

// switchProxy forwards each new connection to the current target; open connections keep theirs.
type switchProxy struct {
	listener net.Listener

	mu     sync.Mutex
	target string
	conns  []net.Conn
}

func newSwitchProxy(ip net.IP, target string) *switchProxy {
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	gm.Expect(err).ToNot(gm.HaveOccurred())
	proxy := &switchProxy{listener: listener, target: target}
	go proxy.serve()
	return proxy
}

func (proxy *switchProxy) port() int {
	return proxy.listener.Addr().(*net.TCPAddr).Port
}

func (proxy *switchProxy) switchTo(target string) {
	proxy.mu.Lock()
	proxy.target = target
	proxy.mu.Unlock()
}

func (proxy *switchProxy) serve() {
	for {
		in, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		proxy.mu.Lock()
		target := proxy.target
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

func (proxy *switchProxy) close() {
	proxy.listener.Close()
	proxy.mu.Lock()
	conns := proxy.conns
	proxy.conns = nil
	proxy.mu.Unlock()
	for _, conn := range conns {
		conn.Close()
	}
}
