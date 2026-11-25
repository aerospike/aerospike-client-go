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
	"log"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go/v8"

	gm "github.com/onsi/gomega"
)

type testBLOB struct {
	name string
}

func (tb *testBLOB) EncodeBlob() ([]byte, error) {
	return []byte(tb.name), nil
}

// generates a random string of specified length
func randString(size int) string {
	const random_alpha_num = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const l = 62
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = random_alpha_num[rand.Intn(l)]
	}
	return string(buf)
}

func normalizeValue(v any, isMapKey bool) any {
	if v != nil {
		switch v.(type) {
		case int8, int16, int32, int, int64:
			return reflect.ValueOf(v).Int()
		case uint8, uint16, uint32, uint:
			return int64(reflect.ValueOf(v).Uint())
		}

		// check for array and map
		switch reflect.TypeOf(v).Kind() {
		case reflect.Array:
			if isMapKey {
				return v
			}
			return sliceToIfcSlice(v)
		case reflect.Slice:
			return sliceToIfcSlice(v)
		case reflect.Map:
			return mapToIfcMap(v)
		}

		return v
	}

	return nil
}

func mapToIfcMap(v any) map[any]any {
	s := reflect.ValueOf(v)
	l := s.Len()
	res := make(map[any]any, l)
	for _, k := range s.MapKeys() {
		v := s.MapIndex(k).Interface()
		res[normalizeValue(k.Interface(), true)] = normalizeValue(v, false)
	}

	return res
}

func sliceToIfcSlice(v any) []any {
	s := reflect.ValueOf(v)
	l := s.Len()
	res := make([]any, l)
	for i := 0; i < l; i++ {
		t := s.Index(i).Interface()
		res[i] = normalizeValue(t, false)
	}

	return res
}

func arraysEqual(ia, ib any) {
	a := sliceToIfcSlice(ia)
	b := sliceToIfcSlice(ib)

	gm.Expect(len(a)).To(gm.Equal(len(b)))
	// gm.Expect(a).To(gm.BeEquivalentTo(b))

	for i := range a {
		switch reflect.ValueOf(a[i]).Kind() {
		case reflect.Map:
			mapsEqual(a[i], b[i])
		case reflect.Slice:
			arraysEqual(a[i], b[i])
		default:
			if a[i] != nil {
				gm.Expect(a[i]).To(gm.BeEquivalentTo(b[i]))
			} else {
				gm.Expect(b[i]).To(gm.BeNil())
			}
		}
	}
}

func mapsEqual(ia, ib any) {
	a := mapToIfcMap(ia)
	b := mapToIfcMap(ib)

	gm.Expect(len(a)).To(gm.Equal(len(b)))
	gm.Expect(a).To(gm.BeEquivalentTo(b))
}

func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// pausableNodeProxy relays connections to one node. When armed, it stops passing
// on the node's next response on a data connection after passBytes of it, until
// resumed.
type pausableNodeProxy struct {
	ln       net.Listener
	upstream string

	mu             sync.Mutex
	armed          bool
	passBytes      int
	release        chan struct{}
	releaseOnce    *sync.Once
	pausedClosed   chan struct{}
	responsePaused atomic.Bool
	conns          []net.Conn
}

func newPausableNodeProxy(upstream string) *pausableNodeProxy {
	// listen on the local interface that routes to the upstream
	probe, err := net.Dial("tcp", upstream)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	local := probe.LocalAddr().(*net.TCPAddr).IP
	probe.Close()

	ln, err := net.Listen("tcp", net.JoinHostPort(local.String(), "0"))
	gm.Expect(err).ToNot(gm.HaveOccurred())

	p := &pausableNodeProxy{ln: ln, upstream: upstream}
	go p.serve()
	return p
}

func (p *pausableNodeProxy) host(tlsName string) *as.Host {
	addr := p.ln.Addr().(*net.TCPAddr)
	return &as.Host{Name: addr.IP.String(), Port: addr.Port, TLSName: tlsName}
}

func (p *pausableNodeProxy) port() int {
	return p.ln.Addr().(*net.TCPAddr).Port
}

func (p *pausableNodeProxy) serve() {
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

func (p *pausableNodeProxy) pipe(c, s net.Conn) {
	var isData atomic.Bool
	var closedOnce sync.Once
	var pausedClosed chan struct{}
	var pausedMu sync.Mutex

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
				pausedMu.Lock()
				if pausedClosed != nil {
					closedOnce.Do(func() { close(pausedClosed) })
				}
				pausedMu.Unlock()
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
					pausedMu.Lock()
					pausedClosed = p.pausedClosed
					pausedMu.Unlock()
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
					p.responsePaused.Store(true)
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

// pauseNextResponse arms the proxy and returns the channel closed once the client
// side of the paused connection closes.
func (p *pausableNodeProxy) pauseNextResponse(passBytes int) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
	p.passBytes = passBytes
	p.release = make(chan struct{})
	p.releaseOnce = new(sync.Once)
	p.pausedClosed = make(chan struct{})
	p.responsePaused.Store(false)
	return p.pausedClosed
}

func (p *pausableNodeProxy) resumeResponse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.release != nil {
		p.releaseOnce.Do(func() { close(p.release) })
	}
}

func (p *pausableNodeProxy) close() {
	p.ln.Close()
	p.resumeResponse()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = false
	for _, c := range p.conns {
		c.Close()
	}
}
