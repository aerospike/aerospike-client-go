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
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

var _ = gg.Describe("Seed validation against a scripted server", gg.Label("discovery"), func() {

	var server *fakeInfoServer

	gg.BeforeEach(func() {
		var err error
		server, err = newFakeInfoServer()
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		server.close()
	})

	newValidationCluster := func(policy *ClientPolicy) *Cluster {
		policy.Timeout = 2 * time.Second

		policyPointer := &atomic.Pointer[ClientPolicy]{}
		policyPointer.Store(policy)

		translator := policy.AddressTranslator
		if translator == nil {
			translator = NewStaticMapAddressTranslator(policy.IpMap)
		}

		return &Cluster{
			clientPolicy:      policyPointer,
			infoPolicy:        InfoPolicy{Timeout: policy.Timeout},
			addressTranslator: translator,
			translatorName:    translatorDisplayName(translator),
			translationOnMiss: policy.TranslationOnMiss,
		}
	}

	gg.It("fails a seed with no alternate address when UseServicesAlternate is set", func() {
		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "")

		validator := &nodeValidator{}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Error()).To(gm.ContainSubstring("UseServicesAlternate"))
		gm.Expect(err.Error()).To(gm.ContainSubstring("FAKE000000000001"))
	})

	gg.It("keeps the seed itself when its advertised service address is a strict miss", func() {
		translator, terr := NewPortMapAddressTranslator(map[string]string{"10.9.9.9:999": "unused.example.com:1"})
		gm.Expect(terr).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		policy.AddressTranslator = translator
		policy.TranslationOnMiss = TranslationMissStrict
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "10.88.0.7:3100")

		validator := &nodeValidator{}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(validator.name).To(gm.Equal("FAKE000000000001"))
		gm.Expect(validator.primaryHost.Name).To(gm.Equal(server.host().Name))
		gm.Expect(validator.primaryHost.Port).To(gm.Equal(server.host().Port))
	})

	gg.It("replaces a load balancer seed with its translated advertised address", func() {
		serverAddress := server.host()
		translator, terr := NewPortMapAddressTranslator(map[string]string{
			"10.88.0.7:3100": serverAddress.Name + ":" + strconv.Itoa(serverAddress.Port),
		})
		gm.Expect(terr).ToNot(gm.HaveOccurred())

		policy := NewClientPolicy()
		policy.UseServicesAlternate = true
		policy.AddressTranslator = translator
		cluster := newValidationCluster(policy)
		server.setReply("service-clear-alt", "10.88.0.7:3100")

		validator := &nodeValidator{detectLoadBalancer: true}
		err := validator.validateNode(cluster, server.host())

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(validator.primaryHost.Name).To(gm.Equal(serverAddress.Name))
		gm.Expect(validator.primaryHost.Port).To(gm.Equal(serverAddress.Port))
	})
})

// fakeInfoServer is a loopback TCP server speaking the info protocol. Tests
// script its answers per info command without a real cluster.
type fakeInfoServer struct {
	listener net.Listener

	repliesLock sync.Mutex
	replies     map[string]string
}

func newFakeInfoServer() (*fakeInfoServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &fakeInfoServer{
		listener: listener,
		replies: map[string]string{
			"partition-generation": "1",
			"peers-generation":     "1",
			"build":                "8.1.0.0",
		},
	}
	go server.serve()
	return server, nil
}

func (server *fakeInfoServer) host() *Host {
	address := server.listener.Addr().(*net.TCPAddr)
	return NewHost(address.IP.String(), address.Port)
}

func (server *fakeInfoServer) setReply(command, reply string) {
	server.repliesLock.Lock()
	server.replies[command] = reply
	server.repliesLock.Unlock()
}

func (server *fakeInfoServer) reply(command string) (string, bool) {
	server.repliesLock.Lock()
	defer server.repliesLock.Unlock()
	reply, exists := server.replies[command]
	return reply, exists
}

func (server *fakeInfoServer) close() {
	server.listener.Close()
}

func (server *fakeInfoServer) serve() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		go server.serveConnection(conn)
	}
}

func (server *fakeInfoServer) serveConnection(conn net.Conn) {
	defer conn.Close()

	for {
		var header types.MessageHeader
		if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
			return
		}

		body := make([]byte, header.Length())
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		var response bytes.Buffer
		for _, command := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if command == "" {
				continue
			}
			if reply, exists := server.reply(command); exists {
				response.WriteString(command + "\t" + reply + "\n")
			} else if command == "node" {
				response.WriteString("node\tFAKE000000000001\n")
			}
		}

		message := types.NewMessage(types.MSG_INFO, response.Bytes())
		serialized, err := message.Serialize()
		if err != nil {
			return
		}
		if _, err := conn.Write(serialized); err != nil {
			return
		}
	}
}
