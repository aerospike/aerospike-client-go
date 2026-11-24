//go:build !as_performance

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
	"encoding/binary"
	"io"
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

// asMsgProtoType is the proto type of the messages that carry queries and scans.
const asMsgProtoType = 3

// requestDropper is a TCP relay between a client and one Aerospike node. It
// reads the proto framing of what the client sends, and closes the connection
// instead of forwarding an AS_MSG request while it is told to drop requests.
// The client sees the connection close while it waits for the reply, which is
// a retryable NETWORK_ERROR. Info and login requests always go through.
type requestDropper struct {
	listener net.Listener
	target   string

	// number of upcoming AS_MSG requests to drop; negative drops all of them
	toDrop atomic.Int64

	mu    sync.Mutex
	conns []net.Conn
}

func newRequestDropper(target string) *requestDropper {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	gm.Expect(err).ToNot(gm.HaveOccurred())

	rd := &requestDropper{listener: listener, target: target}
	go rd.acceptLoop()
	return rd
}

func (rd *requestDropper) host() *as.Host {
	addr := rd.listener.Addr().(*net.TCPAddr)
	return as.NewHost(addr.IP.String(), addr.Port)
}

// dropRequests makes the relay drop the next n AS_MSG requests. A negative n drops all of them.
func (rd *requestDropper) dropRequests(n int64) {
	rd.toDrop.Store(n)
}

func (rd *requestDropper) shouldDrop() bool {
	for {
		n := rd.toDrop.Load()
		if n == 0 {
			return false
		}
		if n < 0 {
			return true
		}
		if rd.toDrop.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

func (rd *requestDropper) acceptLoop() {
	for {
		conn, err := rd.listener.Accept()
		if err != nil {
			return
		}
		go rd.relay(conn)
	}
}

func (rd *requestDropper) relay(clientConn net.Conn) {
	defer clientConn.Close()

	serverConn, err := net.Dial("tcp", rd.target)
	if err != nil {
		return
	}
	defer serverConn.Close()

	rd.mu.Lock()
	rd.conns = append(rd.conns, clientConn, serverConn)
	rd.mu.Unlock()

	go func() {
		_, _ = io.Copy(clientConn, serverConn)
		clientConn.Close()
	}()

	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(clientConn, header); err != nil {
			return
		}

		// the proto header is the version, the type and a 6 byte size
		size := binary.BigEndian.Uint64(header) & 0x0000FFFFFFFFFFFF
		body := make([]byte, size)
		if _, err := io.ReadFull(clientConn, body); err != nil {
			return
		}

		if header[1] == asMsgProtoType && rd.shouldDrop() {
			return
		}

		if _, err := serverConn.Write(header); err != nil {
			return
		}
		if _, err := serverConn.Write(body); err != nil {
			return
		}
	}
}

func (rd *requestDropper) close() {
	rd.listener.Close()

	rd.mu.Lock()
	defer rd.mu.Unlock()
	for _, conn := range rd.conns {
		conn.Close()
	}
}

type retryTestObject struct {
	Bin int `as:"bin"`
}

// drainResults reads a Recordset until it ends, and returns the number of
// records and the errors it delivered.
func drainResults(rs *as.Recordset) (int, []as.Error) {
	records := 0
	var errs []as.Error
	for res := range rs.Results() {
		if res.Err != nil {
			errs = append(errs, res.Err)
			continue
		}
		records++
	}
	return records, errs
}

// expectNoErrors fails with the text of the errors, if there are any.
func expectNoErrors(errs []as.Error) {
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}
	gm.Expect(messages).To(gm.BeEmpty())
}

// expectFinalError fails unless the last error matches the result code.
func expectFinalError(errs []as.Error, code ast.ResultCode) {
	gm.Expect(errs).ToNot(gm.BeEmpty())

	final := errs[len(errs)-1]
	message := final.Error()
	matches := final.Matches(code)
	gm.Expect(matches).To(gm.BeTrue(), message)
}

// drainObjects reads the object channel of a Recordset until it is closed, then reads the
// errors the Recordset delivered.
func drainObjects(rs *as.Recordset, objChan chan *retryTestObject) (int, []as.Error) {
	objects := 0
	for range objChan {
		objects++
	}

	var errs []as.Error
	for err := range rs.Errors() {
		errs = append(errs, err)
	}
	return objects, errs
}

// The partition executors retry the partitions that failed with a retryable error.
// ALL tests are isolated by SetName and Key.
var _ = gg.Describe("Partition executor retries", func() {

	const keyCount = 100

	var ns = *namespace
	var set string
	var dropper *requestDropper
	var proxied *as.Client

	newQueryPolicy := func(maxRetries int) *as.QueryPolicy {
		policy := as.NewQueryPolicy()
		policy.MaxRetries = maxRetries
		policy.SleepBetweenRetries = 50 * time.Millisecond
		policy.TotalTimeout = 20 * time.Second
		return policy
	}

	newScanPolicy := func(maxRetries int) *as.ScanPolicy {
		policy := as.NewScanPolicy()
		policy.MaxRetries = maxRetries
		policy.SleepBetweenRetries = 50 * time.Millisecond
		policy.TotalTimeout = 20 * time.Second
		return policy
	}

	gg.BeforeEach(func() {
		if tlsConfig != nil {
			gg.Skip("the request dropper does not terminate TLS")
		}

		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, i)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(nil, key, as.NewBin("bin", i))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		seed := dbHosts[0]
		dropper = newRequestDropper(net.JoinHostPort(seed.Name, strconv.Itoa(seed.Port)))

		policy := as.NewClientPolicy()
		policy.User = clientPolicy.User
		policy.Password = clientPolicy.Password
		policy.AuthMode = clientPolicy.AuthMode

		var err error
		proxied, err = as.NewClientWithPolicyAndHost(policy, dropper.host())
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Pool a connection that goes through the dropper, so the first request
		// of a test is sent on it and not on a connection opened for the occasion.
		warmupKey, err := as.NewKey(ns, set, 0)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = proxied.Get(nil, warmupKey)
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		if proxied != nil {
			proxied.Close()
		}
		if dropper != nil {
			dropper.close()
		}
		client.Truncate(nil, ns, set, nil)
	})

	gg.Context("when an attempt fails with a retryable error and a retry completes every partition", func() {

		gg.It("must not report the error of the retried attempt at the end of a query", func() {
			dropper.dropRequests(1)

			rs, err := proxied.Query(newQueryPolicy(3), as.NewStatement(ns, set))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, errs := drainResults(rs)
			expectNoErrors(errs)
			gm.Expect(records).To(gm.Equal(keyCount))
		})

		gg.It("must report a query as done on its partition filter and not report the retried error", func() {
			dropper.dropRequests(1)

			filter := as.NewPartitionFilterAll()
			rs, err := proxied.QueryPartitions(newQueryPolicy(3), as.NewStatement(ns, set), filter)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, errs := drainResults(rs)
			expectNoErrors(errs)
			gm.Expect(records).To(gm.Equal(keyCount))
			gm.Expect(filter.IsDone()).To(gm.BeTrue())
		})

		gg.It("must not report the error of the retried attempt at the end of a scan", func() {
			dropper.dropRequests(1)

			rs, err := proxied.ScanAll(newScanPolicy(3), ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, errs := drainResults(rs)
			expectNoErrors(errs)
			gm.Expect(records).To(gm.Equal(keyCount))
		})
	})

	gg.Context("when every attempt fails with a retryable error and retries run out", func() {

		gg.It("must report MAX_RETRIES_EXCEEDED, with the errors of the attempts, at the end of a query", func() {
			dropper.dropRequests(-1)

			rs, err := proxied.Query(newQueryPolicy(2), as.NewStatement(ns, set))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, errs := drainResults(rs)
			expectFinalError(errs, ast.MAX_RETRIES_EXCEEDED)
			expectFinalError(errs, ast.NETWORK_ERROR)
		})

		gg.It("must report MAX_RETRIES_EXCEEDED at the end of a scan", func() {
			dropper.dropRequests(-1)

			rs, err := proxied.ScanAll(newScanPolicy(2), ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, errs := drainResults(rs)
			expectFinalError(errs, ast.MAX_RETRIES_EXCEEDED)
		})

		gg.It("must deliver the final error of QueryObjects on Errors()", func() {
			dropper.dropRequests(-1)

			objChan := make(chan *retryTestObject, keyCount)
			rs, err := proxied.QueryObjects(newQueryPolicy(2), as.NewStatement(ns, set), objChan)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			objects, errs := drainObjects(rs, objChan)
			gm.Expect(objects).To(gm.BeNumerically("<", keyCount))
			expectFinalError(errs, ast.MAX_RETRIES_EXCEEDED)
		})

		gg.It("must deliver the final error of QueryPartitionObjects on Errors() and leave its filter not done", func() {
			dropper.dropRequests(-1)

			filter := as.NewPartitionFilterAll()
			objChan := make(chan *retryTestObject, keyCount)
			rs, err := proxied.QueryPartitionObjects(newQueryPolicy(2), as.NewStatement(ns, set), objChan, filter)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, errs := drainObjects(rs, objChan)
			expectFinalError(errs, ast.MAX_RETRIES_EXCEEDED)
			gm.Expect(filter.IsDone()).To(gm.BeFalse())
		})

		gg.It("must deliver the final error of ScanAllObjects on Errors()", func() {
			dropper.dropRequests(-1)

			objChan := make(chan *retryTestObject, keyCount)
			rs, err := proxied.ScanAllObjects(newScanPolicy(2), objChan, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			objects, errs := drainObjects(rs, objChan)
			gm.Expect(objects).To(gm.BeNumerically("<", keyCount))
			expectFinalError(errs, ast.MAX_RETRIES_EXCEEDED)
		})
	})

	gg.Context("when an attempt fails with a retryable error and a retry completes every object", func() {

		gg.It("must complete QueryObjects without an error", func() {
			dropper.dropRequests(1)

			objChan := make(chan *retryTestObject, keyCount)
			rs, err := proxied.QueryObjects(newQueryPolicy(3), as.NewStatement(ns, set), objChan)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			objects, errs := drainObjects(rs, objChan)
			expectNoErrors(errs)
			gm.Expect(objects).To(gm.Equal(keyCount))
		})

		gg.It("must complete ScanAllObjects without an error", func() {
			dropper.dropRequests(1)

			objChan := make(chan *retryTestObject, keyCount)
			rs, err := proxied.ScanAllObjects(newScanPolicy(3), objChan, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			objects, errs := drainObjects(rs, objChan)
			expectNoErrors(errs)
			gm.Expect(objects).To(gm.Equal(keyCount))
		})
	})
})
