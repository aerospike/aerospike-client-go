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
	"net"
	"time"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// blockingCloseConn is a net.Conn whose Close blocks until release is closed.
type blockingCloseConn struct {
	net.Conn
	closeStarted chan struct{}
	release      chan struct{}
}

func newBlockingCloseConn() *blockingCloseConn {
	return &blockingCloseConn{
		closeStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (c *blockingCloseConn) Close() error {
	close(c.closeStarted)
	<-c.release
	return nil
}

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Connection Heap tests", func() {

	conn := new(Connection)

	gg.Context("singleConnectionHeap", func() {

		gg.It("Must add until full", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				gm.Expect(h.Len()).To(gm.Equal(i))
				gm.Expect(h.head).To(gm.Equal(uint32(i)))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
				gm.Expect(h.Offer(conn)).To(gm.BeTrue())
				gm.Expect(h.Len()).To(gm.Equal(i + 1))
				gm.Expect(h.head).To(gm.Equal(uint32(i+1) % 10))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
			}

			gm.Expect(h.Offer(conn)).To(gm.BeFalse())
			gm.Expect(h.Offer(conn)).To(gm.BeFalse())
			gm.Expect(h.Len()).To(gm.Equal(10))
			gm.Expect(h.full).To(gm.BeTrue())
			gm.Expect(h.head).To(gm.Equal(h.tail))
		})

		gg.It("Must add until full, then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				gm.Expect(h.Offer(conn)).To(gm.BeTrue())
			}

			for i := 0; i < 10; i++ {
				gm.Expect(h.Len()).To(gm.Equal(10 - i))
				gm.Expect(h.head).To(gm.Equal(uint32(10-i) % 10))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
				gm.Expect(h.Poll()).NotTo(gm.BeNil())
				gm.Expect(h.full).To(gm.BeFalse())
				gm.Expect(h.Len()).To(gm.Equal(10 - i - 1))
				gm.Expect(h.head).To(gm.Equal(uint32(10 - i - 1)))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
			}

			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Len()).To(gm.Equal(0))
			gm.Expect(h.full).To(gm.BeFalse())
			gm.Expect(h.head).To(gm.Equal(h.tail))
		})

		gg.It("Must add then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			gm.Expect(h.Offer(conn)).To(gm.BeTrue())
			gm.Expect(h.Len()).To(gm.Equal(1))
			gm.Expect(h.head).To(gm.Equal(uint32(1)))
			gm.Expect(h.full).To(gm.BeFalse())

			gm.Expect(h.Poll()).NotTo(gm.BeNil())
			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Len()).To(gm.Equal(0))
			gm.Expect(h.full).To(gm.BeFalse())
			gm.Expect(h.head).To(gm.Equal(uint32(0)))
			gm.Expect(h.head).To(gm.Equal(h.tail))

		})

		gg.It("Must not block DropIdleTail or the heap when closing an idle connection hangs", func() {
			netConn := newBlockingCloseConn()
			defer close(netConn.release)

			// a zero idleDeadline makes the connection idle immediately
			idleConn := &Connection{conn: netConn, idleTimeout: time.Millisecond}
			h := newSingleConnectionHeap(10)
			gm.Expect(h.Offer(idleConn)).To(gm.BeTrue())

			dropped := make(chan bool)
			go func() {
				dropped <- h.DropIdleTail()
			}()
			gm.Eventually(dropped).Should(gm.Receive(gm.BeTrue()))
			gm.Eventually(netConn.closeStarted).Should(gm.BeClosed())

			gm.Expect(h.Len()).To(gm.Equal(0))
			gm.Expect(h.Offer(conn)).To(gm.BeTrue())
			gm.Expect(h.Poll()).To(gm.BeIdenticalTo(conn))
		})

		gg.It("Must release the heap lock before closing connections in cleanup", func() {
			netConn := newBlockingCloseConn()
			defer close(netConn.release)

			pooledConn := &Connection{conn: netConn}
			h := newSingleConnectionHeap(10)
			gm.Expect(h.Offer(pooledConn)).To(gm.BeTrue())

			go h.cleanup()
			gm.Eventually(netConn.closeStarted).Should(gm.BeClosed())

			offered := make(chan bool)
			go func() {
				offered <- h.Offer(conn)
			}()
			gm.Eventually(offered).Should(gm.Receive(gm.BeFalse()))
			gm.Expect(h.Poll()).To(gm.BeNil())
		})

	})
})
