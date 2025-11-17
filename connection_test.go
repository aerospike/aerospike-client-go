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
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Connection Test", func() {
	var conn *as.Connection

	type testExpectations struct {
		totalTimeout, socketTimeout time.Duration
		expTotalDeadline            time.Time
		expSocketDeadline           time.Time
		expSocketTimeout            time.Duration
	}

	gg.BeforeEach(func() {
		var err as.Error
		conn, err = as.NewConnection(clientPolicy, dbHosts[0])
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(conn).ToNot(gm.BeNil())
	})

	gg.It("Deadlines should be calculated correctly", func() {
		deadline := func(timeout time.Duration) (res time.Time) {
			if timeout > 0 {
				res = time.Now().Add(timeout)
			}
			return res
		}

		testMatrix := []testExpectations{
			{0, 0, time.Time{}, time.Now().Add(as.DefaultTimeout()), as.DefaultTimeout()},
			{0, time.Second, time.Time{}, time.Now().Add(time.Second), time.Second},
			{time.Second, 0, time.Now().Add(time.Second), time.Now().Add(time.Second), time.Second},
			{5 * time.Second, time.Second, time.Now().Add(5 * time.Second), time.Now().Add(time.Second), time.Second},
		}

		for _, matrix := range testMatrix {
			gg.By(fmt.Sprintf("TotalTimeout: %v, SocketTimeout: %v", matrix.totalTimeout, matrix.socketTimeout))
			err := conn.SetTimeout(deadline(matrix.totalTimeout), matrix.socketTimeout)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			expTotalDeadline, expSocketDeadline, expSocketTimeout, err := conn.UpdateDeadline()
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gg.By(fmt.Sprintf("expTotalDeadline: %v, expSocketDeadline: %v, expSocketTimeout: %v", matrix.expTotalDeadline, matrix.expSocketDeadline, matrix.expSocketTimeout))

			gm.Expect(expTotalDeadline).To(gm.BeTemporally("~", matrix.expTotalDeadline, 2*time.Millisecond))
			gm.Expect(expSocketDeadline).To(gm.BeTemporally("~", matrix.expSocketDeadline, 2*time.Millisecond))
			gm.Expect(expSocketTimeout).To(gm.BeNumerically("~", matrix.expSocketTimeout, 2*time.Millisecond))
		}
	})

})
