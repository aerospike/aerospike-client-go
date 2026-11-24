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
	"reflect"
	"time"

	"github.com/aerospike/aerospike-client-go/v6/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Recordset test", func() {

	gg.It("must avoid panic on sendError", func() {
		rs := newRecordset(100, 1)

		rs.sendError(newError(types.PARAMETER_ERROR, "Error"))
		rs.wgGoroutines.Done()
		rs.Close()
		rs.sendError(newError(types.PARAMETER_ERROR, "Error"))

		timeout := time.After(time.Second)
		select {
		case res := <-rs.Results():
			gm.Expect(res).ToNot(gm.BeNil())
		case <-timeout:
			panic("wrong result!")
		}
	})

	gg.Context("when the caller reads only the object channel", func() {

		type recordsetTestObject struct {
			Bin int
		}

		// Object recordsets get an errors channel with room for a single error.
		newObjectRecordset := func() (*Recordset, chan *recordsetTestObject) {
			objChan := make(chan *recordsetTestObject)
			rs := &Recordset{
				objectset: *newObjectset(reflect.ValueOf(objChan), 1),
			}
			return rs, objChan
		}

		gg.It("must not block sendError, and must keep every error, once the errors channel is full", func() {
			rs, objChan := newObjectRecordset()

			codes := []types.ResultCode{types.PARAMETER_ERROR, types.NETWORK_ERROR, types.TIMEOUT}

			// the executor sends the errors of the node commands, and then signals the end
			sent := make(chan struct{})
			go func() {
				defer close(sent)
				for _, code := range codes {
					err := newError(code, "Error")
					rs.sendError(err)
				}
				rs.signalEnd()
			}()

			gm.Eventually(sent).WithTimeout(5 * time.Second).Should(gm.BeClosed())

			// signalEnd needs chanLock to close the channels
			_, open := <-objChan
			gm.Expect(open).To(gm.BeFalse())

			var errs []Error
			for err := range rs.Errors() {
				errs = append(errs, err)
			}

			gm.Expect(errs).ToNot(gm.BeEmpty())
			for _, code := range codes {
				found := false
				for _, err := range errs {
					found = found || err.Matches(code)
				}
				gm.Expect(found).To(gm.BeTrue(), "missing error with result code %s", code)
			}
		})

		gg.It("must deliver the errors one by one to a caller who reads them", func() {
			rs, _ := newObjectRecordset()

			codes := []types.ResultCode{types.PARAMETER_ERROR, types.NETWORK_ERROR, types.TIMEOUT}

			received := make(chan Error, len(codes))
			go func() {
				for err := range rs.Errors() {
					received <- err
				}
				close(received)
			}()

			for _, code := range codes {
				sentErr := newError(code, "Error")
				rs.sendError(sentErr)

				var receivedErr Error
				gm.Eventually(received).WithTimeout(time.Second).Should(gm.Receive(&receivedErr))
				matches := receivedErr.Matches(code)
				gm.Expect(matches).To(gm.BeTrue())
			}

			rs.signalEnd()
			gm.Eventually(received).WithTimeout(time.Second).Should(gm.BeClosed())
		})
	})

})
