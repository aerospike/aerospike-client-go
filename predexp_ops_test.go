// Copyright 2013-2019 Aerospike, Inc.
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
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
)

const udfPredexpBody = `local function putBin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

-- Set a particular bin
function writeBin(r,name,value)
    putBin(r,name,value)
end

-- Get a particular bin
function readBin(r,name)
    return r[name]
end

-- Return generation count of record
function getGeneration(r)
    return record.gen(r)
end

-- Update record only if gen hasn't changed
function writeIfGenerationNotChanged(r,name,value,gen)
    if record.gen(r) == gen then
        r[name] = value
        aerospike:update(r)
    end
end

-- Set a particular bin only if record does not already exist.
function writeUnique(r,name,value)
    if not aerospike:exists(r) then 
        aerospike:create(r) 
        r[name] = value
        aerospike:update(r)
    end
end

-- Validate value before writing.
function writeWithValidation(r,name,value)
    if (value >= 1 and value <= 10) then
        putBin(r,name,value)
    else
        error("1000:Invalid value") 
    end
end

-- Record contains two integer bins, name1 and name2.
-- For name1 even integers, add value to existing name1 bin.
-- For name1 integers with a multiple of 5, delete name2 bin.
-- For name1 integers with a multiple of 9, delete record. 
function processRecord(r,name1,name2,addValue)
    local v = r[name1]

    if (v % 9 == 0) then
        aerospike:remove(r)
        return
    end

    if (v % 5 == 0) then
        r[name2] = nil
        aerospike:update(r)
        return
    end

    if (v % 2 == 0) then
        r[name1] = v + addValue
        aerospike:update(r)
    end
end

-- Append to end of regular list bin
function appendListBin(r, binname, value)
  local l = r[binname]

  if l == nil then
    l = list()
  end

  list.append(l, value)
  r[binname] = l
  aerospike:update(r)
end

-- Set expiration of record
-- function expire(r,ttl)
--    if record.ttl(r) == gen then
--        r[name] = value
--        aerospike:update(r)
--    end
-- end
`

var _ = Describe("PredExp in Transactions Test", func() {

	BeforeEach(func() {
		if !isEnterpriseEdition() {
			Skip("Predexp Tests for All transactions are not supported in the Community Edition.")
			return
		}
	})

	var udfReg sync.Once

	var registerUDF = func() {
		udfReg.Do(func() {
			regTask, err := client.RegisterUDF(nil, []byte(udfPredexpBody), "udf1.lua", as.LUA)
			Expect(err).ToNot(HaveOccurred())

			// wait until UDF is created
			Expect(<-regTask.OnComplete()).NotTo(HaveOccurred())
		})
	}

	// connection data
	var ns = *namespace
	var set string
	var keyA, keyB *as.Key

	set = randString(50)
	keyA, _ = as.NewKey(ns, set, randString(50))
	keyB, _ = as.NewKey(ns, set, randString(50))

	var binAName string

	var predAEq1WPolicy *as.WritePolicy
	var predAEq1BPolicy *as.BatchPolicy
	var predAEq1RPolicy *as.BasePolicy

	var binA1 *as.Bin
	var binA2 *as.Bin
	var binA3 *as.Bin

	binAName = "binAName"
	binA1 = as.NewBin(binAName, 1)
	binA2 = as.NewBin(binAName, 2)
	binA3 = as.NewBin(binAName, 3)

	Describe("PredExp in Transactions Test", func() {

		AfterEach(func() {
			_, err := client.Delete(nil, keyA)
			Expect(err).ToNot(HaveOccurred())
			_, err = client.Delete(nil, keyB)
			Expect(err).ToNot(HaveOccurred())
		})

		BeforeEach(func() {
			predAEq1WPolicy = as.NewWritePolicy(0, 0)
			predAEq1BPolicy = as.NewBatchPolicy()
			predAEq1RPolicy = as.NewPolicy()

			predAEq1BPolicy.PredExp = []as.PredExp{
				as.NewPredExpIntegerBin(binAName),
				as.NewPredExpIntegerValue(1),
				as.NewPredExpIntegerEqual(),
			}

			predAEq1RPolicy.PredExp = []as.PredExp{
				as.NewPredExpIntegerBin(binAName),
				as.NewPredExpIntegerValue(1),
				as.NewPredExpIntegerEqual(),
			}

			predAEq1WPolicy.PredExp = []as.PredExp{
				as.NewPredExpIntegerBin(binAName),
				as.NewPredExpIntegerValue(1),
				as.NewPredExpIntegerEqual(),
			}

			_, err := client.Delete(nil, keyA)
			Expect(err).ToNot(HaveOccurred())
			_, err = client.Delete(nil, keyB)
			Expect(err).ToNot(HaveOccurred())

			err = client.PutBins(nil, keyA, binA1)
			Expect(err).ToNot(HaveOccurred())
			err = client.PutBins(nil, keyB, binA2)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should work for Put", func() {
			err := client.PutBins(predAEq1WPolicy, keyA, binA3)
			Expect(err).ToNot(HaveOccurred())

			r, err := client.Get(nil, keyA)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.Bins[binA3.Name]).To(Equal(binA3.Value.GetObject()))

			client.PutBins(predAEq1WPolicy, keyB, binA3)
			r, err = client.Get(nil, keyB)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.Bins[binA2.Name]).To(Equal(binA2.Value.GetObject()))
		})

		It("should work for Put Except...", func() {
			err := client.PutBins(predAEq1WPolicy, keyA, binA3)
			Expect(err).ToNot(HaveOccurred())

			err = client.PutBins(predAEq1WPolicy, keyB, binA3)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for Get", func() {
			r, err := client.Get(predAEq1RPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())
			Expect(r.Bins[binA1.Name]).To(Equal(binA1.Value.GetObject()))

			r, err = client.Get(predAEq1RPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
			Expect(r).To(BeNil())
		})

		It("should work for Get Except...", func() {
			_, err := client.Get(predAEq1RPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Get(predAEq1RPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for BatchGet", func() {
			keys := []*as.Key{keyA, keyB}

			records, err := client.BatchGet(predAEq1BPolicy, keys)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(ast.ErrFilteredOut.Error()))

			Expect(records[0].Bins[binA1.Name]).To(Equal(binA1.Value.GetObject()))
			Expect(records[1]).To(BeNil())
		})

		It("should work for Delete", func() {
			existed, err := client.Delete(predAEq1WPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())
			Expect(existed).To(BeTrue())

			_, err = client.Get(nil, keyA)
			Expect(err).To(Equal(ast.ErrKeyNotFound))

			_, err = client.Delete(predAEq1WPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			Expect(err).ToNot(HaveOccurred())
			Expect(r.Bins[binA2.Name]).To(Equal(binA2.Value.GetObject()))
		})

		It("should work for Delete Except...", func() {
			_, err := client.Delete(predAEq1WPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for Durable Delete", func() {
			predAEq1WPolicy.DurableDelete = true

			_, err := client.Delete(predAEq1WPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())

			r, err := client.Get(nil, keyA)
			Expect(err).To(Equal(ast.ErrKeyNotFound))
			Expect(r).To(BeNil())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			Expect(err).ToNot(HaveOccurred())
			Expect(r.Bins[binA2.Name]).To(Equal(binA2.Value.GetObject()))
		})

		It("should work for Durable Delete Except...", func() {
			predAEq1WPolicy.DurableDelete = true

			_, err := client.Delete(predAEq1WPolicy, keyA)
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for Operate Read", func() {
			r, err := client.Operate(predAEq1WPolicy, keyA, as.GetOpForBin(binAName))
			Expect(err).ToNot(HaveOccurred())

			Expect(r.Bins[binA1.Name]).To(Equal(binA1.Value.GetObject()))

			r, err = client.Operate(predAEq1WPolicy, keyB, as.GetOpForBin(binAName))
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
			Expect(r).To(BeNil())
		})

		It("should work for Operate Read Except...", func() {
			_, err := client.Operate(predAEq1WPolicy, keyA, as.GetOpForBin(binAName))
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Operate(predAEq1WPolicy, keyB, as.GetOpForBin(binAName))
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for Operate Write", func() {
			r, err := client.Operate(predAEq1WPolicy, keyA, as.PutOp(binA3), as.GetOpForBin(binAName))
			Expect(err).ToNot(HaveOccurred())

			Expect(r.Bins[binA3.Name]).To(Equal(binA3.Value.GetObject()))

			r, err = client.Operate(predAEq1WPolicy, keyB, as.PutOp(binA3), as.GetOpForBin(binAName))
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
			Expect(r).To(BeNil())
		})

		It("should work for Operate Write Except...", func() {
			_, err := client.Operate(predAEq1WPolicy, keyA, as.PutOp(binA3), as.GetOpForBin(binAName))
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Operate(predAEq1WPolicy, keyB, as.PutOp(binA3), as.GetOpForBin(binAName))
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

		It("should work for UDF", func() {
			registerUDF()

			_, err := client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			Expect(err).ToNot(HaveOccurred())

			r, err := client.Get(nil, keyA)
			Expect(err).ToNot(HaveOccurred())

			Expect(r.Bins[binA3.Name]).To(Equal(binA3.Value.GetObject()))

			_, err = client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			Expect(err).ToNot(HaveOccurred())
			Expect(r.Bins[binA2.Name]).To(Equal(binA2.Value.GetObject()))
		})

		It("should work for UDF Except...", func() {
			registerUDF()

			_, err := client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			Expect(err).ToNot(HaveOccurred())

			_, err = client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			Expect(err).To(HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			Expect(ok).To(BeTrue())
			Expect(aerr.ResultCode()).To(Equal(ast.FILTERED_OUT))
		})

	})

}) // describe
