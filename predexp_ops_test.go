// Copyright 2014-2021 Aerospike, Inc.
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

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"

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

var _ = gg.Describe("PredExp in Transactions Test", func() {

	gg.BeforeEach(func() {
		if !isEnterpriseEdition() {
			gg.Skip("Predexp Tests for All transactions are not supported in the Community Edition.")
			return
		}
	})

	var udfReg sync.Once

	var registerUDF = func() {
		udfReg.Do(func() {
			regTask, err := client.RegisterUDF(nil, []byte(udfPredexpBody), "udf1.lua", as.LUA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// wait until UDF is created
			gm.Expect(<-regTask.OnComplete()).NotTo(gm.HaveOccurred())
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

	gg.Describe("PredExp in Transactions Test", func() {

		gg.AfterEach(func() {
			_, err := client.Delete(nil, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			_, err = client.Delete(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.BeforeEach(func() {
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
			gm.Expect(err).ToNot(gm.HaveOccurred())
			_, err = client.Delete(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(nil, keyA, binA1)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			err = client.PutBins(nil, keyB, binA2)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.It("should work for Put", func() {
			err := client.PutBins(predAEq1WPolicy, keyA, binA3)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			r, err := client.Get(nil, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(r.Bins[binA3.Name]).To(gm.Equal(binA3.Value.GetObject()))

			client.PutBins(predAEq1WPolicy, keyB, binA3)
			r, err = client.Get(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(r.Bins[binA2.Name]).To(gm.Equal(binA2.Value.GetObject()))
		})

		gg.It("should work for Put Except...", func() {
			err := client.PutBins(predAEq1WPolicy, keyA, binA3)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(predAEq1WPolicy, keyB, binA3)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for Get", func() {
			r, err := client.Get(predAEq1RPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins[binA1.Name]).To(gm.Equal(binA1.Value.GetObject()))

			r, err = client.Get(predAEq1RPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
			gm.Expect(r).To(gm.BeNil())
		})

		gg.It("should work for Get Except...", func() {
			_, err := client.Get(predAEq1RPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Get(predAEq1RPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for BatchGet", func() {
			keys := []*as.Key{keyA, keyB}

			records, err := client.BatchGet(predAEq1BPolicy, keys)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Error()).To(gm.Equal(ast.ErrFilteredOut.Error()))

			gm.Expect(records[0].Bins[binA1.Name]).To(gm.Equal(binA1.Value.GetObject()))
			gm.Expect(records[1]).To(gm.BeNil())
		})

		gg.It("should work for Delete", func() {
			existed, err := client.Delete(predAEq1WPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			_, err = client.Get(nil, keyA)
			gm.Expect(err).To(gm.Equal(ast.ErrKeyNotFound))

			_, err = client.Delete(predAEq1WPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins[binA2.Name]).To(gm.Equal(binA2.Value.GetObject()))
		})

		gg.It("should work for Delete Except...", func() {
			_, err := client.Delete(predAEq1WPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for Durable Delete", func() {
			predAEq1WPolicy.DurableDelete = true

			_, err := client.Delete(predAEq1WPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			r, err := client.Get(nil, keyA)
			gm.Expect(err).To(gm.Equal(ast.ErrKeyNotFound))
			gm.Expect(r).To(gm.BeNil())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins[binA2.Name]).To(gm.Equal(binA2.Value.GetObject()))
		})

		gg.It("should work for Durable Delete Except...", func() {
			predAEq1WPolicy.DurableDelete = true

			_, err := client.Delete(predAEq1WPolicy, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Delete(predAEq1WPolicy, keyB)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for Operate Read", func() {
			r, err := client.Operate(predAEq1WPolicy, keyA, as.GetOpForBin(binAName))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(r.Bins[binA1.Name]).To(gm.Equal(binA1.Value.GetObject()))

			r, err = client.Operate(predAEq1WPolicy, keyB, as.GetOpForBin(binAName))
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
			gm.Expect(r).To(gm.BeNil())
		})

		gg.It("should work for Operate Read Except...", func() {
			_, err := client.Operate(predAEq1WPolicy, keyA, as.GetOpForBin(binAName))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Operate(predAEq1WPolicy, keyB, as.GetOpForBin(binAName))
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for Operate Write", func() {
			r, err := client.Operate(predAEq1WPolicy, keyA, as.PutOp(binA3), as.GetOpForBin(binAName))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(r.Bins[binA3.Name]).To(gm.Equal(binA3.Value.GetObject()))

			r, err = client.Operate(predAEq1WPolicy, keyB, as.PutOp(binA3), as.GetOpForBin(binAName))
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
			gm.Expect(r).To(gm.BeNil())
		})

		gg.It("should work for Operate Write Except...", func() {
			_, err := client.Operate(predAEq1WPolicy, keyA, as.PutOp(binA3), as.GetOpForBin(binAName))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Operate(predAEq1WPolicy, keyB, as.PutOp(binA3), as.GetOpForBin(binAName))
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

		gg.It("should work for UDF", func() {
			registerUDF()

			_, err := client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			r, err := client.Get(nil, keyA)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(r.Bins[binA3.Name]).To(gm.Equal(binA3.Value.GetObject()))

			_, err = client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))

			r, err = client.Get(nil, keyB)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins[binA2.Name]).To(gm.Equal(binA2.Value.GetObject()))
		})

		gg.It("should work for UDF Except...", func() {
			registerUDF()

			_, err := client.Execute(predAEq1WPolicy, keyA, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Execute(predAEq1WPolicy, keyB, "record_example", "writeBin", as.StringValue(binA3.Name), binA3.Value)
			gm.Expect(err).To(gm.HaveOccurred())
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.FILTERED_OUT))
		})

	})

}) // describe
