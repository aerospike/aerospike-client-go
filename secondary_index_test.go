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
	as "github.com/aerospike/aerospike-client-go/v8"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

const (
	setName   = "exp_SI_test_set"
	indexName = "εχπ_ΣΙ_τεστ_ιδχ"
)

var countries = []string{"Australia", "Canada", "USA"}
var exp = as.ExpCond(
	as.ExpAnd(
		as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(18)),
		as.ExpOr(
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[0])),
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[1])),
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[2])),
		),
	),
	as.ExpIntVal(1),
	as.ExpUnknown(),
)

var _ = gg.Describe("Secondary index test", func() {
	var wpolicy = as.NewWritePolicy(0, 0)

	gg.Describe("Index creation with expression", gg.Ordered, func() {
		gg.BeforeAll(func() {
			// Make sure the global set‑up really happened.
			gm.Expect(client).NotTo(gm.BeNil(), "client must be initialized in the suite’s set‑up")

			task, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, indexName, as.NUMERIC, as.ICT_DEFAULT, exp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndex: %v", err))
			}

			// wait until index is created
			<-task.OnComplete()

			insertTestRecords()
		})

		gg.Context("Create non-existing index", func() {
			gg.It("is listed after creation", func() {
				info := getSIInfo()
				gm.Expect(info).To(gm.ContainSubstring("indexname=" + indexName))
			})

			gg.It("returns six records when filtering by index *name*", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewRangeWithIndexNameFilter(indexName, as.NewValue(1), as.NewValue(1)))
				gm.Expect(runQueryAndAssert(stmt)).To(gm.Equal(6))
			})

			gg.It("returns six records when filtering by *expression*", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewRangeWithExpressionFilter(exp, as.NewValue(1), as.NewValue(1)))
				expectedResponse := runQueryAndAssert(stmt)
				gm.Expect(expectedResponse).To(gm.Equal(6))
			})
		})
	})
})

func insertTestRecords() {
	people := []struct {
		key     int
		name    string
		age     int
		country string
	}{
		{1, "Tim", 312, "Australia"},
		{2, "Bob", 47, "Canada"},
		{3, "Jo", 15, "USA"},
		{4, "Steven", 23, "Botswana"},
		{5, "Susan", 32, "Canada"},
		{6, "Jess", 17, "USA"},
		{7, "Sam", 18, "USA"},
		{8, "Alex", 47, "Canada"},
		{9, "Pam", 56, "Australia"},
		{10, "Vivek", 12, "India"},
		{11, "Kiril", 22, "Sweden"},
		{12, "Bill", 23, "UK"},
	}

	for _, p := range people {
		key, _ := as.NewKey(*namespace, setName, p.key)
		err := client.PutBins(nil, key, as.NewBin("name", p.name), as.NewBin("age", p.age), as.NewBin("country", p.country))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	}
}

func getSIInfo() string {
	cmd := fmt.Sprintf("sindex-list/%s/%s", *namespace, indexName)
	node := client.GetNodes()[0]
	info, err := node.RequestInfo(as.NewInfoPolicy(), cmd)

	gm.Expect(err).NotTo(gm.HaveOccurred())
	return info[cmd]
}

func runQueryAndAssert(stmt *as.Statement) int {
	qp := as.NewQueryPolicy()
	rs, err := client.Query(qp, stmt)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	defer rs.Close()

	count := 0
	for res := range rs.Results() {
		gm.Expect(res.Err).NotTo(gm.HaveOccurred())

		age := res.Record.Bins["age"].(int)
		country := res.Record.Bins["country"].(string)

		gm.Expect(age).To(gm.BeNumerically(">=", 18))
		gm.Expect(country).To(gm.BeElementOf(countries))
		count++
	}
	return count
}
