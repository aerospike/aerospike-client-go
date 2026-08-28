// Copyright 2026 Aerospike, Inc.
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
	"strconv"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Ported from the Java client's TestQueryFilterExp StringExp additions.
// Exercises StringExp.{startsWith,contains,endsWith,regexCompare} as a query
// filterExp on a NUMERIC-indexed set narrowed via a range filter, plus
// contains() against setName() and regexCompare() against key().
//
// String expressions require server version 8.1.3+; the suite is skipped on
// older clusters.
var _ = gg.Describe("Query StringExp filter expressions", func() {
	const (
		keyPrefix = "flt"
		binName   = "fltint"
		strBin    = "strbin"
		setSize   = 50
	)

	var (
		ns        = *namespace
		setName   string
		indexName string
	)

	// sendKey=true so Exp.key() can be evaluated server-side. The stored-key
	// flag is set at record-create time; the per-test set name is randomized
	// so there are no pre-existing records to delete.
	wpolicy := as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	gg.BeforeEach(func() {
		requiredVersion, err := version.Parse("8.1.3")
		if err != nil {
			gg.Fail("Failed to parse server required version")
		}
		nodeVersion := client.GetNodes()[0].GetServerVersion()
		if nodeVersion.IsSmaller(requiredVersion) {
			gg.Skip("String expressions require server version 8.1.3+.")
			return
		}

		setName = randString(50) + "flt"
		indexName = setName + "flt"

		createIndex(wpolicy, ns, setName, indexName, binName, as.NUMERIC)

		for i := 1; i <= setSize; i++ {
			key, err := as.NewKey(ns, setName, keyPrefix+strconv.Itoa(i))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Rotate prefixes so StringExp predicates pick deterministic
			// subsets within range 1..10:
			//   i % 3 == 0 -> "alpha-i" -> records 3, 6, 9
			//   i % 3 == 1 -> "beta-i"  -> records 1, 4, 7, 10
			//   i % 3 == 2 -> "gamma-i" -> records 2, 5, 8
			var prefix string
			switch i % 3 {
			case 0:
				prefix = "alpha"
			case 1:
				prefix = "beta"
			default:
				prefix = "gamma"
			}
			str := prefix + "-" + strconv.Itoa(i)

			err = client.PutBins(wpolicy, key,
				as.NewBin(binName, i),
				as.NewBin(strBin, str))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	gg.AfterEach(func() {
		client.DropIndex(nil, ns, setName, indexName)
		client.Truncate(nil, ns, setName, nil)
	})

	runQuery := func(filterExp *as.Expression) int {
		stmt := as.NewStatement(ns, setName)
		stmt.SetFilter(as.NewRangeFilter(binName, 1, 10))

		policy := as.NewQueryPolicy()
		policy.FilterExpression = filterExp

		rs, err := client.Query(policy, stmt)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range rs.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			count++
		}
		return count
	}

	gg.It("startsWith on strbin matches alpha records (3, 6, 9)", func() {
		count := runQuery(as.ExpStringStartsWith(as.ExpStringBin(strBin), as.ExpStringVal("alpha")))
		gm.Expect(count).To(gm.Equal(3))
	})

	gg.It("contains on strbin matches gamma records (2, 5, 8)", func() {
		count := runQuery(as.ExpStringContains(as.ExpStringBin(strBin), as.ExpStringVal("amma")))
		gm.Expect(count).To(gm.Equal(3))
	})

	gg.It("endsWith on strbin matches only -10", func() {
		count := runQuery(as.ExpStringEndsWith(as.ExpStringBin(strBin), as.ExpStringVal("-10")))
		gm.Expect(count).To(gm.Equal(1))
	})

	gg.It("regexCompare ^beta- on strbin matches 4 records (1, 4, 7, 10)", func() {
		count := runQuery(as.ExpStringRegexCompare(as.ExpStringBin(strBin), as.ExpStringVal("^beta-")))
		gm.Expect(count).To(gm.Equal(4))
	})

	gg.It("contains on setName() matches all records in range", func() {
		// Probe whether the new string-ops family supports source expressions
		// other than bin/CDT projections (setName(), key()).
		count := runQuery(as.ExpStringContains(as.ExpSetName(), as.ExpStringVal("flt")))
		gm.Expect(count).To(gm.Equal(10))
	})

	gg.It("regexCompareWithFlags on key() finds flt3", func() {
		count := runQuery(as.ExpStringRegexCompareWithFlags(as.ExpKey(as.ExpTypeSTRING), as.ExpStringVal("^flt3$"), as.StringRegexDefault))
		gm.Expect(count).To(gm.Equal(1))
	})
})
