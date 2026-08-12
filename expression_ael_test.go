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
	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("AEL filter expressions", func() {
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)

	gg.BeforeEach(func() {
		// Seed a handful of records with an age bin to filter on.
		for i := range 10 {
			key, err := as.NewKey(ns, set, i)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			err = client.PutBins(wpolicy, key,
				as.NewBin("age", i*10),
				as.NewBin("status", map[bool]string{true: "active", false: "idle"}[i%2 == 0]),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	gg.It("must construct an AEL expression and report it as AEL", func() {
		exp := as.ExpAEL(`$.age >= 50`)
		gm.Expect(exp).ToNot(gm.BeNil())
		gm.Expect(exp.IsAEL()).To(gm.BeTrue())

		// A client-compiled expression is not AEL.
		typed := as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(50))
		gm.Expect(typed.IsAEL()).To(gm.BeFalse())
	})

	gg.It("must filter a query with AEL source text", func() {
		if !serverSupportsAEL() {
			gg.Skip("server does not compile AEL (requires 8.1.3+)")
		}

		stmt := as.NewStatement(ns, set)
		policy := as.NewQueryPolicy()
		policy.FilterExpression = as.ExpAEL(`$.age >= 50`)

		rs, err := client.Query(policy, stmt)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for rec := range rs.Results() {
			gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Record.Bins["age"]).To(gm.BeNumerically(">=", 50))
			count++
		}
		// ages 50, 60, 70, 80, 90
		gm.Expect(count).To(gm.Equal(5))
	})

	gg.It("must filter a single read with AEL source text", func() {
		if !serverSupportsAEL() {
			gg.Skip("server does not compile AEL (requires 8.1.3+)")
		}

		key, err := as.NewKey(ns, set, 9) // age 90
		gm.Expect(err).ToNot(gm.HaveOccurred())

		policy := as.NewPolicy()
		policy.FilterExpression = as.ExpAEL(`$.age >= 50`)
		rec, err := client.Get(policy, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec).ToNot(gm.BeNil())

		// The same record fails a filter it does not satisfy.
		policy.FilterExpression = as.ExpAEL(`$.age < 50`)
		_, err = client.Get(policy, key)
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must surface an AEL syntax error as a server error", func() {
		if !serverSupportsAEL() {
			gg.Skip("server does not compile AEL (requires 8.1.3+)")
		}

		stmt := as.NewStatement(ns, set)
		policy := as.NewQueryPolicy()
		// The client does not parse AEL, so a syntax error can only come back
		// from the server.
		policy.FilterExpression = as.ExpAEL(`$.age >>>> `)

		rs, err := client.Query(policy, stmt)
		if err == nil {
			// The failure may arrive on the recordset instead of the call.
			sawErr := false
			for res := range rs.Results() {
				if res.Err != nil {
					sawErr = true
					break
				}
			}
			gm.Expect(sawErr).To(gm.BeTrue())
			return
		}
		gm.Expect(err).To(gm.HaveOccurred())
	})
})

// serverSupportsAEL reports whether every node compiles AEL (server 8.1.3+).
func serverSupportsAEL() bool {
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "build")
		if err != nil {
			return false
		}
		if !versionAtLeast(info["build"], 8, 1, 3) {
			return false
		}
	}
	return true
}

// versionAtLeast compares a dotted build string against a minimum.
func versionAtLeast(build string, major, minor, patch int) bool {
	var ma, mi, pa int
	n := 0
	field := 0
	for i := 0; i <= len(build); i++ {
		if i < len(build) && build[i] >= '0' && build[i] <= '9' {
			n = n*10 + int(build[i]-'0')
			continue
		}
		switch field {
		case 0:
			ma = n
		case 1:
			mi = n
		case 2:
			pa = n
		}
		field++
		n = 0
		if field > 2 || i >= len(build) {
			break
		}
		if build[i] != '.' {
			break
		}
	}
	switch {
	case ma != major:
		return ma > major
	case mi != minor:
		return mi > minor
	default:
		return pa >= patch
	}
}
