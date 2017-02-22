// Copyright 2017 Aerospike, Inc.
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
	. "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("Predicate API Test", func() {

	It("must convert to string correctly", func() {

		matrix := map[*boolExpression]string{
			BinValue("bin").Equal(5):                                                       "bin = 5",
			BinValue("bin").Equal("hello"):                                                 "bin = 'hello'",
			BinValue("bin1").Equal("hello").And(BinValue("bin2").Equal(5)):                 "bin1 = 'hello' AND bin2 = 5",
			BinValue("bin1").NotEqual("hello").And(BinValue("bin2").NotEqual(5)):           "bin1 != 'hello' AND bin2 != 5",
			BinValue("bin1").EqualOrGreaterThan(5).Or(BinValue("bin2").EqualOrLessThan(5)): "bin1 >= 5 OR bin2 <= 5",
			BinValue("bin1").Regexp("[A-Z]([a-z])*"):                                       "bin1 regex: '[A-Z]([a-z])*'",

			// BinValue("bin1").Between(5, 10):   "bin1 >= 5 AND bin1 <= 10",
			// BinValue("bin1").In(1, 7, 15, 10): "bin1 = 1 OR bin1 = 7 OR bin1 = 15 OR bin1 = 10",
		}

		for e, expected := range matrix {
			gm.Expect((*expression)(e).String()).To(gm.Equal(expected))
		}
	})

})
