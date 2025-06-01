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

	"gopkg.in/yaml.v3"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("YAML Unmarshal for Enum Types", func() {
	DescribeTable("ReadModeAp",
		func(input string, expected dynconfig.ReadModeAp, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeAp `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(wrapper.Val).To(Equal(expected))
			}
		},
		Entry("ONE", "ONE", dynconfig.ONE, false),
		Entry("ALL", "ALL", dynconfig.ALL, false),
		Entry("lowercase", "one", dynconfig.ONE, false),
		Entry("invalid", "foo", nil, true),
		Entry("empty", "", nil, true),
		Entry("quoted invalid", `"badval"`, nil, true),
		Entry("number", "123", nil, true),
	)

	DescribeTable("ReadModeSc",
		func(input string, expected dynconfig.ReadModeSc, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeSc `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(wrapper.Val).To(Equal(expected))
			}
		},
		Entry("SESSION", "SESSION", dynconfig.SESSION, false),
		Entry("LINEARIZE", "LINEARIZE", dynconfig.LINEARIZE, false),
		Entry("ALLOW_REPLICA", "ALLOW_REPLICA", dynconfig.ALLOW_REPLICA, false),
		Entry("ALLOW_UNAVAILABLE", "ALLOW_UNAVAILABLE", dynconfig.ALLOW_UNAVAILABLE, false),
		Entry("lowercase", "session", dynconfig.SESSION, false),
		Entry("invalid", "foo", nil, true),
		Entry("empty", "", nil, true),
		Entry("quoted invalid", `"badval"`, nil, true),
		Entry("number", "123", nil, true),
	)

	DescribeTable("Replica",
		func(input string, expected dynconfig.Replica, expectErr bool) {
			var wrapper struct {
				Val dynconfig.Replica `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(wrapper.Val).To(Equal(expected))
			}
		},
		Entry("MASTER", "MASTER", dynconfig.MASTER, false),
		Entry("MASTER_PROLES", "MASTER_PROLES", dynconfig.MASTER_PROLES, false),
		Entry("SEQUENCE", "SEQUENCE", dynconfig.SEQUENCE, false),
		Entry("PREFER_RACK", "PREFER_RACK", dynconfig.PREFER_RACK, false),
		Entry("lowercase", "master", dynconfig.MASTER, false),
		Entry("invalid", "foo", nil, true),
		Entry("empty", "", nil, true),
		Entry("quoted invalid", `"badval"`, nil, true),
		Entry("number", "123", nil, true),
	)

	DescribeTable("QueryDuration",
		func(input string, expected dynconfig.QueryDuration, expectErr bool) {
			var wrapper struct {
				Val dynconfig.QueryDuration `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(wrapper.Val).To(Equal(expected))
			}
		},
		Entry("LONG", "LONG", dynconfig.LONG, false),
		Entry("SHORT", "SHORT", dynconfig.SHORT, false),
		Entry("LONG_RELAX_AP", "LONG_RELAX_AP", dynconfig.LONG_RELAX_AP, false),
		Entry("lowercase", "long", dynconfig.LONG, false),
		Entry("invalid", "foo", nil, true),
		Entry("empty", "", nil, true),
		Entry("quoted invalid", `"badval"`, nil, true),
		Entry("number", "123", nil, true),
	)
})
