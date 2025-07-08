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
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("YAML Unmarshal for Enum Types", func() {
	gg.DescribeTable("ReadModeAp",
		func(input string, expected dynconfig.ReadModeAp, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeAp `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("ONE", "ONE", dynconfig.ONE, false),
		gg.Entry("ALL", "ALL", dynconfig.ALL, false),
		gg.Entry("lowercase", "one", dynconfig.ONE, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("ReadModeSc",
		func(input string, expected dynconfig.ReadModeSc, expectErr bool) {
			var wrapper struct {
				Val dynconfig.ReadModeSc `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("SESSION", "SESSION", dynconfig.SESSION, false),
		gg.Entry("LINEARIZE", "LINEARIZE", dynconfig.LINEARIZE, false),
		gg.Entry("ALLOW_REPLICA", "ALLOW_REPLICA", dynconfig.ALLOW_REPLICA, false),
		gg.Entry("ALLOW_UNAVAILABLE", "ALLOW_UNAVAILABLE", dynconfig.ALLOW_UNAVAILABLE, false),
		gg.Entry("lowercase", "session", dynconfig.SESSION, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("Replica",
		func(input string, expected dynconfig.Replica, expectErr bool) {
			var wrapper struct {
				Val dynconfig.Replica `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("MASTER", "MASTER", dynconfig.MASTER, false),
		gg.Entry("MASTER_PROLES", "MASTER_PROLES", dynconfig.MASTER_PROLES, false),
		gg.Entry("SEQUENCE", "SEQUENCE", dynconfig.SEQUENCE, false),
		gg.Entry("PREFER_RACK", "PREFER_RACK", dynconfig.PREFER_RACK, false),
		gg.Entry("lowercase", "master", dynconfig.MASTER, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)

	gg.DescribeTable("QueryDuration",
		func(input string, expected dynconfig.QueryDuration, expectErr bool) {
			var wrapper struct {
				Val dynconfig.QueryDuration `yaml:"val"`
			}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("val: \"%s\"", input)), &wrapper)
			if expectErr {
				gm.Expect(err).To(gm.HaveOccurred())
			} else {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(wrapper.Val).To(gm.Equal(expected))
			}
		},
		gg.Entry("LONG", "LONG", dynconfig.LONG, false),
		gg.Entry("SHORT", "SHORT", dynconfig.SHORT, false),
		gg.Entry("LONG_RELAX_AP", "LONG_RELAX_AP", dynconfig.LONG_RELAX_AP, false),
		gg.Entry("lowercase", "long", dynconfig.LONG, false),
		gg.Entry("invalid", "foo", nil, true),
		gg.Entry("empty", "", nil, true),
		gg.Entry("quoted invalid", `"badval"`, nil, true),
		gg.Entry("number", "123", nil, true),
	)
})
