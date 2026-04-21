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
	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// binSerDerPerson is a hand-written BinSerDer (representative of what
// the binserdergen tool emits) used by the integration tests.
type binSerDerPerson struct {
	Name string
	Age  int
	Bio  string

	// Metadata populated via SetAerospikeMeta.
	TTL uint32
	Gen uint32
}

var _binSerDerPersonBinNames = []string{"name", "age", "bio"}

func (p *binSerDerPerson) AerospikeBinNames() []string { return _binSerDerPersonBinNames }

func (p *binSerDerPerson) UnmarshalBin(name string, value any) as.Error {
	if value == nil {
		return nil
	}
	switch name {
	case "name":
		if v, ok := value.(string); ok {
			p.Name = v
		}
	case "age":
		if v, ok := value.(int); ok {
			p.Age = v
		}
	case "bio":
		if v, ok := value.(string); ok {
			p.Bio = v
		}
	}
	return nil
}

func (p *binSerDerPerson) SetAerospikeMeta(generation uint32, expiration uint32) {
	p.Gen = generation
	p.TTL = expiration
}

var _ = gg.Describe("Aerospike BinSerDer", func() {
	var ns = *namespace
	var set = randString(50)

	gg.It("must populate a BinSerDer object without reflection", func() {
		key, kerr := as.NewKey(ns, set, randString(50))
		gm.Expect(kerr).ToNot(gm.HaveOccurred())

		err := client.PutBins(nil, key,
			as.NewBin("name", "Ada"),
			as.NewBin("age", 37),
			as.NewBin("bio", "analyst"),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		p := &binSerDerPerson{}
		err = client.GetObjectBinSerDer(nil, key, p)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(p.Name).To(gm.Equal("Ada"))
		gm.Expect(p.Age).To(gm.Equal(37))
		gm.Expect(p.Bio).To(gm.Equal("analyst"))
		gm.Expect(p.Gen).To(gm.BeNumerically(">=", uint32(1)))
	})

	gg.It("must return an error on nil target", func() {
		key, kerr := as.NewKey(ns, set, randString(50))
		gm.Expect(kerr).ToNot(gm.HaveOccurred())

		err := client.GetObjectBinSerDer(nil, key, nil)
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must ignore bins that the implementation does not recognise", func() {
		key, kerr := as.NewKey(ns, set, randString(50))
		gm.Expect(kerr).ToNot(gm.HaveOccurred())

		err := client.PutBins(nil, key,
			as.NewBin("name", "Grace"),
			as.NewBin("age", 45),
			as.NewBin("bio", "admiral"),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		p := &binSerDerPerson{}
		err = client.GetObjectBinSerDer(nil, key, p)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(p.Name).To(gm.Equal("Grace"))
	})
})
