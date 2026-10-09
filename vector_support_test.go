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
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Vector support check", func() {
	const unsupported = "Vector is not supported by all nodes in the cluster"

	// commandWithoutVectorSupport returns a command bound to a node of a
	// cluster where not every node supports vectors.
	commandWithoutVectorSupport := func() *baseCommand {
		cluster := &Cluster{}
		cluster.supportsVector.Set(false)
		return &baseCommand{
			bufferEx: bufferEx{dataBuffer: make([]byte, 4096)},
			node:     &Node{cluster: cluster},
		}
	}

	vector := NewVectorFloat32([]float32{1, 2})
	containsVector := ExpEq(ExpListVal(vector), ExpListVal(vector))

	// The ways a vector reaches the wire.
	vectorWrites := []struct {
		name  string
		write func(cmd *baseCommand) Error
	}{
		{"a vector bin", func(cmd *baseCommand) Error {
			return cmd.writeOperationForBin(NewBin("v", vector), _WRITE)
		}},
		{"a vector in a list bin", func(cmd *baseCommand) Error {
			return cmd.writeOperationForBin(NewBin("l", []any{1, vector}), _WRITE)
		}},
		{"a vector in a map bin", func(cmd *baseCommand) Error {
			return cmd.writeOperationForBin(NewBin("m", map[any]any{"emb": vector}), _WRITE)
		}},
		{"a list append", func(cmd *baseCommand) Error {
			return cmd.writeOperationForOperation(ListAppendOp("l", vector))
		}},
		{"a map put", func(cmd *baseCommand) Error {
			return cmd.writeOperationForOperation(MapPutOp(DefaultMapPolicy(), "m", "emb", vector))
		}},
		{"a filter expression", func(cmd *baseCommand) Error {
			size, err := containsVector.size()
			if err != nil {
				return err
			}
			return cmd.writeFilterExpression(containsVector, size)
		}},
		{"a vector distance filter expression", func(cmd *baseCommand) Error {
			closeEnough := ExpLess(ExpVectorDistance(VectorEuclidean, vector, ExpVectorBin("v")), ExpFloatVal(1))
			size, err := closeEnough.size()
			if err != nil {
				return err
			}
			return cmd.writeFilterExpression(closeEnough, size)
		}},
		{"an expression read operation", func(cmd *baseCommand) Error {
			return cmd.writeOperationForOperation(ExpReadOp("r", containsVector, ExpReadFlagDefault))
		}},
		{"a context filter expression", func(cmd *baseCommand) Error {
			return cmd.writeOperationForOperation(ListGetByIndexOp("l", 0, ListReturnTypeValue, CtxAllChildrenWithFilter(containsVector)))
		}},
		{"a modify-by-path expression", func(cmd *baseCommand) Error {
			return cmd.writeOperationForOperation(ModifyByPath("l", EXP_PATH_MODIFY_DEFAULT, ExpListVal(vector), CtxListIndex(0)))
		}},
		{"UDF arguments", func(cmd *baseCommand) Error {
			return cmd.writeUdfArgs(NewValueArray([]Value{vector}))
		}},
	}

	gg.Context("when a node does not support vectors", func() {
		for _, testCase := range vectorWrites {
			gg.It("refuses "+testCase.name, func() {
				expectParameterError(testCase.write(commandWithoutVectorSupport()), unsupported)
			})
		}

		gg.It("still writes scalar and list bins", func() {
			cmd := commandWithoutVectorSupport()
			gm.Expect(cmd.writeOperationForBin(NewBin("n", 7), _WRITE)).To(gm.Succeed())
			gm.Expect(cmd.writeOperationForBin(NewBin("l", []any{1, "two"}), _WRITE)).To(gm.Succeed())
		})

		// Raw bytes are sent as given: the caller has chosen the particle.
		gg.It("still writes raw type-16 bytes", func() {
			raw := NewRawBlobValue(ParticleType.VECTOR, []byte{0xde, 0xad})
			gm.Expect(commandWithoutVectorSupport().writeOperationForBin(NewBin("v", raw), _WRITE)).To(gm.Succeed())
		})

		// Prebuilt expression bytes are opaque to the client.
		gg.It("still writes an expression built from base64", func() {
			encoded, err := containsVector.Base64()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			prebuilt, err := ExpFromBase64(encoded)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			size, err := prebuilt.size()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(commandWithoutVectorSupport().writeFilterExpression(prebuilt, size)).To(gm.Succeed())
		})
	})
})
