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
	"fmt"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// VectorDistanceMetric selects how [ExpVectorDistance] compares two vectors.
type VectorDistanceMetric int

const (
	// VectorEuclidean is the squared Euclidean (L2) distance, without
	// taking the square root. Smaller values indicate closer vectors.
	VectorEuclidean VectorDistanceMetric = iota
	// VectorDotProduct is the dot product. Larger values indicate closer vectors.
	VectorDotProduct
	// VectorCosine is cosine similarity. It returns 1 for vectors
	// pointing in the same direction and 0 if either vector is all zeros.
	// Larger values indicate greater similarity.
	VectorCosine
)

// expOp returns the server expression op for the distance.
func (distance VectorDistanceMetric) expOp() *expOp {
	switch distance {
	case VectorEuclidean:
		return &expOpVectorEuclidean
	case VectorDotProduct:
		return &expOpVectorDotProduct
	case VectorCosine:
		return &expOpVectorCosine
	}
	panic(newError(types.PARAMETER_ERROR, fmt.Sprintf("Invalid VectorDistanceMetric: %d", distance)))
}

// ExpVectorDistance compares query with a vector bin expression and returns
// a float (the server computes in float32). Use it in a filter or an
// [ExpReadOp]:
//
//	as.ExpVectorDistance(as.VectorCosine, query, as.ExpVectorBin("embedding"))
//
// Unknown (missing bin, not a vector, or type/dimensions mismatch): a filter
// skips the record; [ExpReadOp] returns OP_NOT_APPLICABLE, or nil if
// [ExpReadFlagEvalNoFail] is set.
//
// Packing is PARAMETER_ERROR if query is nil or the cluster has no vector
// support. Panics on an unknown metric.
func ExpVectorDistance(distance VectorDistanceMetric, query *Vector, bin *Expression) *Expression {
	return newFilterExpression(
		distance.expOp(),
		query,
		bin,
		nil,
		nil,
		nil,
	)
}

// packVectorDistance packs [op, bin, query]. The server only accepts the query
// as a BLOB holding the serialized vector, the vector particle type is
// rejected there.
func (filterExp *Expression) packVectorDistance(buf BufferEx) (int, Error) {
	query, _ := filterExp.val.(*Vector)
	if query == nil {
		return 0, newError(types.PARAMETER_ERROR, "vector: distance query must not be nil")
	}
	if err := markVector(buf); err != nil {
		return 0, err
	}

	size := 0
	sz, err := packArrayBegin(buf, 3)
	size += sz
	if err != nil {
		return size, err
	}
	sz, err = packAInt64(buf, int64(*filterExp.cmd))
	size += sz
	if err != nil {
		return size, err
	}
	sz, err = filterExp.bin.pack(buf)
	size += sz
	if err != nil {
		return size, err
	}
	sz, err = packBytes(buf, query.wire)
	size += sz
	return size, err
}
