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

package aerospike

import (
	"fmt"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// Filter specifies a query filter definition.
type Filter struct {
	name              string
	indexName         string
	idxType           IndexCollectionType
	valueParticleType int
	begin             Value
	end               Value
	ctx               []*CDTContext
	expression        *Expression
}

// NewEqualFilter creates a new equality filter instance for query.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewEqualFilter(binName string, value interface{}, ctx ...*CDTContext) *Filter {
	val := NewValue(value)
	return newFilter(binName, "", ICT_DEFAULT, val.GetType(), val, val, ctx, nil)
}

// NewEqualWithExpressionFilter creates an equality filter for query with an expression.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewEqualWithExpressionFilter(expression *Expression, value interface{}) *Filter {
	v := NewValue(value)
	return newFilter("", "", ICT_DEFAULT, v.GetType(), v, v, nil, expression)
}

// NewEqualWithIndexNameFilter creates an equality filter for query with an index name.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewEqualWithIndexNameFilter(indexName string, value interface{}) *Filter {
	v := NewValue(value)
	return newFilter("", indexName, ICT_DEFAULT, v.GetType(), v, v, nil, nil)
}

// NewRangeFilter creates a range filter for query.
// Range arguments must be int64 values.
// String ranges are not supported.
func NewRangeFilter(binName string, begin int64, end int64, ctx ...*CDTContext) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter(binName, "", ICT_DEFAULT, vBegin.GetType(), vBegin, vEnd, ctx, nil)
}

// NewRangeWithExpressionFilter creates a range filter for query with an expression.
// Range arguments must be int64 values.
// String ranges are not supported.
func NewRangeWithExpressionFilter(expression *Expression, begin int64, end int64) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter("", "", ICT_DEFAULT, vBegin.GetType(), vBegin, vEnd, nil, expression)
}

// NewRangeWithIndexNameFilter creates a range filter for query with an index name.
// Range arguments must be int64 values.
// String ranges are not supported.
func NewRangeWithIndexNameFilter(indexName string, begin int64, end int64) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter("", indexName, ICT_DEFAULT, vBegin.GetType(), vBegin, vEnd, nil, nil)
}

// NewContainsFilter creates a contains filter for query on collection index.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewContainsFilter(binName string, indexCollectionType IndexCollectionType, value interface{}, ctx ...*CDTContext) *Filter {
	v := NewValue(value)
	return newFilter(binName, "", indexCollectionType, v.GetType(), v, v, ctx, nil)
}

// NewContainsWithExpressionFilter creates a contains filter for query on collection index with an expression.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewContainsWithExpressionFilter(expression *Expression, indexCollectionType IndexCollectionType, value interface{}) *Filter {
	v := NewValue(value)
	return newFilter("", "", indexCollectionType, v.GetType(), v, v, nil, expression)
}

// NewContainsWithIndexNameFilter creates a contains filter for query on collection index with an index name.
// Value can be an integer, string or a blob (byte array). Byte arrays are only supported on server v7+.
func NewContainsWithIndexNameFilter(indexName string, indexCollectionType IndexCollectionType, value interface{}) *Filter {
	v := NewValue(value)
	return newFilter("", indexName, indexCollectionType, v.GetType(), v, v, nil, nil)
}

// NewContainsRangeFilter creates a contains filter for query on ranges of data in a collection index.
func NewContainsRangeFilter(binName string, indexCollectionType IndexCollectionType, begin, end int64, ctx ...*CDTContext) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter(binName, "", indexCollectionType, vBegin.GetType(), vBegin, vEnd, ctx, nil)
}

// NewContainsRangeWithExpressionFilter creates a contains filter for query on ranges of data in a collection index with an expression.
func NewContainsRangeWithExpressionFilter(expression *Expression, collectionType IndexCollectionType, begin, end int64) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter("", "", collectionType, vBegin.GetType(), vBegin, vEnd, nil, expression)
}

// NewContainsRangeWithIndexNameFilter creates a contains filter for query on ranges of data in a collection index with an index name.
func NewContainsRangeWithIndexNameFilter(indexName string, collectionType IndexCollectionType, begin, end int64) *Filter {
	vBegin, vEnd := NewValue(begin), NewValue(end)
	return newFilter("", indexName, collectionType, vBegin.GetType(), vBegin, vEnd, nil, nil)
}

// NewGeoWithinRegionFilter creates a geospatial "within region" filter for query.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionFilter(binName, region string, ctx ...*CDTContext) *Filter {
	v := NewStringValue(region)
	return newFilter(binName, "", ICT_DEFAULT, ParticleType.GEOJSON, v, v, ctx, nil)
}

// NewGeoWithinRegionWithExpressionFilter creates a geospatial "within region" filter for query with an expression.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionWithExpressionFilter(expression *Expression, region string) *Filter {
	v := NewStringValue(region)
	return newFilter("", "", ICT_DEFAULT, ParticleType.GEOJSON, v, v, nil, expression)
}

// NewGeoWithinRegionWithIndexNameFilter creates a geospatial "within region" filter for query with an index name.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionWithIndexNameFilter(indexName string, region string) *Filter {
	v := NewStringValue(region)
	return newFilter("", indexName, ICT_DEFAULT, ParticleType.GEOJSON, v, v, nil, nil)
}

// NewGeoWithinRegionForCollectionFilter creates a geospatial "within region" filter for query on collection index.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionForCollectionFilter(binName string, collectionType IndexCollectionType, region string, ctx ...*CDTContext) *Filter {
	v := NewStringValue(region)
	return newFilter(binName, "", collectionType, ParticleType.GEOJSON, v, v, ctx, nil)
}

// NewGeoWithinRegionForCollectionWithExpressionFilter creates a geospatial "within region" filter for query on collection index with an expression.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionForCollectionWithExpressionFilter(expression *Expression, collectionType IndexCollectionType, region string) *Filter {
	v := NewStringValue(region)
	return newFilter("", "", collectionType, ParticleType.GEOJSON, v, v, nil, expression)
}

// NewGeoWithinRegionForCollectionWithIndexNameFilter creates a geospatial "within region" filter for query on collection index with an index name.
// Argument must be a valid GeoJSON region.
func NewGeoWithinRegionForCollectionWithIndexNameFilter(indexName string, collectionType IndexCollectionType, region string) *Filter {
	v := NewStringValue(region)
	return newFilter("", indexName, collectionType, ParticleType.GEOJSON, v, v, nil, nil)
}

// NewGeoRegionsContainingPointFilter creates a geospatial "containing point" filter for query.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointFilter(binName, point string, ctx ...*CDTContext) *Filter {
	v := NewStringValue(point)
	return newFilter(binName, "", ICT_DEFAULT, ParticleType.GEOJSON, v, v, ctx, nil)
}

// NewGeoRegionsContainingPointWithExpressionFilter creates a geospatial "containing point" filter for query with an expression.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointWithExpressionFilter(expression *Expression, point string) *Filter {
	v := NewStringValue(point)
	return newFilter("", "", ICT_DEFAULT, ParticleType.GEOJSON, v, v, nil, expression)
}

// NewGeoRegionsContainingPointWithIndexNameFilter creates a geospatial "containing point" filter for query with an index name.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointWithIndexNameFilter(indexName string, point string) *Filter {
	v := NewStringValue(point)
	return newFilter("", indexName, ICT_DEFAULT, ParticleType.GEOJSON, v, v, nil, nil)
}

// NewGeoRegionsContainingPointForCollectionFilter creates a geospatial "containing point" filter for query on collection index.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointForCollectionFilter(binName string, collectionType IndexCollectionType, point string, ctx ...*CDTContext) *Filter {
	v := NewStringValue(point)
	return newFilter(binName, "", collectionType, ParticleType.GEOJSON, v, v, ctx, nil)
}

// NewGeoRegionsContainingPointForCollectionWithExpressionFilter creates a geospatial "containing point" filter for query on collection index with an expression.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointForCollectionWithExpressionFilter(expression *Expression, collectionType IndexCollectionType, point string) *Filter {
	v := NewStringValue(point)
	return newFilter("", "", collectionType, ParticleType.GEOJSON, v, v, nil, expression)
}

// NewGeoRegionsContainingPointForCollectionWithIndexNameFilter creates a geospatial "containing point" filter for query on collection index with an index name.
// Argument must be a valid GeoJSON point.
func NewGeoRegionsContainingPointForCollectionWithIndexNameFilter(indexName string, collectionType IndexCollectionType, point string) *Filter {
	v := NewStringValue(point)
	return newFilter("", indexName, collectionType, ParticleType.GEOJSON, v, v, nil, nil)
}

// NewGeoWithinRadiusFilter creates a geospatial "within radius" filter for query.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusFilter(binName string, lng, lat, radius float64, ctx ...*CDTContext) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter(binName, "", ICT_DEFAULT, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), ctx, nil)
}

// NewGeoWithinRadiusWithExpressionFilter creates a geospatial "within radius" filter for query with an expression.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusWithExpressionFilter(expression *Expression, lng, lat, radius float64) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter("", "", ICT_DEFAULT, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), nil, expression)
}

// NewGeoWithinRadiusWithIndexNameFilter creates a geospatial "within radius" filter for query with an index name.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusWithIndexNameFilter(indexName string, lng, lat, radius float64) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter("", indexName, ICT_DEFAULT, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), nil, nil)
}

// NewGeoWithinRadiusForCollectionFilter creates a geospatial "within radius" filter for query on collection index.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusForCollectionFilter(binName string, collectionType IndexCollectionType, lng, lat, radius float64, ctx ...*CDTContext) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter(binName, "", collectionType, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), ctx, nil)
}

// NewGeoWithinRadiusForCollectionWithExpressionFilter creates a geospatial "within radius" filter for query on collection index with an expression.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusForCollectionWithExpressionFilter(expression *Expression, collectionType IndexCollectionType, lng, lat, radius float64) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter("", "", collectionType, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), nil, expression)
}

// NewGeoWithinRadiusForCollectionWithIndexNameFilter creates a geospatial "within radius" filter for query on collection index with an index name.
// Arguments must be valid longitude/latitude/radius (meters) values.
func NewGeoWithinRadiusForCollectionWithIndexNameFilter(indexName string, collectionType IndexCollectionType, lng, lat, radius float64) *Filter {
	rgnStr := fmt.Sprintf("{ \"type\": \"AeroCircle\", "+"\"coordinates\": [[%.8f, %.8f], %f] }", lng, lat, radius)
	return newFilter("", indexName, collectionType, ParticleType.GEOJSON, NewValue(rgnStr), NewValue(rgnStr), nil, nil)
}

// Create a filter for query.
// Range arguments must be longs or integers which can be cast to longs.
// String ranges are not supported.
func NewFilter(name string, indexCollectionType IndexCollectionType, valueParticleType int, begin Value, end Value, ctx []*CDTContext) *Filter {
	return newFilter(name, "", indexCollectionType, valueParticleType, begin, end, ctx, nil)
}

// NewWithExpressionFilter creates a filter for query with an expression.
// Range arguments must be longs or integers which can be cast to longs.
// String ranges are not supported.
func NewWithExpressionFilter(expression *Expression, indexCollectionType IndexCollectionType, valueParticleType int, begin Value, end Value) *Filter {
	return newFilter("", "", indexCollectionType, valueParticleType, begin, end, nil, expression)
}

// NewWithIndexNameFilter creates a filter for query with an index name.
// Range arguments must be longs or integers which can be cast to longs.
// String ranges are not supported.
func NewWithIndexNameFilter(indexName string, indexCollectionType IndexCollectionType, valueParticleType int, begin Value, end Value) *Filter {
	return newFilter("", indexName, indexCollectionType, valueParticleType, begin, end, nil, nil)
}

// newFilter creates a filter for query.
func newFilter(name string, indexName string, indexCollectionType IndexCollectionType, valueParticleType int, begin Value, end Value, ctx []*CDTContext, expression *Expression) *Filter {
	return &Filter{
		name:              name,
		indexName:         indexName,
		idxType:           indexCollectionType,
		valueParticleType: valueParticleType,
		begin:             begin,
		end:               end,
		ctx:               ctx,
		expression:        expression,
	}
}

func (fltr *Filter) String() string {
	return fmt.Sprintf("Filter: {name: %s, indexName: %s, index type: %s, value particle type: %d, begin: %s, end: %s, context: %v, expression: %v}",
		fltr.name,
		fltr.indexName,
		fltr.idxType,
		fltr.valueParticleType,
		fltr.begin,
		fltr.end,
		fltr.ctx,
		fltr.expression,
	)
}

// IndexCollectionType returns filter's index type.
func (fltr *Filter) IndexCollectionType() IndexCollectionType {
	return fltr.idxType
}

// EstimateSize will estimate the size of the filter for wire protocol
func (fltr *Filter) EstimateSize() (int, Error) {
	// bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
	szBegin, err := fltr.begin.EstimateSize()
	if err != nil {
		return szBegin, err
	}

	szEnd, err := fltr.end.EstimateSize()
	if err != nil {
		return szEnd, err
	}

	return len(fltr.name) + szBegin + szEnd + 10, nil
}

// Retrieve packed Context.
// For internal use only.
func (fltr *Filter) packCtx(cmd BufferEx) (sz int, err Error) {
	if len(fltr.ctx) > 0 {
		sz, err = cdtContextList(fltr.ctx).packArray(cmd)
	}
	return sz, err
}

// Retrieve packed Context size.
// For internal use only.
func (fltr *Filter) estimatePackedCtxSize() (sz int, err Error) {
	if len(fltr.ctx) > 0 {
		sz, err = cdtContextList(fltr.ctx).packArray(nil)
	}
	return sz, err
}

func (fltr *Filter) write(cmd *baseCommand) (int, Error) {
	size := 0

	// Write name length
	cmd.WriteByte(byte(len(fltr.name)))
	size++

	// Write Name
	n, err := cmd.WriteString(fltr.name)
	if err != nil {
		return size + n, err
	}
	size += n

	// Write particle type.
	cmd.WriteByte(byte(fltr.valueParticleType))
	size++

	// Write filter begin.
	esz, err := fltr.begin.EstimateSize()
	if err != nil {
		return size, err
	}

	n = cmd.WriteInt32(int32(esz))
	size += n

	n, err = fltr.begin.write(cmd)
	if err != nil {
		return size + n, err
	}
	size += n

	// Write filter end.
	esz, err = fltr.end.EstimateSize()
	if err != nil {
		return size, err
	}

	n = cmd.WriteInt32(int32(esz))
	size += n

	n, err = fltr.end.write(cmd)
	if err != nil {
		return size + n, err
	}
	size += n

	return size, nil
}
