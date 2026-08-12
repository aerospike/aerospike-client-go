//go:build go1.27

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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// IndexValueType is the value type a secondary index covers.
type IndexValueType int

// The index value types.
const (
	// IndexValueUnset means no type was chosen yet.
	IndexValueUnset IndexValueType = iota
	// IndexValueNumeric indexes integers.
	IndexValueNumeric
	// IndexValueString indexes strings.
	IndexValueString
	// IndexValueGeo2DSphere indexes GeoJSON regions.
	IndexValueGeo2DSphere
	// IndexValueBlob indexes byte slices (server 7.0+).
	IndexValueBlob
)

// IndexBuilder creates and drops secondary indexes.
type IndexBuilder struct {
	session *Session
	ds      *DataSet

	binName       string
	indexName     string
	valueType     IndexValueType
	collection    as.IndexCollectionType
	hasCollection bool

	pendingErr error
}

// Index opens an index builder for a dataset.
func (s *Session) Index(ds *DataSet) *IndexBuilder {
	b := &IndexBuilder{session: s, ds: ds}
	if ds == nil {
		b.pendingErr = NewError(KindInvalidArgument, "dataset must not be nil")
	}
	return b
}

// OnBin indexes a bin.
func (b *IndexBuilder) OnBin(name string) *IndexBuilder { b.binName = name; return b }

// Named sets the index name. It is required to create or drop.
func (b *IndexBuilder) Named(name string) *IndexBuilder { b.indexName = name; return b }

// Numeric indexes integer values.
func (b *IndexBuilder) Numeric() *IndexBuilder { b.valueType = IndexValueNumeric; return b }

// String indexes string values.
func (b *IndexBuilder) String() *IndexBuilder { b.valueType = IndexValueString; return b }

// Geo2DSphere indexes GeoJSON values.
func (b *IndexBuilder) Geo2DSphere() *IndexBuilder { b.valueType = IndexValueGeo2DSphere; return b }

// Blob indexes byte-slice values. It requires server 7.0 or newer.
func (b *IndexBuilder) Blob() *IndexBuilder { b.valueType = IndexValueBlob; return b }

// Collection indexes inside a collection: list elements, map keys or map
// values.
func (b *IndexBuilder) Collection(t as.IndexCollectionType) *IndexBuilder {
	b.collection = t
	b.hasCollection = true
	return b
}

// coreIndexType maps the SDK value type onto the core one.
func (b *IndexBuilder) coreIndexType() (as.IndexType, error) {
	switch b.valueType {
	case IndexValueNumeric:
		return as.NUMERIC, nil
	case IndexValueString:
		return as.STRING, nil
	case IndexValueGeo2DSphere:
		return as.GEO2DSPHERE, nil
	case IndexValueBlob:
		return as.BLOB, nil
	default:
		return as.NUMERIC, NewError(KindInvalidArgument,
			"an index needs a value type: call Numeric, String, Geo2DSphere or Blob")
	}
}

// Create asks the server to build the index.
//
// It returns as soon as the request is accepted; wait for the build with
// OnComplete on the returned task.
func (b *IndexBuilder) Create() (*as.IndexTask, error) {
	if b.pendingErr != nil {
		return nil, b.pendingErr
	}
	if b.indexName == "" {
		return nil, NewError(KindInvalidArgument, "an index needs a name: call Named")
	}
	if b.binName == "" {
		return nil, NewError(KindInvalidArgument, "an index needs a bin: call OnBin")
	}
	idxType, err := b.coreIndexType()
	if err != nil {
		return nil, err
	}
	if b.valueType == IndexValueBlob && !b.session.client.SupportsBlobIndex() {
		return nil, NewError(KindInvalidArgument,
			"blob secondary indexes require server 7.0 or newer on every node")
	}

	core, err := b.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	if b.hasCollection {
		task, aerr := core.CreateComplexIndex(nil,
			b.ds.namespace, b.ds.setName, b.indexName, b.binName, idxType, b.collection)
		if aerr != nil {
			return nil, WrapError(aerr)
		}
		return task, nil
	}
	task, aerr := core.CreateIndex(nil, b.ds.namespace, b.ds.setName, b.indexName, b.binName, idxType)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// Drop removes the index by name.
func (b *IndexBuilder) Drop() error {
	if b.pendingErr != nil {
		return b.pendingErr
	}
	if b.indexName == "" {
		return NewError(KindInvalidArgument, "an index needs a name: call Named")
	}
	core, err := b.session.client.UnderlyingClient()
	if err != nil {
		return err
	}
	return wrapNilError(core.DropIndex(nil, b.ds.namespace, b.ds.setName, b.indexName))
}

// Info runs an info command against a random node.
func (s *Session) Info(command string) (map[string]string, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	nodes := core.GetNodes()
	if len(nodes) == 0 {
		return nil, NewError(KindConnection, "cluster has no nodes")
	}
	info, aerr := nodes[0].RequestInfo(as.NewInfoPolicy(), command)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return info, nil
}

// InfoOnAllNodes runs an info command on every node, keyed by node name.
func (s *Session) InfoOnAllNodes(command string) (map[string]map[string]string, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]string{}
	for _, n := range core.GetNodes() {
		info, aerr := n.RequestInfo(as.NewInfoPolicy(), command)
		if aerr != nil {
			return nil, WrapError(aerr)
		}
		out[n.GetName()] = info
	}
	return out, nil
}

// IsNamespaceSC reports whether a namespace runs in strong-consistency mode.
func (s *Session) IsNamespaceSC(namespace string) (bool, error) {
	return s.client.namespaceIsSC(namespace)
}
