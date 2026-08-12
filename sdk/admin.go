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
	"sort"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// --- User-defined function administration ---

// RegisterUDF registers a module from bytes. Wait for propagation with
// OnComplete on the returned task.
func (s *Session) RegisterUDF(body []byte, serverPath string, language as.Language) (*as.RegisterTask, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	task, aerr := core.RegisterUDF(nil, body, serverPath, language)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// RegisterUDFFromFile registers a module read from a local file.
func (s *Session) RegisterUDFFromFile(clientPath, serverPath string, language as.Language) (*as.RegisterTask, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	task, aerr := core.RegisterUDFFromFile(nil, clientPath, serverPath, language)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// RemoveUDF removes a module.
func (s *Session) RemoveUDF(serverPath string) (*as.RemoveTask, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	task, aerr := core.RemoveUDF(nil, serverPath)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// ListUDF reports the registered modules.
func (s *Session) ListUDF() ([]*as.UDF, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	mods, aerr := core.ListUDF(nil)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return mods, nil
}

// --- Index listing ---

// IndexInfo describes one secondary index as the cluster reports it.
type IndexInfo struct {
	Namespace      string
	Set            string
	Bin            string
	Name           string
	IndexValueType string
	IndexType      string
	Context        string
}

// ListIndexes reports the secondary indexes the cluster knows about,
// deduplicated across nodes.
func (s *Session) ListIndexes() ([]IndexInfo, error) {
	responses, err := s.InfoOnAllNodes("sindex-list:")
	if err != nil {
		// Older servers answer a differently-spelled command.
		responses, err = s.InfoOnAllNodes("sindex")
		if err != nil {
			return nil, err
		}
	}

	seen := map[string]IndexInfo{}
	for _, byCommand := range responses {
		for _, body := range byCommand {
			for _, entry := range strings.Split(body, ";") {
				if strings.TrimSpace(entry) == "" {
					continue
				}
				info := parseIndexEntry(entry)
				if info.Name == "" {
					continue
				}
				seen[info.Namespace+"|"+info.Set+"|"+info.Name] = info
			}
		}
	}

	out := make([]IndexInfo, 0, len(seen))
	for _, v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// parseIndexEntry parses one colon-separated sindex-list entry.
func parseIndexEntry(entry string) IndexInfo {
	var info IndexInfo
	for _, kv := range strings.Split(entry, ":") {
		name, value, found := strings.Cut(kv, "=")
		if !found {
			continue
		}
		switch name {
		case "ns":
			info.Namespace = value
		case "set":
			info.Set = value
		case "bin", "bins":
			info.Bin = value
		case "indexname", "name":
			info.Name = value
		case "type":
			info.IndexValueType = value
		case "indextype":
			info.IndexType = value
		case "context":
			info.Context = value
		}
	}
	return info
}

// --- Namespace consistency mode ---

// NamespaceScStatus reports whether a namespace runs in strong-consistency mode,
// with a human-readable reason when it does not.
type NamespaceScStatus struct {
	IsSC   bool
	Detail string
}

// NamespaceScStatus reports a namespace's consistency mode and why.
func (s *Session) NamespaceScStatus(namespace string) (NamespaceScStatus, error) {
	core, err := s.client.UnderlyingClient()
	if err != nil {
		return NamespaceScStatus{}, err
	}
	nodes := core.GetNodes()
	if len(nodes) == 0 {
		return NamespaceScStatus{}, NewError(KindConnection, "cluster has no nodes")
	}

	cmd := "namespace/" + namespace
	for _, n := range nodes {
		info, aerr := n.RequestInfo(as.NewInfoPolicy(), cmd)
		if aerr != nil {
			continue
		}
		exists, sc := parseNamespaceInfoBody(info[cmd])
		if !exists {
			continue
		}
		if sc {
			return NamespaceScStatus{IsSC: true,
				Detail: "namespace " + namespace + " runs in strong-consistency mode"}, nil
		}
		return NamespaceScStatus{IsSC: false,
			Detail: "namespace " + namespace + " runs in availability mode; " +
				"multi-record transactions need a strong-consistency namespace"}, nil
	}
	return NamespaceScStatus{IsSC: false,
		Detail: "namespace " + namespace + " is unknown to every node"}, nil
}

// --- Typed user-defined function results ---

// FirstUDFResultAs scans forward for the first row carrying a user-defined
// function result and maps it to T.
//
// The Lua value must be a map with string keys, which is fed through the same
// mapping the typed layer uses for records.
func FirstUDFResultAs[T any](s *RecordStream) (*T, error) {
	for {
		row, err := s.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		if row.UDFResult != nil {
			return UDFResultAsObject[T](row)
		}
	}
}

// UDFResultAsObject maps a row's user-defined function result to T.
//
// A nil result reports no object. A non-map result, or a map with non-string
// keys, is an error.
func UDFResultAsObject[T any](row *RecordResult) (*T, error) {
	if row == nil || row.UDFResult == nil {
		return nil, nil
	}
	raw := row.UDFResult.GetObject()
	if raw == nil {
		return nil, nil
	}

	bins := as.BinMap{}
	switch m := raw.(type) {
	case map[any]any:
		for k, v := range m {
			name, ok := k.(string)
			if !ok {
				return nil, NewError(KindInvalidArgument,
					"a user-defined function result mapped to an object needs string keys, got %T", k)
			}
			bins[name] = v
		}
	case map[string]any:
		for k, v := range m {
			bins[k] = v
		}
	case []as.MapPair:
		for _, p := range m {
			name, ok := p.Key.(string)
			if !ok {
				return nil, NewError(KindInvalidArgument,
					"a user-defined function result mapped to an object needs string keys, got %T", p.Key)
			}
			bins[name] = p.Value
		}
	default:
		return nil, NewError(KindInvalidArgument,
			"a user-defined function result mapped to an object must be a map, got %T", raw)
	}

	// Reuse the typed layer's mapping by presenting the Lua map as a record.
	synthetic := &RecordResult{
		Key:        row.Key,
		Record:     &as.Record{Key: row.Key, Bins: bins},
		ResultCode: row.ResultCode,
	}
	return objectFromRecord[T](synthetic)
}

// FirstUDFResultAs is the method form, available because the type parameter is
// explicit at the call site.
func (s *RecordStream) FirstUDFResultAs[T any]() (*T, error) { return FirstUDFResultAs[T](s) }

// UDFResultAsObject is the method form of the package-level function.
func (r *RecordResult) UDFResultAsObject[T any]() (*T, error) { return UDFResultAsObject[T](r) }
