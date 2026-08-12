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
	"strconv"
	"strings"
)

// InfoStats is a parsed key=value info body.
//
// Lookups are dash- and underscore-flexible, because the server mixes the two
// between configuration and statistic keys: Get("stop-writes") also finds
// "stop_writes".
type InfoStats struct {
	raw map[string]string
}

// ParseInfoStats splits a key=value body on the given pair separator.
//
// Use ";" for a whole-response body such as namespace/<ns>, and ":" for the
// entries of a multi-item response such as sets or sindex-list.
func ParseInfoStats(body, pairSep string) InfoStats {
	out := InfoStats{raw: map[string]string{}}
	for _, pair := range strings.Split(body, pairSep) {
		name, value, found := strings.Cut(pair, "=")
		if !found {
			continue
		}
		out.raw[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	return out
}

// normalizeKey folds dashes and underscores together.
func normalizeKey(k string) string { return strings.ReplaceAll(k, "-", "_") }

// Get reports a value and whether the key was present.
func (s InfoStats) Get(key string) (string, bool) {
	if v, ok := s.raw[key]; ok {
		return v, true
	}
	want := normalizeKey(key)
	for k, v := range s.raw {
		if normalizeKey(k) == want {
			return v, true
		}
	}
	return "", false
}

// GetInt reports a value parsed as an integer.
func (s InfoStats) GetInt(key string) (int64, bool) {
	v, ok := s.Get(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil
}

// GetFloat reports a value parsed as a floating-point number.
func (s InfoStats) GetFloat(key string) (float64, bool) {
	v, ok := s.Get(key)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

// GetBool reports a value parsed as a boolean.
func (s InfoStats) GetBool(key string) (bool, bool) {
	v, ok := s.Get(key)
	if !ok {
		return false, false
	}
	return strings.EqualFold(v, "true"), true
}

// Len reports how many keys the body held.
func (s InfoStats) Len() int { return len(s.raw) }

// IsEmpty reports whether the body held nothing.
func (s InfoStats) IsEmpty() bool { return len(s.raw) == 0 }

// Raw reports the parsed map.
func (s InfoStats) Raw() map[string]string { return s.raw }

// MergeStrategy selects how one statistic is combined across nodes.
type MergeStrategy int

// The merge strategies.
const (
	// MergeSum adds the values.
	MergeSum MergeStrategy = iota
	// MergeAverage averages the values.
	MergeAverage
	// MergeMinimum takes the smallest value.
	MergeMinimum
	// MergeMaximum takes the largest value.
	MergeMaximum
	// MergeAnd requires every node to report true.
	MergeAnd
	// MergeOr accepts any node reporting true.
	MergeOr
	// MergeMostCommon takes the most frequent value, first seen winning ties.
	MergeMostCommon
	// MergeMustMatch treats disagreement as an error.
	MergeMustMatch
	// MergeFirst takes the first value.
	MergeFirst
)

// MergeStatValues combines one statistic's per-node values.
//
// The numeric strategies fall back to [MergeMostCommon] when the values do not
// parse as numbers, matching the other SDKs' leniency.
func MergeStatValues(key string, values []string, strategy MergeStrategy) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	switch strategy {
	case MergeFirst:
		return values[0], nil

	case MergeMustMatch:
		for _, v := range values[1:] {
			if v != values[0] {
				return "", NewError(KindAerospike,
					"nodes disagree on %q: %q and %q", key, values[0], v)
			}
		}
		return values[0], nil

	case MergeAnd, MergeOr:
		result := strategy == MergeAnd
		for _, v := range values {
			b := strings.EqualFold(v, "true")
			if strategy == MergeAnd {
				result = result && b
			} else {
				result = result || b
			}
		}
		return strconv.FormatBool(result), nil

	case MergeSum, MergeAverage, MergeMinimum, MergeMaximum:
		nums := make([]float64, 0, len(values))
		allInt := true
		for _, v := range values {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return MergeStatValues(key, values, MergeMostCommon)
			}
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				allInt = false
			}
			nums = append(nums, f)
		}
		var out float64
		switch strategy {
		case MergeSum:
			for _, n := range nums {
				out += n
			}
		case MergeAverage:
			for _, n := range nums {
				out += n
			}
			out /= float64(len(nums))
			allInt = false
		case MergeMinimum:
			out = nums[0]
			for _, n := range nums[1:] {
				out = min(out, n)
			}
		case MergeMaximum:
			out = nums[0]
			for _, n := range nums[1:] {
				out = max(out, n)
			}
		}
		if allInt {
			return strconv.FormatInt(int64(out), 10), nil
		}
		return strconv.FormatFloat(out, 'f', -1, 64), nil

	default: // MergeMostCommon
		counts := map[string]int{}
		order := []string{}
		for _, v := range values {
			if _, seen := counts[v]; !seen {
				order = append(order, v)
			}
			counts[v]++
		}
		best := order[0]
		for _, v := range order {
			if counts[v] > counts[best] {
				best = v
			}
		}
		return best, nil
	}
}

// MergeInfoStats combines per-node stat bodies into one.
//
// With no override for a key, the strategy is sniffed from the value shape:
// integers sum, floating-point values average, booleans require every node to
// agree, and anything else takes the most common value. Override keys match
// dash- and underscore-insensitively.
func MergeInfoStats(perNode []InfoStats, overrides map[string]MergeStrategy) (InfoStats, error) {
	values := map[string][]string{}
	var order []string
	for _, s := range perNode {
		for k, v := range s.raw {
			if _, seen := values[k]; !seen {
				order = append(order, k)
			}
			values[k] = append(values[k], v)
		}
	}

	normalizedOverrides := map[string]MergeStrategy{}
	for k, v := range overrides {
		normalizedOverrides[normalizeKey(k)] = v
	}

	out := InfoStats{raw: map[string]string{}}
	for _, k := range order {
		vals := values[k]
		strategy, ok := normalizedOverrides[normalizeKey(k)]
		if !ok {
			strategy = sniffStrategy(vals)
		}
		merged, err := MergeStatValues(k, vals, strategy)
		if err != nil {
			return out, err
		}
		out.raw[k] = merged
	}
	return out, nil
}

// sniffStrategy picks a default from the value shape.
func sniffStrategy(values []string) MergeStrategy {
	if len(values) == 0 {
		return MergeMostCommon
	}
	allInt, allFloat, allBool := true, true, true
	for _, v := range values {
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			allFloat = false
		}
		if !strings.EqualFold(v, "true") && !strings.EqualFold(v, "false") {
			allBool = false
		}
	}
	switch {
	case allBool:
		return MergeAnd
	case allInt:
		return MergeSum
	case allFloat:
		return MergeAverage
	default:
		return MergeMostCommon
	}
}

// NamespaceDetail is a namespace's statistics.
//
// It deliberately does not mirror every field the server may report: the
// namespace response varies by version, so the type wraps the whole stat map
// and names only the commonly-used core. Reach anything else through Stats.
type NamespaceDetail struct {
	stats InfoStats
}

// Stats reports the whole parsed stat map.
func (n NamespaceDetail) Stats() InfoStats { return n.stats }

// Objects reports the record count.
func (n NamespaceDetail) Objects() (int64, bool) { return n.stats.GetInt("objects") }

// Tombstones reports the tombstone count.
func (n NamespaceDetail) Tombstones() (int64, bool) { return n.stats.GetInt("tombstones") }

// MasterObjects reports the master record count.
func (n NamespaceDetail) MasterObjects() (int64, bool) { return n.stats.GetInt("master_objects") }

// DataUsedBytes reports the bytes of data in use.
func (n NamespaceDetail) DataUsedBytes() (int64, bool) { return n.stats.GetInt("data_used_bytes") }

// ReplicationFactor reports the configured replication factor.
func (n NamespaceDetail) ReplicationFactor() (int64, bool) {
	return n.stats.GetInt("replication-factor")
}

// EffectiveReplicationFactor reports the effective replication factor.
func (n NamespaceDetail) EffectiveReplicationFactor() (int64, bool) {
	return n.stats.GetInt("effective_replication_factor")
}

// DefaultTTL reports the namespace's default expiration in seconds.
func (n NamespaceDetail) DefaultTTL() (int64, bool) { return n.stats.GetInt("default-ttl") }

// StopWrites reports whether the namespace has stopped accepting writes.
func (n NamespaceDetail) StopWrites() (bool, bool) { return n.stats.GetBool("stop_writes") }

// StrongConsistency reports whether the namespace runs in strong-consistency
// mode.
func (n NamespaceDetail) StrongConsistency() (bool, bool) {
	return n.stats.GetBool("strong-consistency")
}

// ClusterSize reports the namespace's cluster size.
func (n NamespaceDetail) ClusterSize() (int64, bool) { return n.stats.GetInt("ns_cluster_size") }

// StorageEngine reports the configured storage engine.
func (n NamespaceDetail) StorageEngine() (string, bool) { return n.stats.Get("storage-engine") }

// namespaceMergeOverrides are the strategies that differ from the sniffed
// default, matching the other SDKs' annotations.
var namespaceMergeOverrides = map[string]MergeStrategy{
	"effective_replication_factor": MergeAverage,
	"replication-factor":           MergeAverage,
	"migrate-sleep":                MergeAverage,
	"stop_writes":                  MergeOr,
	"hwm_breached":                 MergeOr,
}

// SetDetail is one entry of the sets response.
type SetDetail struct {
	Namespace     string
	Set           string
	Objects       int64
	Tombstones    int64
	DataUsedBytes int64
	DefaultTTL    int64
	StopWrites    int64
	Truncating    bool
	EnableIndex   bool
}

// IndexState is a secondary index's build state.
type IndexState string

// The index states.
const (
	// IndexWriteOnly means the index is still populating.
	IndexWriteOnly IndexState = "WO"
	// IndexReadWrite means the index is usable.
	IndexReadWrite IndexState = "RW"
)

// Sindex is one entry of the secondary-index listing.
type Sindex struct {
	Namespace      string
	IndexName      string
	Set            string
	Bin            string
	IndexType      string
	CollectionType string
	Context        string
	State          IndexState
}

// InfoCommands is the info surface bound to a session.
type InfoCommands struct {
	session *Session
}

// InfoCommands returns the info surface.
func (s *Session) InfoCommands() *InfoCommands { return &InfoCommands{session: s} }

// Namespaces reports the namespace names, sorted.
func (i *InfoCommands) Namespaces() ([]string, error) {
	info, err := i.session.Info("namespaces")
	if err != nil {
		return nil, err
	}
	names := splitNonEmpty(info["namespaces"], ";")
	sort.Strings(names)
	return names, nil
}

// Build reports the build versions across the cluster, sorted.
func (i *InfoCommands) Build() ([]string, error) {
	responses, err := i.session.InfoOnAllNodes("build")
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, byCommand := range responses {
		if v, ok := byCommand["build"]; ok && v != "" {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out, nil
}

// Sets reports the set names of a namespace, sorted.
func (i *InfoCommands) Sets(namespace string) ([]string, error) {
	details, err := i.SetDetails(namespace)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(details))
	for _, d := range details {
		names = append(names, d.Set)
	}
	sort.Strings(names)
	return names, nil
}

// ClusterSize reports how many nodes the cluster holds.
func (i *InfoCommands) ClusterSize() (int, error) {
	core, err := i.session.client.UnderlyingClient()
	if err != nil {
		return 0, err
	}
	return len(core.GetNodes()), nil
}

// IsClusterStable reports whether every node calls the cluster stable.
func (i *InfoCommands) IsClusterStable() (bool, error) {
	responses, err := i.session.InfoOnAllNodes("cluster-stable")
	if err != nil {
		return false, err
	}
	if len(responses) == 0 {
		return false, nil
	}
	for _, byCommand := range responses {
		body := byCommand["cluster-stable"]
		if strings.HasPrefix(body, "ERROR") {
			return false, nil
		}
	}
	return true, nil
}

// NamespaceDetail reports a namespace's statistics, merged across nodes.
//
// It reports absence when no node knows the namespace.
func (i *InfoCommands) NamespaceDetail(namespace string) (*NamespaceDetail, error) {
	perNode, err := i.NamespaceDetailPerNode(namespace)
	if err != nil {
		return nil, err
	}
	stats := make([]InfoStats, 0, len(perNode))
	for _, d := range perNode {
		if d != nil {
			stats = append(stats, d.stats)
		}
	}
	if len(stats) == 0 {
		return nil, nil
	}
	merged, err := MergeInfoStats(stats, namespaceMergeOverrides)
	if err != nil {
		return nil, err
	}
	return &NamespaceDetail{stats: merged}, nil
}

// NamespaceDetailPerNode reports a namespace's statistics per node.
func (i *InfoCommands) NamespaceDetailPerNode(namespace string) (map[string]*NamespaceDetail, error) {
	cmd := "namespace/" + namespace
	responses, err := i.session.InfoOnAllNodes(cmd)
	if err != nil {
		return nil, err
	}
	out := map[string]*NamespaceDetail{}
	for node, byCommand := range responses {
		body := byCommand[cmd]
		if body == "" || strings.HasPrefix(body, "ERROR") {
			out[node] = nil
			continue
		}
		stats := ParseInfoStats(body, ";")
		if v, ok := stats.Get("type"); ok && v == "unknown" {
			out[node] = nil
			continue
		}
		out[node] = &NamespaceDetail{stats: stats}
	}
	return out, nil
}

// SetDetails reports the sets of a namespace, merged across nodes. An empty
// namespace reports every set.
func (i *InfoCommands) SetDetails(namespace string) ([]SetDetail, error) {
	cmd := "sets"
	if namespace != "" {
		cmd = "sets/" + namespace
	}
	responses, err := i.session.InfoOnAllNodes(cmd)
	if err != nil {
		return nil, err
	}

	grouped := map[string][]InfoStats{}
	var order []string
	for _, byCommand := range responses {
		for _, entry := range splitNonEmpty(byCommand[cmd], ";") {
			stats := ParseInfoStats(entry, ":")
			ns, _ := stats.Get("ns")
			set, _ := stats.Get("set")
			key := ns + "|" + set
			if _, seen := grouped[key]; !seen {
				order = append(order, key)
			}
			grouped[key] = append(grouped[key], stats)
		}
	}

	out := make([]SetDetail, 0, len(order))
	for _, key := range order {
		merged, err := MergeInfoStats(grouped[key], map[string]MergeStrategy{
			"truncating":   MergeOr,
			"enable-index": MergeAnd,
		})
		if err != nil {
			return nil, err
		}
		ns, _ := merged.Get("ns")
		set, _ := merged.Get("set")
		objects, _ := merged.GetInt("objects")
		tombstones, _ := merged.GetInt("tombstones")
		used, _ := merged.GetInt("data_used_bytes")
		ttl, _ := merged.GetInt("default-ttl")
		stopWrites, _ := merged.GetInt("stop-writes-count")
		truncating, _ := merged.GetBool("truncating")
		enableIndex, _ := merged.GetBool("enable-index")
		out = append(out, SetDetail{
			Namespace: ns, Set: set,
			Objects: objects, Tombstones: tombstones, DataUsedBytes: used,
			DefaultTTL: ttl, StopWrites: stopWrites,
			Truncating: truncating, EnableIndex: enableIndex,
		})
	}
	return out, nil
}

// SetDetail reports one set's statistics, or absence.
func (i *InfoCommands) SetDetail(namespace, set string) (*SetDetail, error) {
	details, err := i.SetDetails(namespace)
	if err != nil {
		return nil, err
	}
	for idx := range details {
		if details[idx].Set == set {
			return &details[idx], nil
		}
	}
	return nil, nil
}

// SindexList reports the secondary indexes of a namespace, merged across nodes.
//
// A merged index reads as write-only while *any* node is still populating it.
func (i *InfoCommands) SindexList(namespace string) ([]Sindex, error) {
	cmd := "sindex-list:"
	if namespace != "" {
		cmd = "sindex-list:namespace=" + namespace
	}
	responses, err := i.session.InfoOnAllNodes(cmd)
	if err != nil {
		return nil, err
	}

	grouped := map[string][]InfoStats{}
	var order []string
	for _, byCommand := range responses {
		for _, entry := range splitNonEmpty(byCommand[cmd], ";") {
			stats := ParseInfoStats(entry, ":")
			ns, _ := stats.Get("ns")
			name, _ := stats.Get("indexname")
			key := ns + "|" + name
			if _, seen := grouped[key]; !seen {
				order = append(order, key)
			}
			grouped[key] = append(grouped[key], stats)
		}
	}

	out := make([]Sindex, 0, len(order))
	for _, key := range order {
		group := grouped[key]
		merged, err := MergeInfoStats(group, map[string]MergeStrategy{
			"type":      MergeMustMatch,
			"indextype": MergeMustMatch,
		})
		if err != nil {
			return nil, err
		}
		ns, _ := merged.Get("ns")
		name, _ := merged.Get("indexname")
		set, _ := merged.Get("set")
		bin, _ := merged.Get("bin")
		idxType, _ := merged.Get("type")
		collType, _ := merged.Get("indextype")
		ctx, _ := merged.Get("context")

		// Write-only wins: the index is not fully usable while any node builds.
		state := IndexReadWrite
		for _, s := range group {
			if v, ok := s.Get("state"); ok && v == string(IndexWriteOnly) {
				state = IndexWriteOnly
				break
			}
		}
		out = append(out, Sindex{
			Namespace: ns, IndexName: name, Set: set, Bin: bin,
			IndexType: idxType, CollectionType: collType, Context: ctx,
			State: state,
		})
	}
	return out, nil
}

// splitNonEmpty splits and drops empty fields.
func splitNonEmpty(s, sep string) []string {
	var out []string
	for _, f := range strings.Split(s, sep) {
		if strings.TrimSpace(f) != "" {
			out = append(out, f)
		}
	}
	return out
}
