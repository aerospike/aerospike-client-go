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

package policy_cache

import "maps"

type PolicyT int

const (
	CLIENT_POLICY PolicyT = iota
	READ_POLICY
	WRITE_POLICY
	QUERY_POLICY
	SCAN_POLICY
	BATCH_POLICY
	BATCH_PARENT_WRITE_POLICY
	BATCH_READ_POLICY
	BATCH_WRITE_POLICY
	BATCH_UDF_POLICY
	BATCH_DELETE_POLICY
	TXN_ROLL_POLICY
	TXN_VERIFY_POLICY
	METRICS_POLICY
)

var policyType = map[PolicyT]string{
	CLIENT_POLICY:             "CLIENT_POLICY",
	READ_POLICY:               "READ_POLICY",
	WRITE_POLICY:              "WRITE_POLICY",
	QUERY_POLICY:              "QUERY_POLICY",
	SCAN_POLICY:               "SCAN_POLICY",
	BATCH_POLICY:              "BATCH_POLICY",
	BATCH_PARENT_WRITE_POLICY: "BATCH_PARENT_WRITE_POLICY",
	BATCH_READ_POLICY:         "BATCH_READ_POLICY",
	BATCH_WRITE_POLICY:        "BATCH_WRITE_POLICY",
	BATCH_UDF_POLICY:          "BATCH_UDF_POLICY",
	BATCH_DELETE_POLICY:       "BATCH_DELETE_POLICY",
	TXN_ROLL_POLICY:           "TXN_ROLL_POLICY",
	TXN_VERIFY_POLICY:         "TXN_VERIFY_POLICY",
	METRICS_POLICY:            "METRICS_POLICY",
}

type PolicyCache struct {
	Static  map[PolicyT]any
	Dynamic map[PolicyT]any
}

func NewPolicyCache() *PolicyCache {
	return &PolicyCache{
		Static:  make(map[PolicyT]any, 0),
		Dynamic: make(map[PolicyT]any, 0),
	}
}

func NewPolicyCacheWithData(static, dynamic map[PolicyT]any) *PolicyCache {
	return &PolicyCache{
		Static:  static,
		Dynamic: dynamic,
	}
}

func (pc *PolicyCache) Get(policyType PolicyT) any {
	if pc.Static != nil {
		if policy, ok := pc.Static[policyType]; ok {
			return policy
		}
	}
	if pc.Dynamic != nil {
		if policy, ok := pc.Dynamic[policyType]; ok {
			return policy
		}
	}
	return nil
}

func (pc *PolicyCache) Set(policyType PolicyT, policy any) {
	if _, ok := pc.Static[policyType]; ok {
		pc.Static[policyType] = policy
	} else {
		pc.Dynamic[policyType] = policy
	}
}

func (pc *PolicyCache) Delete(policyType PolicyT) {
	if _, ok := pc.Static[policyType]; ok {
		delete(pc.Static, policyType)
	} else {
		delete(pc.Dynamic, policyType)
	}
}

func (pc *PolicyCache) PruneDynamic() {
	pc.Dynamic = make(map[PolicyT]any, 0)
}

func (pc *PolicyCache) Clear() {
	pc.Static = make(map[PolicyT]any, 0)
	pc.Dynamic = make(map[PolicyT]any, 0)
}

func (pc *PolicyCache) Length() int {
	return len(pc.Static) + len(pc.Dynamic)
}

func (pc *PolicyCache) Clone() *PolicyCache {
	newCache := &PolicyCache{
		Static:  make(map[PolicyT]any, len(pc.Static)),
		Dynamic: make(map[PolicyT]any, len(pc.Dynamic)),
	}
	maps.Copy(newCache.Static, pc.Static)
	maps.Copy(newCache.Dynamic, pc.Dynamic)
	return newCache
}

func (pc *PolicyCache) Replace(newCache *PolicyCache) {
	pc.Static = make(map[PolicyT]any, len(newCache.Static))
	pc.Dynamic = make(map[PolicyT]any, len(newCache.Dynamic))
	maps.Copy(pc.Static, newCache.Static)
	maps.Copy(pc.Dynamic, newCache.Dynamic)
}
