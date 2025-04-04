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
	policy_cache "github.com/aerospike/aerospike-client-go/v8/internal/cache"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Default Policies", func() {
	var client *Client

	gg.Context("when DynConfig is nil", func() {
		gg.BeforeEach(func() {
			client = &Client{
				dynConfig:                nil,
				DefaultPolicy:            NewPolicy(),
				DefaultBatchPolicy:       NewBatchPolicy(),
				DefaultBatchReadPolicy:   NewBatchReadPolicy(),
				DefaultBatchWritePolicy:  NewBatchWritePolicy(),
				DefaultBatchDeletePolicy: NewBatchDeletePolicy(),
				DefaultBatchUDFPolicy:    NewBatchUDFPolicy(),
				DefaultWritePolicy:       NewWritePolicy(0, 0),
				DefaultScanPolicy:        NewScanPolicy(),
				DefaultQueryPolicy:       NewQueryPolicy(),
				DefaultTxnVerifyPolicy:   NewTxnVerifyPolicy(),
				DefaultTxnRollPolicy:     NewTxnRollPolicy(),
			}
		})

		gg.It("GetDefaultPolicy should load a default client policy", func() {
			policy := client.GetDefaultPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchPolicy should load a default BatchPolicy", func() {
			policy := client.GetDefaultBatchPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchReadPolicy should load a default BatchReadPolicy", func() {
			policy := client.GetDefaultBatchReadPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchWritePolicy should load a default BatchWritePolicy", func() {
			policy := client.GetDefaultBatchWritePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchDeletePolicy should load a default BatchDeletePolicy", func() {
			policy := client.GetDefaultBatchDeletePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchUDFPolicy should load a default BatchUDFPolicy", func() {
			policy := client.GetDefaultBatchUDFPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultWritePolicy should load a default WritePolicy", func() {
			policy := client.GetDefaultWritePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultScanPolicy should load a default ScanPolicy", func() {
			policy := client.GetDefaultScanPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultQueryPolicy should load a default QueryPolicy", func() {
			policy := client.GetDefaultQueryPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultTxnVerifyPolicy should load a default TxnVerifyPolicy", func() {
			policy := client.GetDefaultTxnVerifyPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})

		gg.It("GetDefaultTxnRollPolicy should load a default TxnRollPolicy", func() {
			policy := client.GetDefaultTxnRollPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.dynConfig).To(gm.BeNil())
		})
	})

	gg.Context("when DynConfig is not nil (policy cache is populated)", func() {
		var dynCfg *DynConfig
		gg.BeforeEach(func() {
			dummyClientPolicy := NewClientPolicy()
			dummyBatchPolicy := NewBatchPolicy()
			dummyBatchReadPolicy := NewBatchReadPolicy()
			dummyBatchWritePolicy := NewBatchWritePolicy()
			dummyBatchDeletePolicy := NewBatchDeletePolicy()
			dummyBatchUDFPolicy := NewBatchUDFPolicy()
			dummyWritePolicy := NewWritePolicy(0, 0)
			dummyScanPolicy := NewScanPolicy()
			dummyQueryPolicy := NewQueryPolicy()
			dummyTxnVerifyPolicy := NewTxnVerifyPolicy()
			dummyTxnRollPolicy := NewTxnRollPolicy()
			dummyBasePolicy := NewPolicy()

			pcache := policy_cache.NewPolicyCache()
			pcache.Set(policy_cache.CLIENT_POLICY, dummyClientPolicy)
			pcache.Set(policy_cache.BATCH_POLICY, dummyBatchPolicy)
			pcache.Set(policy_cache.BATCH_READ_POLICY, dummyBatchReadPolicy)
			pcache.Set(policy_cache.BATCH_WRITE_POLICY, dummyBatchWritePolicy)
			pcache.Set(policy_cache.BATCH_DELETE_POLICY, dummyBatchDeletePolicy)
			pcache.Set(policy_cache.BATCH_UDF_POLICY, dummyBatchUDFPolicy)
			pcache.Set(policy_cache.WRITE_POLICY, dummyWritePolicy)
			pcache.Set(policy_cache.SCAN_POLICY, dummyScanPolicy)
			pcache.Set(policy_cache.QUERY_POLICY, dummyQueryPolicy)
			pcache.Set(policy_cache.TXN_VERIFY_POLICY, dummyTxnVerifyPolicy)
			pcache.Set(policy_cache.TXN_ROLL_POLICY, dummyTxnRollPolicy)
			pcache.Set(policy_cache.READ_POLICY, dummyBasePolicy)

			dynCfg = &DynConfig{
				mappedPolicies: pcache,
			}
			client = &Client{
				dynConfig: dynCfg,
			}
		})

		gg.It("GetDefaultPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultBatchPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultBatchPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchReadPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultBatchReadPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultBatchReadPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchWritePolicy should fetch policy from cache", func() {
			policy := client.GetDefaultBatchWritePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultBatchWritePolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchDeletePolicy should fetch policy from cache", func() {
			policy := client.GetDefaultBatchDeletePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultBatchDeletePolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultBatchUDFPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultBatchUDFPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultBatchUDFPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultWritePolicy should fetch policy from cache", func() {
			policy := client.GetDefaultWritePolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultWritePolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultScanPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultScanPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultScanPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultQueryPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultQueryPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultQueryPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultTxnVerifyPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultTxnVerifyPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultTxnVerifyPolicy).To(gm.BeNil())
		})

		gg.It("GetDefaultTxnRollPolicy should fetch policy from cache", func() {
			policy := client.GetDefaultTxnRollPolicy()
			gm.Expect(policy).ToNot(gm.BeNil())
			gm.Expect(client.DefaultTxnRollPolicy).To(gm.BeNil())
		})
	})
})
