// Copyright 2014-2021 Aerospike, Inc.
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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
	asub "github.com/aerospike/aerospike-client-go/utils/buffer"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	var actualClusterName string

	gg.Describe("Client Management", func() {

		dbHost := as.NewHost(*host, *port)
		dbHost.TLSName = *nodeTLSName

		gg.It("must open and close the client without a problem", func() {
			client, err := as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(client.IsConnected()).To(gm.BeTrue())

			time.Sleep(5 * time.Second)

			// set actual cluster name
			node, err := client.Cluster().GetRandomNode()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(node).NotTo(gm.BeNil())
			res, err := node.RequestInfo(as.NewInfoPolicy(), "cluster-name")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			actualClusterName = res["cluster-name"]

			stats, err := client.Stats()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(len(stats)).To(gm.BeNumerically(">", 0))
			for _, nodeStatsIfc := range stats {
				if nodeStats, ok := nodeStatsIfc.(map[string]interface{}); ok {
					gm.Expect(nodeStats["connections-attempts"].(float64)).To(gm.BeNumerically(">=", 1))
					gm.Expect(nodeStats["node-added-count"].(float64)).To(gm.BeNumerically(">=", 1))
					gm.Expect(nodeStats["partition-map-updates"].(float64)).To(gm.BeNumerically(">=", 1))
					gm.Expect(nodeStats["tends-successful"].(float64)).To(gm.BeNumerically(">", 1))
					gm.Expect(nodeStats["tends-total"].(float64)).To(gm.BeNumerically(">", 1))
				}
			}

			client.Close()
			gm.Expect(client.IsConnected()).To(gm.BeFalse())
		})

		gg.It("must return an error if supplied cluster-name is wrong", func() {
			cpolicy := *clientPolicy
			cpolicy.ClusterName = "haha"
			cpolicy.Timeout = 10 * time.Second
			nclient, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.CLUSTER_NAME_MISMATCH_ERROR))
			gm.Expect(nclient).To(gm.BeNil())
		})

		gg.It("must return a client even if cluster-name is wrong, but failIfConnected is false", func() {
			cpolicy := *clientPolicy
			cpolicy.ClusterName = "haha"
			cpolicy.Timeout = 10 * time.Second
			cpolicy.FailIfNotConnected = false
			nclient, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
			aerr, ok := err.(ast.AerospikeError)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(aerr.ResultCode()).To(gm.Equal(ast.CLUSTER_NAME_MISMATCH_ERROR))
			gm.Expect(nclient).NotTo(gm.BeNil())
			gm.Expect(nclient.IsConnected()).To(gm.BeFalse())
		})

		gg.It("must connect to the cluster when cluster-name is correct", func() {
			nodeCount := len(client.GetNodes())

			cpolicy := *clientPolicy
			cpolicy.ClusterName = actualClusterName
			cpolicy.Timeout = 10 * time.Second
			nclient, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(len(nclient.GetNodes())).To(gm.Equal(nodeCount))
		})

		gg.It("must connect to the cluster using external authentication protocol", func() {
			if *authMode != "external" {
				gg.Skip("gg.Skipping External Authentication connection...")
			}

			nodeCount := len(client.GetNodes())

			cpolicy := *clientPolicy
			cpolicy.Timeout = 10 * time.Second
			cpolicy.AuthMode = as.AuthModeExternal
			cpolicy.User = "badwan"
			cpolicy.Password = "blastoff"
			nclient, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(len(nclient.GetNodes())).To(gm.Equal(nodeCount))
		})

		gg.Context("Rackaware", func() {

			gg.It("must connect to the cluster in rackaware mode, and set the RackId = 0, and still get master node for all keys", func() {
				cpolicy := *clientPolicy
				cpolicy.User = *user
				cpolicy.Password = *password
				c, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
				gm.Expect(err).NotTo(gm.HaveOccurred())

				info := info(c, "racks:")
				if strings.HasPrefix(strings.ToUpper(info), "ERROR") {
					gg.Skip("gg.Skipping RackAware test since it is not supported on this cluster...")
				}

				cpolicy = *clientPolicy
				cpolicy.Timeout = 10 * time.Second
				cpolicy.RackAware = true

				for rid := 1; rid <= 20; rid++ {
					nclient, err := as.NewClientWithPolicyAndHost(&cpolicy, dbHost)
					gm.Expect(err).NotTo(gm.HaveOccurred())

					wpolicy := as.NewWritePolicy(0, 0)
					wpolicy.ReplicaPolicy = as.PREFER_RACK
					for i := 0; i < 12; i++ {
						key, _ := as.NewKey(*namespace, "test", 1)
						partition, err := as.PartitionForWrite(nclient.Cluster(), wpolicy.GetBasePolicy(), key)
						gm.Expect(err).NotTo(gm.HaveOccurred())
						masterNode, err := partition.GetMasterNode(nclient.Cluster())
						gm.Expect(err).NotTo(gm.HaveOccurred())

						// node, err := nclient.Cluster().GetReadNode(partition, replicaPolicy, &seq)
						node, err := partition.GetNodeRead(nclient.Cluster())
						gm.Expect(err).NotTo(gm.HaveOccurred())
						gm.Expect(node).NotTo(gm.BeNil())
						gm.Expect(node).To(gm.Equal(masterNode))
					}
					nclient.Close()
				}
			})

			// gg.It("must connect to the cluster in rackaware mode", func() {
			// 	cpolicy := *clientPolicy
			// 	cpolicy.Timeout = 10 * time.Second
			// 	cpolicy.RackAware = true

			// 	rpolicy := as.NewPolicy()
			// 	rpolicy.ReplicaPolicy = as.PREFER_RACK

			// 	for rid := 1; rid <= 20; rid++ {
			// 		cpolicy.RackId = (rid % 2) + 1

			// 		nclient, err := as.NewClientWithPolicyAndHostNewClientWithPolicy(&cpolicy, dbHost)
			// 		gm.Expect(err).NotTo(gm.HaveOccurred())

			// 		for i := 0; i < 12; i++ {
			// 			println(i)
			// 			key, _ := as.NewKey(*namespace, "test", 1)
			// 			partition, err := as.PartitionForRead(nclient.Cluster(), rpolicy.GetBasePolicy(), key)
			// 			gm.Expect(err).NotTo(gm.HaveOccurred())

			// 			node, err := partition.GetNodeRead(nclient.Cluster())
			// 			gm.Expect(err).NotTo(gm.HaveOccurred())
			// 			gm.Expect(node.Rack("test")).To(gm.Equal(cpolicy.RackId))
			// 		}
			// 		nclient.Close()
			// 	}
			// })
		})
	})

	gg.Describe("Data operations on native types", func() {
		// connection data
		var err error
		var ns = *namespace
		var set = randString(50)
		var key *as.Key
		var wpolicy = as.NewWritePolicy(0, 0)
		var rpolicy = as.NewPolicy()
		var bpolicy = as.NewBatchPolicy()
		var rec *as.Record

		if *useReplicas {
			rpolicy.ReplicaPolicy = as.MASTER_PROLES
		}

		gg.BeforeEach(func() {
			key, err = as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.Context("Put operations", func() {

			gg.Context("Expiration values", func() {

				gg.It("must return 30d if set to TTLServerDefault", func() {
					wpolicy := as.NewWritePolicy(0, as.TTLServerDefault)
					bin := as.NewBin("Aerospike", "value")
					rec, err = client.Operate(wpolicy, key, as.PutOp(bin), as.GetOp())
					gm.Expect(err).ToNot(gm.HaveOccurred())

					defaultTTL, err := strconv.Atoi(nsInfo(ns, "default-ttl"))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					switch defaultTTL {
					case 0:
						gm.Expect(rec.Expiration).To(gm.Equal(uint32(math.MaxUint32)))
					default:
						gm.Expect(rec.Expiration).To(gm.Equal(uint32(defaultTTL)))
					}

				})

				gg.It("must return TTLDontExpire if set to TTLDontExpire", func() {
					wpolicy := as.NewWritePolicy(0, as.TTLDontExpire)
					bin := as.NewBin("Aerospike", "value")
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Expiration).To(gm.Equal(uint32(as.TTLDontExpire)))
				})

				gg.It("must not change the TTL if set to TTLDontUpdate", func() {
					wpolicy := as.NewWritePolicy(0, as.TTLServerDefault)
					bin := as.NewBin("Aerospike", "value")
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					time.Sleep(3 * time.Second)

					wpolicy = as.NewWritePolicy(0, as.TTLDontUpdate)
					bin = as.NewBin("Aerospike", "value")
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					defaultTTL, err := strconv.Atoi(nsInfo(ns, "default-ttl"))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					switch defaultTTL {
					case 0:
						gm.Expect(rec.Expiration).To(gm.Equal(uint32(math.MaxUint32)))
					default:
						gm.Expect(rec.Expiration).To(gm.BeNumerically("<=", uint32(defaultTTL-3))) // default expiration on server is set to 30d
					}
				})
			})

			gg.Context("Bins with `nil` values should be deleted", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", "value")
					bin1 := as.NewBin("Aerospike1", "value2") // to avoid deletion of key
					err = client.PutBins(wpolicy, key, bin, bin1)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))

					bin2 := as.NewBin("Aerospike", nil)
					err = client.PutBins(wpolicy, key, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin.Name]
					gm.Expect(exists).To(gm.Equal(false))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", "nil")
					bin2 := as.NewBin("Aerospike2", "value")
					bin3 := as.NewBin("Aerospike3", "value")
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					bin2nil := as.NewBin("Aerospike2", nil)
					bin3nil := as.NewBin("Aerospike3", nil)
					err = client.PutBins(wpolicy, key, bin2nil, bin3nil)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin2.Name]
					gm.Expect(exists).To(gm.Equal(false))
					_, exists = rec.Bins[bin3.Name]
					gm.Expect(exists).To(gm.Equal(false))
				})

				gg.It("must save a key with MULTIPLE bins using a BinMap", func() {
					bin1 := as.NewBin("Aerospike1", "nil")
					bin2 := as.NewBin("Aerospike2", "value")
					bin3 := as.NewBin("Aerospike3", "value")
					err = client.Put(wpolicy, key, as.BinMap{bin1.Name: bin1.Value, bin2.Name: bin2.Value, bin3.Name: bin3.Value})
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					bin2nil := as.NewBin("Aerospike2", nil)
					bin3nil := as.NewBin("Aerospike3", nil)
					err = client.Put(wpolicy, key, as.BinMap{bin2nil.Name: bin2nil.Value, bin3nil.Name: bin3nil.Value})
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin2.Name]
					gm.Expect(exists).To(gm.Equal(false))
					_, exists = rec.Bins[bin3.Name]
					gm.Expect(exists).To(gm.Equal(false))
				})
			})

			gg.Context("Bins with `string` values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", "Awesome")
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", "Awesome1")
					bin2 := as.NewBin("Aerospike2", "")
					err = client.PutBins(wpolicy, key, bin1, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
				})
			})

			gg.Context("Bins with `int8` and `uint8` values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", int8(rand.Intn(math.MaxInt8)))
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", int8(math.MaxInt8))
					bin2 := as.NewBin("Aerospike2", int8(math.MinInt8))
					bin3 := as.NewBin("Aerospike3", uint8(math.MaxUint8))
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
					gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
				})
			})

			gg.Context("Bins with `int16` and `uint16` values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", int16(rand.Intn(math.MaxInt16)))
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", int16(math.MaxInt16))
					bin2 := as.NewBin("Aerospike2", int16(math.MinInt16))
					bin3 := as.NewBin("Aerospike3", uint16(math.MaxUint16))
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
					gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
				})
			})

			gg.Context("Bins with `int` and `uint` values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", rand.Int())
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins; uint of > MaxInt32 will always result in LongValue", func() {
					bin1 := as.NewBin("Aerospike1", math.MaxInt32)
					bin2, bin3 := func() (*as.Bin, *as.Bin) {
						if asub.Arch32Bits {
							return as.NewBin("Aerospike2", int(math.MinInt32)),
								as.NewBin("Aerospike3", uint(math.MaxInt32))
						}
						return as.NewBin("Aerospike2", int(math.MinInt64)),
							as.NewBin("Aerospike3", uint(math.MaxInt64))

					}()

					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					if asub.Arch64Bits {
						gm.Expect(rec.Bins[bin2.Name].(int)).To(gm.Equal(bin2.Value.GetObject()))
						gm.Expect(int64(rec.Bins[bin3.Name].(int))).To(gm.Equal(bin3.Value.GetObject()))
					} else {
						gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
						gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
					}
				})
			})

			gg.Context("Bins with `int64` only values (uint64 is supported via type cast to int64) ", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", rand.Int63())
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					if asub.Arch64Bits {
						gm.Expect(int64(rec.Bins[bin.Name].(int))).To(gm.Equal(bin.Value.GetObject()))
					} else {
						gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
					}
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", math.MaxInt64)
					bin2 := as.NewBin("Aerospike2", math.MinInt64)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
				})
			})

			gg.Context("Bins with `float32` only values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", rand.Float32())
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(float64(rec.Bins[bin.Name].(float64))).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", math.MaxFloat32)
					bin2 := as.NewBin("Aerospike2", -math.MaxFloat32)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
				})
			})

			gg.Context("Bins with `float64` only values", func() {
				gg.It("must save a key with SINGLE bin", func() {
					bin := as.NewBin("Aerospike", rand.Float64())
					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(float64(rec.Bins[bin.Name].(float64))).To(gm.Equal(bin.Value.GetObject()))
				})

				gg.It("must save a key with MULTIPLE bins", func() {
					bin1 := as.NewBin("Aerospike1", math.MaxFloat64)
					bin2 := as.NewBin("Aerospike2", -math.MaxFloat64)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
				})
			})

			// TODO: Uncomment after server v5.6 release.
			// gg.Context("Bins with `bool` only values", func() {
			// 	gg.It("must save a key with SINGLE bin", func() {
			// 		bin := as.NewBin("Aerospike", as.BoolValue(true))
			// 		err = client.PutBins(wpolicy, key, bin)
			// 		gm.Expect(err).ToNot(gm.HaveOccurred())

			// 		rec, err = client.Get(rpolicy, key)
			// 		gm.Expect(err).ToNot(gm.HaveOccurred())
			// 		gm.Expect(bool(rec.Bins[bin.Name].(bool))).To(gm.Equal(bin.Value.GetObject()))
			// 	})

			// 	gg.It("must save a key with MULTIPLE bins", func() {
			// 		bin1 := as.NewBin("Aerospike1", as.BoolValue(true))
			// 		bin2 := as.NewBin("Aerospike2", as.BoolValue(false))
			// 		err = client.PutBins(wpolicy, key, bin1, bin2)
			// 		gm.Expect(err).ToNot(gm.HaveOccurred())

			// 		rec, err = client.Get(rpolicy, key)
			// 		gm.Expect(err).ToNot(gm.HaveOccurred())

			// 		gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
			// 		gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
			// 	})
			// })

			gg.Context("Bins with complex types", func() {

				gg.Context("Bins with BLOB type", func() {
					gg.It("must save and retrieve Bins with AerospikeBlobs type", func() {
						person := &testBLOB{name: "SomeDude"}
						bin := as.NewBin("Aerospike1", person)
						err = client.PutBins(wpolicy, key, bin)
						gm.Expect(err).ToNot(gm.HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						gm.Expect(err).ToNot(gm.HaveOccurred())
					})
				})

				gg.Context("Bins with LIST type", func() {

					gg.It("must save a key with Array Types", func() {
						// All int types and sizes should be encoded into an int64,
						// unless if they are of type uint64, which always encodes to uint64
						// regardless of the values inside
						intList := []interface{}{math.MinInt64, math.MinInt64 + 1}
						for i := uint(0); i < 64; i++ {
							intList = append(intList, -(1 << i))
							intList = append(intList, -(1<<i)-1)
							intList = append(intList, -(1<<i)+1)
							intList = append(intList, 1<<i)
							intList = append(intList, (1<<i)-1)
							intList = append(intList, (1<<i)+1)
						}
						intList = append(intList, -1)
						intList = append(intList, 0)
						intList = append(intList, uint64(1))
						intList = append(intList, math.MaxInt64-1)
						intList = append(intList, math.MaxInt64)
						intList = append(intList, uint64(math.MaxInt64+1))
						intList = append(intList, uint64(math.MaxUint64-1))
						intList = append(intList, uint64(math.MaxUint64))
						bin0 := as.NewBin("Aerospike0", intList)

						bin1 := as.NewBin("Aerospike1", []interface{}{math.MinInt8, 0, 1, 2, 3, math.MaxInt8})
						bin2 := as.NewBin("Aerospike2", []interface{}{math.MinInt16, 0, 1, 2, 3, math.MaxInt16})
						bin3 := as.NewBin("Aerospike3", []interface{}{math.MinInt32, 0, 1, 2, 3, math.MaxInt32})
						bin4 := as.NewBin("Aerospike4", []interface{}{math.MinInt64, 0, 1, 2, 3, math.MaxInt64})
						bin5 := as.NewBin("Aerospike5", []interface{}{0, 1, 2, 3, math.MaxUint8})
						bin6 := as.NewBin("Aerospike6", []interface{}{0, 1, 2, 3, math.MaxUint16})
						bin7 := as.NewBin("Aerospike7", []interface{}{0, 1, 2, 3, math.MaxUint32})
						bin8 := as.NewBin("Aerospike8", []interface{}{"", "\n", "string"})
						bin9 := as.NewBin("Aerospike9", []interface{}{"", 1, nil, true, false, uint64(math.MaxUint64), math.MaxFloat32, math.MaxFloat64, as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), []interface{}{1, 2, 3}})

						// complex type, consisting different arrays
						bin10 := as.NewBin("Aerospike10", []interface{}{
							nil,
							bin0.Value.GetObject(),
							bin1.Value.GetObject(),
							bin2.Value.GetObject(),
							bin3.Value.GetObject(),
							bin4.Value.GetObject(),
							bin5.Value.GetObject(),
							bin6.Value.GetObject(),
							bin7.Value.GetObject(),
							bin8.Value.GetObject(),
							bin9.Value.GetObject(),
							map[interface{}]interface{}{
								1: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
								// [3]int{0, 1, 2}:          []interface{}{"string", 12, nil},
								// [3]string{"0", "1", "2"}: []interface{}{"string", 12, nil},
								15:                        nil,
								int8(math.MaxInt8):        int8(math.MaxInt8),
								int64(math.MinInt64):      int64(math.MinInt64),
								int64(math.MaxInt64):      int64(math.MaxInt64),
								uint64(math.MaxUint64):    uint64(math.MaxUint64),
								float32(-math.MaxFloat32): float32(-math.MaxFloat32),
								float64(-math.MaxFloat64): float64(-math.MaxFloat64),
								float32(math.MaxFloat32):  float32(math.MaxFloat32),
								float64(math.MaxFloat64):  float64(math.MaxFloat64),
								"true":                    true,
								"false":                   false,
								"string":                  map[interface{}]interface{}{nil: "string", "string": 19},                // map to complex array
								nil:                       []interface{}{18, 41},                                                   // array to complex map
								"GeoJSON":                 as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), // bit-sign test
								"intList":                 intList,
							},
						})

						err = client.PutBins(wpolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6, bin7, bin8, bin9, bin10)
						gm.Expect(err).ToNot(gm.HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						gm.Expect(err).ToNot(gm.HaveOccurred())

						arraysEqual(rec.Bins[bin0.Name], bin0.Value.GetObject())
						arraysEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
						arraysEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
						arraysEqual(rec.Bins[bin3.Name], bin3.Value.GetObject())
						arraysEqual(rec.Bins[bin4.Name], bin4.Value.GetObject())
						arraysEqual(rec.Bins[bin5.Name], bin5.Value.GetObject())
						arraysEqual(rec.Bins[bin6.Name], bin6.Value.GetObject())
						arraysEqual(rec.Bins[bin7.Name], bin7.Value.GetObject())
						arraysEqual(rec.Bins[bin8.Name], bin8.Value.GetObject())
						arraysEqual(rec.Bins[bin9.Name], bin9.Value.GetObject())
						arraysEqual(rec.Bins[bin10.Name], bin10.Value.GetObject())
					})

				}) // context list

				gg.Context("Bins with MAP type", func() {

					gg.It("must save a key with Array Types", func() {
						// complex type, consisting different maps
						bin1 := as.NewBin("Aerospike1", map[interface{}]interface{}{
							0:                    "",
							int32(math.MaxInt32): randString(100),
							int32(math.MinInt32): randString(100),
						})

						bin2 := as.NewBin("Aerospike2", map[interface{}]interface{}{
							15:                        nil,
							"true":                    true,
							"false":                   false,
							int8(math.MaxInt8):        int8(math.MaxInt8),
							int64(math.MinInt64):      int64(math.MinInt64),
							int64(math.MaxInt64):      int64(math.MaxInt64),
							uint64(math.MaxUint64):    uint64(math.MaxUint64),
							float32(-math.MaxFloat32): float32(-math.MaxFloat32),
							float64(-math.MaxFloat64): float64(-math.MaxFloat64),
							float32(math.MaxFloat32):  float32(math.MaxFloat32),
							float64(math.MaxFloat64):  float64(math.MaxFloat64),
							"string":                  map[interface{}]interface{}{nil: "string", "string": 19},                // map to complex array
							nil:                       []interface{}{18, 41},                                                   // array to complex map
							"longString":              strings.Repeat("s", 32911),                                              // bit-sign test
							"GeoJSON":                 as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), // bit-sign test
						})

						err = client.PutBins(wpolicy, key, bin1, bin2)
						gm.Expect(err).ToNot(gm.HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						gm.Expect(err).ToNot(gm.HaveOccurred())

						mapsEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
						mapsEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
					})

				}) // context map

			}) // context complex types

		}) // put context

		gg.Context("Append operations", func() {
			bin := as.NewBin("Aerospike", randString(rand.Intn(100)))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must append to a SINGLE bin", func() {
				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.AppendBins(wpolicy, key, appbin)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
			})

			gg.It("must append to a SINGLE bin using a BinMap", func() {
				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.Append(wpolicy, key, as.BinMap{bin.Name: appbin.Value})
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
			})

		}) // append context

		gg.Context("Prepend operations", func() {
			bin := as.NewBin("Aerospike", randString(rand.Intn(100)))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must Prepend to a SINGLE bin", func() {
				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.PrependBins(wpolicy, key, appbin)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
			})

			gg.It("must Prepend to a SINGLE bin using a BinMap", func() {
				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.Prepend(wpolicy, key, as.BinMap{bin.Name: appbin.Value})
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
			})

		}) // prepend context

		gg.Context("Add operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must Add to a SINGLE bin", func() {
				addBin := as.NewBin(bin.Name, rand.Intn(math.MaxInt16))
				err = client.AddBins(wpolicy, key, addBin)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
			})

			gg.It("must Add to a SINGLE bin using a BinMap", func() {
				addBin := as.NewBin(bin.Name, rand.Intn(math.MaxInt16))
				err = client.Add(wpolicy, key, as.BinMap{addBin.Name: addBin.Value})
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
			})

		}) // add context

		gg.Context("Delete operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must Delete a non-existing key", func() {
				var nxkey *as.Key
				nxkey, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				var existed bool
				existed, err = client.Delete(wpolicy, nxkey)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(existed).To(gm.Equal(false))
			})

			gg.It("must Delete an existing key", func() {
				var existed bool
				existed, err = client.Delete(wpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(existed).To(gm.Equal(true))

				existed, err = client.Exists(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(existed).To(gm.Equal(false))
			})

		}) // Delete context

		gg.Context("Touch operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must Touch a non-existing key", func() {
				var nxkey *as.Key
				nxkey, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.Touch(wpolicy, nxkey)
				gm.Expect(err).To(gm.Equal(ast.ErrKeyNotFound))
			})

			gg.It("must Touch an existing key", func() {
				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				generation := rec.Generation

				wpolicy := as.NewWritePolicy(0, 0)
				wpolicy.SendKey = true
				err = client.Touch(wpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Generation).To(gm.BeNumerically(">", generation))

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// make sure the
				for r := range recordset.Results() {
					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
					}
				}
			})

		}) // Touch context

		gg.Context("Exists operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must check Existence of a non-existing key", func() {
				var nxkey *as.Key
				nxkey, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				var exists bool
				exists, err = client.Exists(rpolicy, nxkey)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(exists).To(gm.Equal(false))
			})

			gg.It("must checks Existence of an existing key", func() {
				var exists bool
				exists, err = client.Exists(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(exists).To(gm.Equal(true))
			})

		}) // Exists context

		gg.Context("Batch Exists operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))
			const keyCount = 2048

			gg.BeforeEach(func() {
			})

			for _, useInline := range []bool{true, false} {
				gg.It(fmt.Sprintf("must return the result with same ordering. AllowInline: %v", useInline), func() {
					var exists []bool
					keys := []*as.Key{}

					for i := 0; i < keyCount; i++ {
						key, err := as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())
						keys = append(keys, key)

						// if key shouldExist == true, put it in the DB
						if i%2 == 0 {
							err = client.PutBins(wpolicy, key, bin)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							// make sure they exists in the DB
							exists, err := client.Exists(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(exists).To(gm.Equal(true))
						}
					}

					bpolicy.AllowInline = useInline
					exists, err = client.BatchExists(bpolicy, keys)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(len(exists)).To(gm.Equal(len(keys)))
					for idx, keyExists := range exists {
						gm.Expect(keyExists).To(gm.Equal(idx%2 == 0))
					}
				})
			}

		}) // Batch Exists context

		gg.Context("Batch Get operations", func() {
			bin := as.NewBin("Aerospike", rand.Int())
			const keyCount = 2048

			gg.BeforeEach(func() {
			})

			for _, useInline := range []bool{true, false} {
				gg.It(fmt.Sprintf("must return the records with same ordering as keys. AllowInline: %v", useInline), func() {
					binRedundant := as.NewBin("Redundant", "Redundant")

					var records []*as.Record
					type existence struct {
						key         *as.Key
						shouldExist bool // set randomly and checked against later
					}

					exList := make([]existence, 0, keyCount)
					keys := make([]*as.Key, 0, keyCount)

					for i := 0; i < keyCount; i++ {
						key, err := as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())
						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
						exList = append(exList, e)
						keys = append(keys, key)

						// if key shouldExist == true, put it in the DB
						if e.shouldExist {
							err = client.PutBins(wpolicy, key, bin, binRedundant)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							// make sure they exists in the DB
							rec, err := client.Get(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.Equal(binRedundant.Value.GetObject()))
						} else {
							// make sure they exists in the DB
							exists, err := client.Exists(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(exists).To(gm.Equal(false))
						}
					}

					brecords := make([]*as.BatchRead, len(keys))
					for i := range keys {
						brecords[i] = &as.BatchRead{
							Key:         keys[i],
							ReadAllBins: true,
						}
					}
					bpolicy.AllowInline = useInline
					err = client.BatchGetComplex(bpolicy, brecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					for idx, rec := range brecords {
						if exList[idx].shouldExist {
							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec.Record).To(gm.BeNil())
						}
					}

					records, err = client.BatchGet(bpolicy, keys)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(len(records)).To(gm.Equal(len(keys)))
					for idx, rec := range records {
						if exList[idx].shouldExist {
							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec).To(gm.BeNil())
						}
					}

					records, err = client.BatchGet(bpolicy, keys, bin.Name)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(len(records)).To(gm.Equal(len(keys)))
					for idx, rec := range records {
						if exList[idx].shouldExist {
							// only bin1 has been requested
							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.BeNil())
							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec).To(gm.BeNil())
						}
					}
				})

				gg.It(fmt.Sprintf("must return the records with same ordering as keys via Batch Complex Protocol. AllowInline: %v", useInline), func() {
					binRedundant := as.NewBin("Redundant", "Redundant")

					type existence struct {
						key         *as.Key
						shouldExist bool // set randomly and checked against later
					}

					exList := make([]existence, 0, keyCount)
					keys := make([]*as.Key, 0, keyCount)

					for i := 0; i < keyCount; i++ {
						key, err := as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())
						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
						exList = append(exList, e)
						keys = append(keys, key)

						// if key shouldExist == true, put it in the DB
						if e.shouldExist {
							err = client.PutBins(wpolicy, key, bin, binRedundant)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							// make sure they exists in the DB
							rec, err := client.Get(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.Equal(binRedundant.Value.GetObject()))
						} else {
							// make sure they exists in the DB
							exists, err := client.Exists(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(exists).To(gm.Equal(false))
						}
					}

					brecords := make([]*as.BatchRead, len(keys))
					for i := range keys {
						brecords[i] = &as.BatchRead{
							Key:         keys[i],
							ReadAllBins: true,
						}
					}
					bpolicy.AllowInline = useInline
					err = client.BatchGetComplex(bpolicy, brecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					for idx, rec := range brecords {
						if exList[idx].shouldExist {
							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec.Record).To(gm.BeNil())
						}
					}

					brecords = make([]*as.BatchRead, len(keys))
					for i := range keys {
						brecords[i] = &as.BatchRead{
							Key:         keys[i],
							ReadAllBins: false,
							BinNames:    []string{"Aerospike", "Redundant"},
						}
					}
					bpolicy.AllowInline = useInline
					err = client.BatchGetComplex(bpolicy, brecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					for idx, rec := range brecords {
						if exList[idx].shouldExist {
							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec.Record).To(gm.BeNil())
						}
					}

					brecords = make([]*as.BatchRead, len(keys))
					for i := range keys {
						brecords[i] = &as.BatchRead{
							Key:         keys[i],
							ReadAllBins: false,
							BinNames:    nil,
						}
					}
					bpolicy.AllowInline = useInline
					err = client.BatchGetComplex(bpolicy, brecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					for idx, rec := range brecords {
						if exList[idx].shouldExist {
							gm.Expect(len(rec.Record.Bins)).To(gm.Equal(0))
							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
						} else {
							gm.Expect(rec.Record).To(gm.BeNil())
						}
					}

				})
			}
		}) // Batch Get context

		gg.Context("GetHeader operations", func() {
			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

			gg.BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must Get the Header of an existing key after touch", func() {
				rec, err = client.Get(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				generation := rec.Generation

				err = client.Touch(wpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.GetHeader(rpolicy, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Generation).To(gm.BeNumerically(">", generation))
				gm.Expect(rec.Bins[bin.Name]).To(gm.BeNil())
			})

		}) // GetHeader context

		gg.Context("Batch Get Header operations", func() {
			bin := as.NewBin("Aerospike", rand.Int())
			const keyCount = 1024

			gg.BeforeEach(func() {
			})

			for _, useInline := range []bool{true, false} {
				gg.It(fmt.Sprintf("must return the record headers with same ordering as keys. AllowInline: %v", useInline), func() {
					var records []*as.Record
					type existence struct {
						key         *as.Key
						shouldExist bool // set randomly and checked against later
					}

					exList := []existence{}
					keys := []*as.Key{}

					for i := 0; i < keyCount; i++ {
						key, err := as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())
						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
						exList = append(exList, e)
						keys = append(keys, key)

						// if key shouldExist == true, put it in the DB
						if e.shouldExist {
							err = client.PutBins(wpolicy, key, bin)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							// update generation
							err = client.Touch(wpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							// make sure they exists in the DB
							exists, err := client.Exists(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(exists).To(gm.Equal(true))
						}
					}

					bpolicy.AllowInline = useInline
					records, err = client.BatchGetHeader(bpolicy, keys)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(len(records)).To(gm.Equal(len(keys)))
					for idx, rec := range records {
						if exList[idx].shouldExist {
							gm.Expect(rec.Bins[bin.Name]).To(gm.BeNil())
							gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))
						} else {
							gm.Expect(rec).To(gm.BeNil())
						}
					}
				})
			}
		}) // Batch Get Header context

		gg.Context("Operate operations", func() {
			bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
			bin2 := as.NewBin("Aerospike2", randString(100))

			gg.BeforeEach(func() {
				// err = client.PutBins(wpolicy, key, bin)
				// gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must return proper error on write operations, but not reads", func() {
				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				wpolicy := as.NewWritePolicy(0, 0)
				rec, err = client.Operate(wpolicy, key, as.GetOp())
				gm.Expect(err).To(gm.Equal(ast.ErrKeyNotFound))

				rec, err = client.Operate(wpolicy, key, as.TouchOp())
				gm.Expect(err).To(gm.HaveOccurred())
			})

			gg.It("must work correctly when no BinOps are passed as argument", func() {
				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{}

				wpolicy := as.NewWritePolicy(0, 0)
				rec, err = client.Operate(wpolicy, key, ops1...)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Error()).To(gm.ContainSubstring("No operations were passed."))
			})

			gg.It("must send key on Put operations", func() {
				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{
					as.PutOp(bin1),
					as.PutOp(bin2),
					as.GetOp(),
				}

				wpolicy := as.NewWritePolicy(0, 0)
				wpolicy.SendKey = true
				rec, err = client.Operate(wpolicy, key, ops1...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// make sure the result is what we put in
				for r := range recordset.Results() {
					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
					}
				}
			})

			gg.It("must send List key on Put operations", func() {
				kval := []int{1, 2, 3}
				key, err := as.NewKey(ns, set, kval)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{
					as.PutOp(bin1),
					as.PutOp(bin2),
					as.GetOp(),
				}

				wpolicy := as.NewWritePolicy(0, 0)
				wpolicy.SendKey = true
				rec, err = client.Operate(wpolicy, key, ops1...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// make sure the result is what we put in
				for r := range recordset.Results() {
					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						gm.Expect(r.Record.Key.Value()).To(gm.Equal(as.NewListValue([]interface{}{1, 2, 3})))
						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
					}
				}
			})

			gg.It("must send key on Touch operations", func() {
				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{
					as.GetOp(),
					as.PutOp(bin2),
				}

				wpolicy := as.NewWritePolicy(0, 0)
				wpolicy.SendKey = false
				rec, err = client.Operate(wpolicy, key, ops1...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// make sure the key is not saved
				for r := range recordset.Results() {
					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						gm.Expect(r.Record.Key.Value()).To(gm.BeNil())
					}
				}

				ops2 := []*as.Operation{
					as.GetOp(),
					as.TouchOp(),
				}
				wpolicy.SendKey = true
				rec, err = client.Operate(wpolicy, key, ops2...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				recordset, err = client.ScanAll(nil, key.Namespace(), key.SetName())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// make sure the
				for r := range recordset.Results() {
					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
					}
				}
			})

			gg.It("must apply all operations, and result should match expectation", func() {
				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{
					as.PutOp(bin1),
					as.PutOp(bin2),
					as.GetOp(),
				}

				rec, err = client.Operate(nil, key, ops1...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int)))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string)))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(1)))

				ops2 := []*as.Operation{
					as.AddOp(bin1),    // double the value of the bin
					as.AppendOp(bin2), // with itself
					as.GetOp(),
				}

				rec, err = client.Operate(nil, key, ops2...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) * 2))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(strings.Repeat(bin2.Value.GetObject().(string), 2)))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))

				ops3 := []*as.Operation{
					as.AddOp(bin1),
					as.PrependOp(bin2),
					as.TouchOp(),
					as.GetOp(),
				}

				rec, err = client.Operate(nil, key, ops3...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) * 3))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(strings.Repeat(bin2.Value.GetObject().(string), 3)))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(3)))

				ops4 := []*as.Operation{
					as.TouchOp(),
					as.GetHeaderOp(),
				}

				rec, err = client.Operate(nil, key, ops4...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Generation).To(gm.Equal(uint32(4)))
				gm.Expect(len(rec.Bins)).To(gm.Equal(0))

				// GetOp should override GetHEaderOp
				ops5 := []*as.Operation{
					as.GetOp(),
					as.GetHeaderOp(),
				}

				rec, err = client.Operate(nil, key, ops5...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Generation).To(gm.Equal(uint32(4)))
				gm.Expect(len(rec.Bins)).To(gm.Equal(2))

				// GetOp should override GetHeaderOp
				ops6 := []*as.Operation{
					as.GetHeaderOp(),
					as.DeleteOp(),
					as.PutOp(bin1),
				}

				rec, err = client.Operate(nil, key, ops6...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Generation).To(gm.Equal(uint32(5)))
				gm.Expect(len(rec.Bins)).To(gm.Equal(0))

				// GetOp should override GetHeaderOp
				ops7 := []*as.Operation{
					as.GetOp(),
					as.TouchOp(),
				}

				rec, err = client.Operate(nil, key, ops7...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Generation).To(gm.Equal(uint32(6)))
				gm.Expect(len(rec.Bins)).To(gm.Equal(1))
			})

			gg.It("must re-apply the same operations, and result should match expectation", func() {
				const listSize = 10
				const cdtBinName = "cdtBin"

				// First Part: For CDTs
				list := []interface{}{}
				opAppend := as.ListAppendOp(cdtBinName, 1)
				for i := 1; i <= listSize; i++ {
					list = append(list, i)

					sz, err := client.Operate(wpolicy, key, opAppend)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(sz.Bins[cdtBinName]).To(gm.Equal(i))
				}

				op := as.ListGetOp(cdtBinName, -1)
				cdtListRes, err := client.Operate(wpolicy, key, op)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(cdtListRes.Bins[cdtBinName]).To(gm.Equal(1))

				cdtListRes, err = client.Operate(wpolicy, key, op)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(cdtListRes.Bins[cdtBinName]).To(gm.Equal(1))

				// Second Part: For other normal Ops
				bin1 := as.NewBin("Aerospike1", 1)
				bin2 := as.NewBin("Aerospike2", "a")

				key, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				ops1 := []*as.Operation{
					as.PutOp(bin1),
					as.PutOp(bin2),
					as.GetOp(),
				}

				rec, err = client.Operate(nil, key, ops1...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int)))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string)))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(1)))

				ops2 := []*as.Operation{
					as.AddOp(bin1),
					as.AppendOp(bin2),
					as.GetOp(),
				}

				rec, err = client.Operate(nil, key, ops2...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) + 1))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string) + "a"))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))

				rec, err = client.Operate(nil, key, ops2...)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) + 2))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string) + "aa"))
				gm.Expect(rec.Generation).To(gm.Equal(uint32(3)))

			})

		}) // GetHeader context

	})

	gg.Describe("Commands Test", func() {

		gg.Context("XDR Filter", func() {

			gg.BeforeEach(func() {
				if !xdrEnabled() {
					gg.Skip("XDR Filter Tests are not supported in the Community Edition, or when the server is not configured for XDR")
					return
				}
			})

			gg.It("must successfully send SetXDRFilter command", func() {
				xp := as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(85),
				)

				err := client.SetXDRFilter(nil, "test", "test", xp)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must reject invalid Expression for SetXDRFilter command", func() {
				xp := as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpStringVal("some string"),
				)

				err := client.SetXDRFilter(nil, "test", "test", xp)
				gm.Expect(err).To(gm.HaveOccurred())
			})

			gg.It("must remove the server XDR filter using SetXDRFilter command", func() {
				err := client.SetXDRFilter(nil, "test", "test", nil)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

		}) // gg.Context

	}) // Describe

})
