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

package aerospike

import "github.com/aerospike/aerospike-client-go/types"

type batchIndexCommandGet struct {
	batchCommandGet
}

func newBatchIndexCommandGet(
	batch *batchNode,
	policy *BatchPolicy,
	records []*BatchRead,
) *batchIndexCommandGet {
	var node *Node
	if batch != nil {
		node = batch.Node
	}

	res := &batchIndexCommandGet{
		batchCommandGet{
			batchCommand: batchCommand{
				baseMultiCommand: *newMultiCommand(node, nil),
				policy:           policy,
				batch:            batch,
			},
			records:      nil,
			indexRecords: records,
		},
	}
	return res
}

func (cmd *batchIndexCommandGet) cloneBatchCommand(batch *batchNode) batcher {
	res := *cmd
	res.batch = batch
	res.node = batch.Node

	return &res
}

func (cmd *batchIndexCommandGet) writeBuffer(ifc command) error {
	return cmd.setBatchIndexRead(cmd.policy, cmd.indexRecords, cmd.batch)
}

func (cmd *batchIndexCommandGet) directGet(client *Client) error {
	var errs []error
	for _, br := range cmd.indexRecords {
		var err error
		if br.headerOnly() {
			br.Record, err = client.GetHeader(&cmd.policy.BasePolicy, br.Key)
		} else {
			br.Record, err = client.Get(&cmd.policy.BasePolicy, br.Key, br.BinNames...)
		}
		if err != nil {
			// Key not found is an error for batch requests
			if err == types.ErrKeyNotFound {
				continue
			}

			if err == types.ErrFilteredOut {
				cmd.filteredOutCnt++
				continue
			}

			if cmd.policy.AllowPartialResults {
				errs = append(errs, err)
				continue
			} else {
				return err
			}
		}
	}
	return mergeErrors(errs)
}

func (cmd *batchIndexCommandGet) Execute() error {
	return cmd.execute(cmd, true)
}

func (cmd *batchIndexCommandGet) generateBatchNodes(cluster *Cluster) ([]*batchNode, error) {
	return newBatchNodeListRecords(cluster, cmd.policy, cmd.indexRecords, cmd.sequenceAP, cmd.sequenceSC, cmd.batch)
}
