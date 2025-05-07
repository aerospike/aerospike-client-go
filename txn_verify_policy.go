// Copyright 2014-2024 Aerospike, Inc.
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

import "time"

// Transaction policy fields used to batch verify record versions on commit.
// Used a placeholder for now as there are no additional fields beyond BatchPolicy.
type TxnVerifyPolicy struct {
	BatchPolicy
}

// NewTxnRollPolicy creates a new TxnVerifyPolicy instance with default values.
func NewTxnVerifyPolicy() *TxnVerifyPolicy {
	mp := *NewBatchPolicy()
	mp.ReadModeSC = ReadModeSCLinearize
	mp.ReplicaPolicy = MASTER
	mp.MaxRetries = 5
	mp.SocketTimeout = 3 * time.Second
	mp.TotalTimeout = 10 * time.Second
	mp.SleepBetweenRetries = 1 * time.Second

	return &TxnVerifyPolicy{
		BatchPolicy: mp,
	}
}
