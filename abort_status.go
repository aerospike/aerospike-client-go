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

// Transaction abort status code.
type AbortStatus string

const (
	AbortStatusOK                AbortStatus = "Abort succeeded"
	AbortStatusAlreadyCommitted  AbortStatus = "Already committed"
	AbortStatusAlreadyAborted    AbortStatus = "Already aborted"
	AbortStatusCommitFailed      AbortStatus = "Abort not allowed because a commit already failed on this transaction with an in-doubt outcome"
	AbortStatusRollBackAbandoned AbortStatus = "Transaction client roll back abandoned. Server will eventually abort the Transaction."
	AbortStatusCloseAbandoned    AbortStatus = "Transaction has been rolled back, but Transaction client close was abandoned. Server will eventually close the Transaction."
)
