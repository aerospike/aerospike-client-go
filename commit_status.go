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

// Transaction commit status code.
type CommitStatus string

const (
	CommitStatusOK                       CommitStatus = "Commit succeeded"
	CommitStatusUnverified               CommitStatus = "Commit process was disrupted on client side and unverified"
	CommitStatusAlreadyCommitted         CommitStatus = "Already committed"
	CommitStatusAlreadyAborted           CommitStatus = "Already aborted"
	CommitStatusMarkRollForwardAbandoned CommitStatus = "Transaction client mark roll forward abandoned. Server will eventually abort the Transaction."
	CommitStatusRollForwardAbandoned     CommitStatus = "Transaction client roll forward abandoned. Server will eventually commit the Transaction."
	CommitStatusCloseAbandoned           CommitStatus = "Transaction has been rolled forward, but Transaction client close was abandoned. Server will eventually close the Transaction."
)
