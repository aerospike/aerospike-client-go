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

import "github.com/aerospike/aerospike-client-go/v6/types"

type queryObjectsCommand struct {
	queryCommand
}

func newQueryObjectsCommand(node *Node, policy *QueryPolicy, statement *Statement, recordset *Recordset) *queryObjectsCommand {
	cmd := &queryObjectsCommand{
		queryCommand: *newQueryCommand(node, policy, nil, statement, nil, recordset),
	}

	cmd.terminationErrorType = types.QUERY_TERMINATED

	return cmd
}

func (cmd *queryObjectsCommand) Execute() Error {
	defer cmd.recordset.signalEnd()
	err := cmd.execute(cmd)
	if err != nil {
		cmd.recordset.sendError(err)
	}
	return err
}
