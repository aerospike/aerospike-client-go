// Copyright 2013-2014 Aerospike, Inc.
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

type ExecuteCommand struct {
	ReadCommand

	packageName  string
	functionName string
	args         []Value
}

func NewExecuteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	packageName string,
	functionName string,
	args []Value,
) *ExecuteCommand {
	return &ExecuteCommand{
		ReadCommand:  *NewReadCommand(cluster, policy, key, nil),
		packageName:  packageName,
		functionName: functionName,
		args:         args,
	}
}

func (this *ExecuteCommand) writeBuffer(ifc Command) error {
	return this.SetUdf(this.key, this.packageName, this.functionName, this.args)
}

func (this *ExecuteCommand) Execute() error {
	return this.execute(this)
}
