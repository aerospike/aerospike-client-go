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

import "iter"

type operateCommandRead struct {
	readCommand

	args operateArgs
}

func newOperateCommandRead(cluster *Cluster, key *Key, args operateArgs) (operateCommandRead, Error) {
	rdCommand, err := newReadCommand(cluster, &args.writePolicy.BasePolicy, key, nil)
	if err != nil {
		return operateCommandRead{}, err
	}

	res := operateCommandRead{
		readCommand: rdCommand,
		args:        args,
	}

	res.isOperation = true

	return res, nil
}

func (cmd *operateCommandRead) writeBuffer(ifc command) (err Error) {
	return cmd.setOperate(cmd.args.writePolicy, cmd.key, &cmd.args)
}

func (cmd *operateCommandRead) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *operateCommandRead) commandType() commandType {
	return ttOperate
}

func (cmd *operateCommandRead) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *operateCommandRead) getNamespace() *string {
	return &cmd.key.namespace
}
