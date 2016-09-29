// Copyright 2013-2016 Aerospike, Inc.
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

type peers struct {
	peers        []*peer
	hosts        map[Host]struct{}
	nodes        map[string]*Node
	refreshCount int
	usePeers     bool
	genChanged   bool
}

func newPeers(peerCapacity int, addCapacity int) *peers {
	return &peers{
		peers:    make([]*peer, peerCapacity),
		hosts:    make(map[Host]struct{}, addCapacity),
		nodes:    make(map[string]*Node, addCapacity),
		usePeers: true,
	}
}

type peer struct {
	nodeName string
	tlsName  string
	hosts    []*Host
}
