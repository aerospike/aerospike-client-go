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

// Create and manage a stack within a single bin. A stack is last in/first out (LIFO).
type LargeStack struct {
	baseLargeObject
}

// Initialize large stack operator.
//
// client        client
// policy        generic configuration parameters, pass in null for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass null for default set
func NewLargeStack(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeStack {
	return &LargeStack{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (this *LargeStack) packageName() string {
	return "lstack"
}

// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
//
// values      values to push
func (this *LargeStack) Push(values ...Value) error {
	var err error
	if len(values) == 1 {
		this.client.Execute(this.policy, this.key, this.packageName(), "push", this.binName, values[0], this.userModule)
	} else {
		this.client.Execute(this.policy, this.key, this.packageName(), "push_all", this.binName, NewValueArray(values), this.userModule)
	}
	return err
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// returns          list of items selected

func (this *LargeStack) Peek(peekCount int) ([]interface{}, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "peek", this.binName, NewIntegerValue(peekCount))
	if err != nil {
		return nil, err
	}
	return ret.([]interface{}), nil
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// returns          list of items selected
func (this *LargeStack) Pop(count int) ([]interface{}, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "pop", this.binName, NewIntegerValue(count))
	if err != nil {
		return nil, err
	}
	return ret.([]interface{}), nil
}

// Return all objects in the list.

func (this *LargeStack) Scan() ([]interface{}, error) {
	return this.scan(this)
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of items selected
func (this *LargeStack) Filter(peekCount int, filterName string, filterArgs ...Value) ([]interface{}, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "filter", this.binName, NewIntegerValue(peekCount), this.userModule, NewStringValue(filterName), NewValueArray(filterArgs))
	if err != nil {
		return nil, err
	}
	return ret.([]interface{}), nil
}

// Delete bin containing the list.
func (this *LargeStack) Destroy() error {
	return this.destroy(this)
}

// Return size of list.
func (this *LargeStack) Size() (int, error) {
	return this.size(this)
}

// Return map of list configuration parameters.
func (this *LargeStack) GetConfig() (map[interface{}]interface{}, error) {
	return this.getConfig(this)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (this *LargeStack) SetCapacity(capacity int) error {
	return this.setCapacity(this, capacity)
}

// Return maximum number of entries in the list.
func (this *LargeStack) GetCapacity() (int, error) {
	return this.getCapacity(this)
}
