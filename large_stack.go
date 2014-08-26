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
// policy        generic configuration parameters, pass in nil for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass nil for default set
func NewLargeStack(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeStack {
	return &LargeStack{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (lstk *LargeStack) packageName() string {
	return "lstack"
}

// Push values onto stack.  If the stack does not exist, create it using specified userModule configuration.
//
// values      values to push
func (lstk *LargeStack) Push(values ...interface{}) error {
	var err error
	if len(values) == 1 {
		lstk.client.Execute(lstk.policy, lstk.key, lstk.packageName(), "push", lstk.binName, NewValue(values[0]), lstk.userModule)
	} else {
		lstk.client.Execute(lstk.policy, lstk.key, lstk.packageName(), "push_all", lstk.binName, ToValueArray(values), lstk.userModule)
	}
	return err
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// returns          list of items selected
func (lstk *LargeStack) Peek(peekCount int) ([]interface{}, error) {
	res, err := lstk.client.Execute(lstk.policy, lstk.key, lstk.packageName(), "peek", lstk.binName, NewIntegerValue(peekCount))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	} else {
		return res.([]interface{}), nil
	}
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// returns          list of items selected
func (lstk *LargeStack) Pop(count int) ([]interface{}, error) {
	res, err := lstk.client.Execute(lstk.policy, lstk.key, lstk.packageName(), "pop", lstk.binName, NewIntegerValue(count))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	} else {
		return res.([]interface{}), nil
	}
}

// Return all objects in the list.

func (lstk *LargeStack) Scan() ([]interface{}, error) {
	return lstk.scan(lstk)
}

// Select items from top of stack.
//
// peekCount     number of items to select.
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of items selected
func (lstk *LargeStack) Filter(peekCount int, filterName string, filterArgs ...interface{}) ([]interface{}, error) {
	res, err := lstk.client.Execute(lstk.policy, lstk.key, lstk.packageName(), "filter", lstk.binName, NewIntegerValue(peekCount), lstk.userModule, NewStringValue(filterName), ToValueArray(filterArgs))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	} else {
		return res.([]interface{}), nil
	}
}

// Delete bin containing the list.
func (lstk *LargeStack) Destroy() error {
	return lstk.destroy(lstk)
}

// Return size of list.
func (lstk *LargeStack) Size() (int, error) {
	return lstk.size(lstk)
}

// Return map of list configuration parameters.
func (lstk *LargeStack) GetConfig() (map[interface{}]interface{}, error) {
	return lstk.getConfig(lstk)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (lstk *LargeStack) SetCapacity(capacity int) error {
	return lstk.setCapacity(lstk, capacity)
}

// Return maximum number of entries in the list.
func (lstk *LargeStack) GetCapacity() (int, error) {
	return lstk.getCapacity(lstk)
}
