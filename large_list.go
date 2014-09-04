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

// Create and manage a list within a single bin.
///
type LargeList struct {
	baseLargeObject
}

// Initialize large list operator.
//
// client        client
// policy        generic configuration parameters, pass in nil for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass nil for default list
func NewLargeList(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	return &LargeList{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (ll *LargeList) packageName() string {
	return "llist"
}

// Add values to the list.  If the list does not exist, create it using specified userModule configuration.
//
// values      values to add
func (ll *LargeList) Add(values ...interface{}) error {
	var err error
	if len(values) == 1 {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "add", ll.binName, NewValue(values[0]), ll.userModule)
	} else {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "add_all", ll.binName, ToValueArray(values), ll.userModule)
	}
	return err
}

// Delete value from list.
//
// value       value to delete
func (ll *LargeList) Remove(value interface{}) error {
	_, err := ll.client.Execute(ll.policy, ll.key, ll.packageName(), "remove", ll.binName, NewValue(value))
	return err
}

// Select values from list.
//
// value       value to select
// returns          list of entries selected
func (ll *LargeList) Find(value interface{}) ([]interface{}, error) {
	res, err := ll.client.Execute(ll.policy, ll.key, ll.packageName(), "find", ll.binName, NewValue(value))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.([]interface{}), err
}

// Select values from list and apply specified Lua filter.
//
// value       value to select
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (ll *LargeList) FindThenFilter(value interface{}, filterName string, filterArgs ...interface{}) ([]interface{}, error) {
	res, err := ll.client.Execute(ll.policy, ll.key, ll.packageName(), "find_then_filter", ll.binName, NewValue(value), ll.userModule, NewValue(filterName), ToValueArray(filterArgs))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.([]interface{}), err
}

// Return all objects in the list.
func (ll *LargeList) Scan() ([]interface{}, error) {
	return ll.scan(ll)
}

// Select values from list and apply specified Lua filter.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (ll *LargeList) Filter(filterName string, filterArgs ...interface{}) ([]interface{}, error) {
	res, err := ll.client.Execute(ll.policy, ll.key, ll.packageName(), "filter", ll.binName, ll.userModule, NewValue(filterName), ToValueArray(filterArgs))
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}
	return res.([]interface{}), err
}

// Delete bin containing the list.
func (ll *LargeList) Destroy() error {
	return ll.destroy(ll)
}

// Return size of list.
func (ll *LargeList) Size() (int, error) {
	return ll.size(ll)
}

// Return map of list configuration parameters.
func (ll *LargeList) GetConfig() (map[interface{}]interface{}, error) {
	return ll.getConfig(ll)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (ll *LargeList) SetCapacity(capacity int) error {
	return ll.setCapacity(ll, capacity)
}

// Return maximum number of entries in the list.
func (ll *LargeList) GetCapacity() (int, error) {
	return ll.getCapacity(ll)
}
