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

// Create and manage a set within a single bin.
type LargeSet struct {
	baseLargeObject
}

// Initialize large set operator.
//
// client        client
// policy        generic configuration parameters, pass in nil for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass nil for default set

func NewLargeSet(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeSet {
	return &LargeSet{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (ls *LargeSet) packageName() string {
	return "lset"
}

// Add values to the set.  If the set does not exist, create it using specified userModule configuration.
//
// values      values to add
func (ls *LargeSet) Add(values ...interface{}) error {
	var err error
	if len(values) == 1 {
		_, err = ls.client.Execute(ls.policy, ls.key, ls.packageName(), "add", ls.binName, NewValue(values[0]), ls.userModule)
	} else {
		_, err = ls.client.Execute(ls.policy, ls.key, ls.packageName(), "add_all", ls.binName, ToValueArray(values), ls.userModule)
	}

	return err
}

// Delete value from set.
//
// value       value to delete
func (ls *LargeSet) Remove(value interface{}) error {
	_, err := ls.client.Execute(ls.policy, ls.key, ls.packageName(), "remove", ls.binName, NewValue(value))
	return err
}

// Select value from set.
//
// value       value to select
// returns          found value
func (ls *LargeSet) Get(value interface{}) (interface{}, error) {
	return ls.client.Execute(ls.policy, ls.key, ls.packageName(), "get", ls.binName, NewValue(value))
}

// Check existence of value in the set.
//
// value       value to check
// returns          true if found, otherwise false
func (ls *LargeSet) Exists(value interface{}) (bool, error) {
	ret, err := ls.client.Execute(ls.policy, ls.key, ls.packageName(), "exists", ls.binName, NewValue(value))
	if err != nil {
		return false, err
	}
	return (ret == 1), nil
}

// Return all objects in the list.

func (ls *LargeSet) Scan() ([]interface{}, error) {
	return ls.scan(ls)
}

// Select values from set and apply specified Lua filter.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (ls *LargeSet) Filter(filterName string, filterArgs ...interface{}) ([]interface{}, error) {
	res, err := ls.client.Execute(ls.policy, ls.key, ls.packageName(), "filter", ls.binName, ls.userModule, NewStringValue(filterName), ToValueArray(filterArgs))
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	} else {
		return res.([]interface{}), err
	}
}

// Delete bin containing the list.
func (ls *LargeSet) Destroy() error {
	return ls.destroy(ls)
}

// Return size of list.
func (ls *LargeSet) Size() (int, error) {
	return ls.size(ls)
}

// Return map of list configuration parameters.
func (ls *LargeSet) GetConfig() (map[interface{}]interface{}, error) {
	return ls.getConfig(ls)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (ls *LargeSet) SetCapacity(capacity int) error {
	return ls.setCapacity(ls, capacity)
}

// Return maximum number of entries in the list.
func (ls *LargeSet) GetCapacity() (int, error) {
	return ls.getCapacity(ls)
}
