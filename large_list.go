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

// LargeList encapsulates a list within a single bin.
type LargeList struct {
	baseLargeObject
}

// NewLargeList initializes a large list operator.
func NewLargeList(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	return &LargeList{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (ll *LargeList) packageName() string {
	return "llist"
}

// Add adds values to the list.
// If the list does not exist, create it using specified userModule configuration.
func (ll *LargeList) Add(values ...interface{}) error {
	var err error
	if len(values) == 1 {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "add", ll.binName, NewValue(values[0]), ll.userModule)
	} else {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "add_all", ll.binName, ToValueArray(values), ll.userModule)
	}
	return err
}

// Update updates/adds each value in values list depending if key exists or not.
func (ll *LargeList) Update(values ...interface{}) error {
	var err error
	if len(values) == 1 {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "update", ll.binName, NewValue(values[0]), ll.userModule)
	} else {
		_, err = ll.client.Execute(ll.policy, ll.key, ll.packageName(), "update_all", ll.binName, ToValueArray(values), ll.userModule)
	}
	return err
}

// Remove deletes value from list.
func (ll *LargeList) Remove(value interface{}) error {
	_, err := ll.client.Execute(ll.policy, ll.key, ll.packageName(), "remove", ll.binName, NewValue(value))
	return err
}

// Find selects values from list.
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

// FindThenFilter selects values from list and applies specified Lua filter.
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

// Scan returns all objects in the list.
func (ll *LargeList) Scan() ([]interface{}, error) {
	return ll.scan(ll)
}

// Filter selects values from list and apply specified Lua filter.
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

// Destroy deletes the bin containing the list.
func (ll *LargeList) Destroy() error {
	return ll.destroy(ll)
}

// Size returns size of list.
func (ll *LargeList) Size() (int, error) {
	return ll.size(ll)
}

// GetConfig returns map of list configuration parameters.
func (ll *LargeList) GetConfig() (map[interface{}]interface{}, error) {
	return ll.getConfig(ll)
}

// SetCapacity sets maximum number of entries in the list.
func (ll *LargeList) SetCapacity(capacity int) error {
	return ll.setCapacity(ll, capacity)
}

// GetCapacity returns maximum number of entries in the list.
func (ll *LargeList) GetCapacity() (int, error) {
	return ll.getCapacity(ll)
}
