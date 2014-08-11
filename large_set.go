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
// policy        generic configuration parameters, pass in null for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass null for default set

func NewLargeSet(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeSet {
	return &LargeSet{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (this *LargeSet) packageName() string {
	return "lset"
}

// Add values to the set.  If the set does not exist, create it using specified userModule configuration.
//
// values      values to add
func (this *LargeSet) Add(values ...Value) error {
	var err error
	if len(values) == 1 {
		_, err = this.client.Execute(this.policy, this.key, this.packageName(), "add", this.binName, values[0], this.userModule)
	} else {
		_, err = this.client.Execute(this.policy, this.key, this.packageName(), "add_all", this.binName, NewValueArray(values), this.userModule)
	}

	return err
}

// Delete value from set.
//
// value       value to delete
func (this *LargeSet) Remove(value Value) error {
	_, err := this.client.Execute(this.policy, this.key, this.packageName(), "remove", this.binName, value)
	return err
}

// Select value from set.
//
// value       value to select
// returns          found value
func (this *LargeSet) Get(value Value) (interface{}, error) {
	return this.client.Execute(this.policy, this.key, this.packageName(), "get", this.binName, value)
}

// Check existence of value in the set.
//
// value       value to check
// returns          true if found, otherwise false
func (this *LargeSet) Exists(value Value) (bool, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "exists", this.binName, value)
	if err != nil {
		return false, err
	}
	return (ret == 1), nil
}

// Return all objects in the list.

func (this *LargeSet) Scan() ([]interface{}, error) {
	return this.scan(this)
}

// Select values from set and apply specified Lua filter.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (this *LargeSet) Filter(filterName string, filterArgs ...Value) ([]interface{}, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "filter", this.binName, this.userModule, NewStringValue(filterName), NewValueArray(filterArgs))
	if err != nil {
		return nil, err
	}
	return ret.([]interface{}), err
}

// Delete bin containing the list.
func (this *LargeSet) Destroy() error {
	return this.destroy(this)
}

// Return size of list.
func (this *LargeSet) Size() (int, error) {
	return this.size(this)
}

// Return map of list configuration parameters.
func (this *LargeSet) GetConfig() (map[interface{}]interface{}, error) {
	return this.getConfig(this)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (this *LargeSet) SetCapacity(capacity int) error {
	return this.setCapacity(this, capacity)
}

// Return maximum number of entries in the list.
func (this *LargeSet) GetCapacity() (int, error) {
	return this.getCapacity(this)
}
