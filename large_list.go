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
// policy        generic configuration parameters, pass in null for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass null for default list
func NewLargeList(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeList {
	return &LargeList{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (this *LargeList) packageName() string {
	return "llist"
}

// Add values to the list.  If the list does not exist, create it using specified userModule configuration.
//
// values      values to add
func (this *LargeList) Add(values ...Value) error {
	var err error
	if len(values) == 1 {
		_, err = this.client.Execute(this.policy, this.key, this.packageName(), "add", this.binName, values[0], this.userModule)
	} else {
		_, err = this.client.Execute(this.policy, this.key, this.packageName(), "add_all", this.binName, NewValueArray(values), this.userModule)
	}
	return err
}

// Delete value from list.
//
// value       value to delete
func (this *LargeList) Remove(value Value) error {
	_, err := this.client.Execute(this.policy, this.key, this.packageName(), "remove", this.binName, value)
	return err
}

// Select values from list.
//
// value       value to select
// returns          list of entries selected
func (this *LargeList) Find(value Value) ([]interface{}, error) {
	res, err := this.client.Execute(this.policy, this.key, this.packageName(), "find", this.binName, value)
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err
}

// Select values from list and apply specified Lua filter.
//
// value       value to select
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (this *LargeList) FindThenFilter(value Value, filterName string, filterArgs ...Value) ([]interface{}, error) {
	res, err := this.client.Execute(this.policy, this.key, this.packageName(), "find_then_filter", this.binName, value, this.userModule, NewValue(filterName), NewValue(filterArgs))
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err
}

// Return all objects in the list.
func (this *LargeList) Scan() ([]interface{}, error) {
	return this.scan(this)
}

// Select values from list and apply specified Lua filter.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// returns          list of entries selected
func (this *LargeList) Filter(filterName string, filterArgs ...Value) ([]interface{}, error) {
	res, err := this.client.Execute(this.policy, this.key, this.packageName(), "filter", this.binName, this.userModule, NewValue(filterName), NewValue(filterArgs))
	if err != nil {
		return nil, err
	}
	return res.([]interface{}), err
}

// Delete bin containing the list.
func (this *LargeList) Destroy() error {
	return this.destroy(this)
}

// Return size of list.
func (this *LargeList) Size() (int, error) {
	return this.size(this)
}

// Return map of list configuration parameters.
func (this *LargeList) GetConfig() (map[interface{}]interface{}, error) {
	return this.getConfig(this)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (this *LargeList) SetCapacity(capacity int) error {
	return this.setCapacity(this, capacity)
}

// Return maximum number of entries in the list.
func (this *LargeList) GetCapacity() (int, error) {
	return this.getCapacity(this)
}
