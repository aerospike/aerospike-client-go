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

// Create and manage a map within a single bin.
type LargeMap struct {
	baseLargeObject
}

// Initialize large map operator.
//
// client        client
// policy        generic configuration parameters, pass in null for defaults
// key         unique record identifier
// binName       bin name
// userModule      Lua function name that initializes list configuration parameters, pass null for default set
func NewLargeMap(client *Client, policy *WritePolicy, key *Key, binName string, userModule string) *LargeMap {
	return &LargeMap{
		baseLargeObject: *newLargeObject(client, policy, key, binName, userModule),
	}
}

func (this *LargeMap) packageName() string {
	return "lmap"
}

// Add entry to map.  If the map does not exist, create it using specified userModule configuration.
//
// name        entry key
// value       entry value
func (this *LargeMap) Put(name Value, value Value) error {
	_, err := this.client.Execute(this.policy, this.key, this.packageName(), "put", this.binName, name, value, this.userModule)
	return err
}

// Add map values to map.  If the map does not exist, create it using specified userModule configuration.
//
// map       map values to push
func (this *LargeMap) PutMap(theMap map[interface{}]interface{}) error {
	_, err := this.client.Execute(this.policy, this.key, this.packageName(), "put_all", this.binName, NewMapValue(theMap), this.userModule)
	return err
}

// Get value from map given name key.
//
// name        key.
// return          map of items selected
func (this *LargeMap) Get(name Value) (map[interface{}]interface{}, error) {
	res, err := this.client.Execute(this.policy, this.key, this.packageName(), "get", this.binName, name)
	return res.(map[interface{}]interface{}), err
}

// Return all objects in the list.
func (this *LargeMap) Scan() (map[interface{}]interface{}, error) {
	ret, err := this.client.Execute(this.policy, this.key, this.packageName(), "scan", this.binName)
	if err != nil {
		return nil, err
	}
	return ret.(map[interface{}]interface{}), nil
}

// Select items from map.
//
// filterName    Lua function name which applies filter to returned list
// filterArgs    arguments to Lua function name
// return          list of items selected
func (this *LargeMap) Filter(filterName string, filterArgs ...Value) (map[interface{}]interface{}, error) {
	res, err := this.client.Execute(this.policy, this.key, this.packageName(), "filter", this.binName, this.userModule, NewStringValue(filterName), NewValue(filterArgs))
	return res.(map[interface{}]interface{}), err

}

// Delete bin containing the list.
func (this *LargeMap) Destroy() error {
	return this.destroy(this)
}

// Return size of list.
func (this *LargeMap) Size() (int, error) {
	return this.size(this)
}

// Return map of list configuration parameters.
func (this *LargeMap) GetConfig() (map[interface{}]interface{}, error) {
	return this.getConfig(this)
}

// Set maximum number of entries in the list.
//
// capacity      max entries in list
func (this *LargeMap) SetCapacity(capacity int) error {
	return this.setCapacity(this, capacity)
}

// Return maximum number of entries in the list.
func (this *LargeMap) GetCapacity() (int, error) {
	return this.getCapacity(this)
}
