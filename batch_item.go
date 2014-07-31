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

type BatchItem struct {
	index      int
	duplicates []int
}

func NewBatchItemList(keys []*Key) map[string]*BatchItem {

	keyMap := make(map[string]*BatchItem, len(keys))

	for i, key := range keys {
		item := keyMap[string(key.Digest())]

		if item == nil {
			item = NewBatchItem(i)
			keyMap[string(key.Digest())] = item
		} else {
			item.AddDuplicate(i)
		}
	}
	return keyMap
}

func NewBatchItem(idx int) *BatchItem {
	return &BatchItem{
		index: idx,
	}
}

func (this *BatchItem) AddDuplicate(idx int) {
	if this.duplicates == nil {
		this.duplicates = []int{}
		this.duplicates = append(this.duplicates, this.index)
		this.index = 0
	}
	this.duplicates = append(this.duplicates, this.index)
}

func (this *BatchItem) GetIndex() int {
	if this.duplicates == nil {
		return this.index
	} else {
		r := this.duplicates[this.index]
		this.index++
		return r
	}
}
