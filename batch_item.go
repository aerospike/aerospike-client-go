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

type batchItem struct {
	index      int
	duplicates []int
}

func newBatchItemList(keys []*Key) map[string]*batchItem {

	keyMap := make(map[string]*batchItem, len(keys))

	for i, key := range keys {
		item := keyMap[string(key.digest)]

		if item == nil {
			item = newBatchItem(i)
			keyMap[string(key.digest)] = item
		} else {
			item.AddDuplicate(i)
		}
	}
	return keyMap
}

func newBatchItem(idx int) *batchItem {
	return &batchItem{
		index: idx,
	}
}

func (bi *batchItem) AddDuplicate(idx int) {
	if bi.duplicates == nil {
		bi.duplicates = []int{}
		bi.duplicates = append(bi.duplicates, bi.index)
		bi.index = 0
	}
	bi.duplicates = append(bi.duplicates, bi.index)
}

func (bi *batchItem) GetIndex() int {
	if bi.duplicates == nil {
		return bi.index
	}
	r := bi.duplicates[bi.index]
	bi.index++
	return r
}
