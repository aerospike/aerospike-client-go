// Copyright 2017 Aerospike, Inc.
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

func (e *expression) predicates() (res []predExp) {
	if e.right != nil {
		res = append(res, e.right.predicates()...)
	}
	if e.left != nil {
		res = append(res, e.left.predicates()...)
	}
	res = append(res, e.predicate)

	return res
}

func (e *expression) Predicates() (res []predExp) {
	return e.predicates()
}

func (e *boolExpression) predicates() (res []predExp) {
	if e.right != nil {
		res = append(res, e.right.predicates()...)
	}
	if e.left != nil {
		res = append(res, e.left.predicates()...)
	}
	res = append(res, e.predicate)

	return res
}

func (e *boolExpression) Predicates() (res []predExp) {
	return e.predicates()
}
