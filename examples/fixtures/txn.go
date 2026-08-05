/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Fixture factories for the transaction examples.

package fixtures

func TxnBasic() Fixture {
	const key = "txn-basic-1"
	return Fixture{
		Setup: func() error { return DeleteKeys(key) },
		// The committed transaction's final update must be visible.
		Validate: func() error { return AssertBin(key, "a", 5678) },
		Cleanup:  func() error { return DeleteKeys(key) },
	}
}

// TxnConcurrent takes the example's key ranges so cleanup matches its scaled
// settings: [0, txnKeys) for the concurrent/batch parts, mixed-batch records
// starting at 100000 and query records starting at 200000.
func TxnConcurrent(txnKeys, mixedTotal, queryTotal int) Fixture {
	return Fixture{
		// The example creates all of its own state; the fixture only
		// guarantees nothing is left behind. Results are randomized, so
		// success is "ran to completion without error".
		Cleanup: func() error {
			if err := DeleteIntKeys(0, txnKeys-1); err != nil {
				return err
			}
			if err := DeleteIntKeys(100000, 100000+mixedTotal-1); err != nil {
				return err
			}
			return DeleteIntKeys(200000, 200000+queryTotal-1)
		},
	}
}
