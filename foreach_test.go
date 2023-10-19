// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/timshannon/badgerhold/v4"
)

func TestForEach(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				count := 0
				err := store.ForEach(tst.query, func(record *ItemTest) error {
					count++

					found := false
					for i := range tst.result {
						if record.equal(&testData[tst.result[i]]) {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							return fmt.Errorf("%v was not found in the result set! Full results: %v",
								record, tst.result)
						}
						return fmt.Errorf("%v was not found in the result set!", record)
					}

					return nil
				})
				if count != len(tst.result) {
					t.Fatalf("ForEach count is %d wanted %d.", count, len(tst.result))
				}
				if err != nil {
					t.Fatalf("Error during ForEach iteration: %s", err)
				}
			})
		}
	})
}

func TestIssue105ForEachKeys(t *testing.T) {

	type Person struct {
		ID     uint64 `badgerhold:"key"`
		Name   string
		Gender string
		Birth  time.Time
	}

	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		data := &Person{Name: "tester1"}
		ok(t, store.Insert(badgerhold.NextSequence(), data))
		equals(t, uint64(0), data.ID)

		data = &Person{Name: "tester2"}
		ok(t, store.Insert(badgerhold.NextSequence(), data))
		equals(t, uint64(1), data.ID)

		data = &Person{Name: "tester3"}
		ok(t, store.Insert(badgerhold.NextSequence(), data))
		equals(t, uint64(2), data.ID)

		var id uint64 = 0

		ok(t, store.ForEach(nil, func(record *Person) error {
			assert(t, id == record.ID, record.Name+" incorrectly set key")
			id++
			return nil
		}))
	})
}
