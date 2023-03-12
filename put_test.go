// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/timshannon/badgerhold/v4"
)

func TestInsert(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error inserting data for test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		// test duplicate insert
		err = store.Insert(key, &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		})

		if err != badgerhold.ErrKeyExists {
			t.Fatalf("Insert didn't fail! Expected %s got %s", badgerhold.ErrKeyExists, err)
		}

	})
}

func TestInsertReadTxn(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Badger().View(func(tx *badger.Txn) error {
			return store.TxInsert(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Inserting into a read only transaction didn't fail!")
		}

	})
}

func TestUpdate(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Update(key, data)
		if err != badgerhold.ErrNotFound {
			t.Fatalf("Update without insert didn't fail! Expected %s got %s", badgerhold.ErrNotFound, err)
		}

		err = store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = store.Update(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Update didn't complete.  Expected %v, got %v", update, result)
		}

	})
}

func TestUpdateReadTxn(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Badger().View(func(tx *badger.Txn) error {
			return store.TxUpdate(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Updating into a read only transaction didn't fail!")
		}

	})
}

func TestUpsert(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Upsert(key, data)
		if err != nil {
			t.Fatalf("Error upserting data: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = store.Upsert(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Upsert didn't complete.  Expected %v, got %v", update, result)
		}
	})
}

func TestUpsertReadTxn(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.Badger().View(func(tx *badger.Txn) error {
			return store.TxUpsert(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Updating into a read only transaction didn't fail!")
		}

	})
}

func TestUpdateMatching(t *testing.T) {
	for _, tst := range testResults {
		t.Run(tst.name, func(t *testing.T) {
			testWrap(t, func(store *badgerhold.Store, t *testing.T) {

				insertTestData(t, store)

				err := store.UpdateMatching(&ItemTest{}, tst.query, func(record interface{}) error {
					update, ok := record.(*ItemTest)
					if !ok {
						return fmt.Errorf("Record isn't the correct type!  Wanted Itemtest, got %T",
							record)
					}

					update.UpdateField = "updated"
					update.UpdateIndex = "updated index"

					return nil
				})

				if err != nil {
					t.Fatalf("Error updating data from badgerhold: %s", err)
				}

				var result []ItemTest
				err = store.Find(&result, badgerhold.Where("UpdateIndex").Eq("updated index").
					And("UpdateField").Eq("updated"))
				if err != nil {
					t.Fatalf("Error finding result after update from badgerhold: %s", err)
				}

				if len(result) != len(tst.result) {
					if testing.Verbose() {
						t.Fatalf("Find result count after update is %d wanted %d.  Results: %v",
							len(result), len(tst.result), result)
					}
					t.Fatalf("Find result count after update is %d wanted %d.", len(result),
						len(tst.result))
				}

				for i := range result {
					found := false
					for k := range tst.result {
						if result[i].Key == testData[tst.result[k]].Key &&
							result[i].UpdateField == "updated" &&
							result[i].UpdateIndex == "updated index" {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							t.Fatalf("Could not find %v in the update result set! Full results: %v",
								result[i], result)
						}
						t.Fatalf("Could not find %v in the updated result set!", result[i])
					}
				}

			})

		})
	}
}

func TestIssue14(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		err = store.Update(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = store.Find(&result, badgerhold.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}

	})
}

func TestIssue14Upsert(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		err = store.Upsert(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = store.Find(&result, badgerhold.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}

	})
}

func TestIssue14UpdateMatching(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		err = store.UpdateMatching(&ItemTest{}, badgerhold.Where("Name").Eq("Test Name"),
			func(record interface{}) error {
				update, ok := record.(*ItemTest)
				if !ok {
					return fmt.Errorf("Record isn't the correct type!  Wanted Itemtest, got %T", record)
				}

				update.Category = "Test Category Updated"

				return nil
			})

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = store.Find(&result, badgerhold.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}

	})
}

func TestInsertSequence(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {

		type SequenceTest struct {
			Key uint64 `badgerholdKey:"Key"`
		}

		for i := 0; i < 10; i++ {
			err := store.Insert(badgerhold.NextSequence(), &SequenceTest{})
			if err != nil {
				t.Fatalf("Error inserting data for sequence test: %s", err)
			}
		}

		var result []SequenceTest

		err := store.Find(&result, nil)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		for i := 0; i < 10; i++ {
			if i != int(result[i].Key) {
				t.Fatalf("Sequence is not correct.  Wanted %d, got %d", i, result[i].Key)
			}
		}

	})
}

func TestInsertSequenceSetKey(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {

		// Properly tagged, passed by reference, and the field is the same type
		// as bucket.NextSequence() produces
		type InsertSequenceSetKeyTest struct {
			// badgerhold.NextSequence() creates an auto-key that is a uint64
			Key uint64 `badgerholdKey:"Key"`
		}

		for i := 0; i < 10; i++ {
			st := InsertSequenceSetKeyTest{}
			if st.Key != 0 {
				t.Fatalf("Zero value of test data should be 0")
			}
			err := store.Insert(badgerhold.NextSequence(), &st)
			if err != nil {
				t.Fatalf("Error inserting data for sequence test: %s", err)
			}
			if int(st.Key) != i {
				t.Fatalf("Inserted data's key field was not updated as expected.  Wanted %d, got %d",
					i, st.Key)
			}
		}
	})
}

func TestInsertSetKey(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {

		type TestInsertSetKey struct {
			Key uint `badgerholdKey:"Key"`
		}

		t.Run("Valid", func(t *testing.T) {
			st := TestInsertSetKey{}
			key := uint(123)
			err := store.Insert(key, &st)
			if err != nil {
				t.Fatalf("Error inserting data for key set test: %s", err)
			}
			if st.Key != key {
				t.Fatalf("Key was not set.  Wanted %d, got %d", key, st.Key)
			}
		})

		// same as "Valid", but passed by value instead of reference
		t.Run("NotSettable", func(t *testing.T) {
			st := TestInsertSetKey{}
			key := uint(234)
			err := store.Insert(key, st)
			if err != nil {
				t.Fatalf("Error inserting data for key set test: %s", err)
			}
			if st.Key != 0 {
				t.Fatalf("Key was set incorrectly.  Wanted %d, got %d", 0, st.Key)
			}
		})

		t.Run("NonZero", func(t *testing.T) {
			key := uint(456)
			st := TestInsertSetKey{424242}
			err := store.Insert(key, &st)
			if err != nil {
				t.Fatalf("Error inserting data for key set test: %s", err)
			}
			if st.Key != 424242 {
				t.Fatalf("Key was set incorrectly.  Wanted %d, got %d", 424242, st.Key)
			}
		})

		t.Run("TypeMismatch", func(t *testing.T) {
			key := int(789)
			st := TestInsertSetKey{}
			err := store.Insert(key, &st)
			if err != nil {
				t.Fatalf("Error inserting data for key set test: %s", err)
			}
			// The fact that we can't even compare them is a pretty good sign
			// that we can't set the key when the types don't match
			if st.Key != 0 {
				t.Fatalf("Key was not set.  Wanted %d, got %d", 0, st.Key)
			}
		})

	})
}

func TestAlternateTags(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		type TestAlternate struct {
			Key  uint64 `badgerhold:"key"`
			Name string `badgerhold:"index"`
		}
		item := TestAlternate{
			Name: "TestName",
		}

		key := uint64(123)
		err := store.Insert(key, &item)
		if err != nil {
			t.Fatalf("Error inserting data for alternate tag test: %s", err)
		}

		if item.Key != key {
			t.Fatalf("Key was not set.  Wanted %d, got %d", key, item.Key)
		}

		var result []TestAlternate

		err = store.Find(&result, badgerhold.Where("Name").Eq(item.Name).Index("Name"))
		if err != nil {
			t.Fatalf("Query on alternate tag index failed: %s", err)
		}

		if len(result) != 1 {
			t.Fatalf("Expected 1 got %d", len(result))
		}
	})
}

func TestUniqueConstraint(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		type TestUnique struct {
			Key  uint64 `badgerhold:"key"`
			Name string `badgerhold:"unique"`
		}

		item := &TestUnique{
			Name: "Tester Name",
		}

		err := store.Insert(badgerhold.NextSequence(), item)
		if err != nil {
			t.Fatalf("Error inserting base record for unique testing: %s", err)
		}

		t.Run("Insert", func(t *testing.T) {
			err = store.Insert(badgerhold.NextSequence(), item)
			if err != badgerhold.ErrUniqueExists {
				t.Fatalf("Inserting duplicate record did not result in a unique constraint error: "+
					"Expected %s, Got %s", badgerhold.ErrUniqueExists, err)
			}
		})

		t.Run("Update", func(t *testing.T) {
			update := &TestUnique{
				Name: "Update Name",
			}
			err = store.Insert(badgerhold.NextSequence(), update)

			if err != nil {
				t.Fatalf("Inserting record for update Unique testing failed: %s", err)
			}
			update.Name = item.Name

			err = store.Update(update.Key, update)
			if err != badgerhold.ErrUniqueExists {
				t.Fatalf("Duplicate record did not result in a unique constraint error: "+
					"Expected %s, Got %s", badgerhold.ErrUniqueExists, err)
			}
		})

		t.Run("Upsert", func(t *testing.T) {
			update := &TestUnique{
				Name: "Upsert Name",
			}
			err = store.Insert(badgerhold.NextSequence(), update)

			if err != nil {
				t.Fatalf("Inserting record for upsert Unique testing failed: %s", err)
			}

			update.Name = item.Name

			err = store.Upsert(update.Key, update)
			if err != badgerhold.ErrUniqueExists {
				t.Fatalf("Duplicate record did not result in a unique constraint error: "+
					"Expected %s, Got %s", badgerhold.ErrUniqueExists, err)
			}
		})

		t.Run("UpdateMatching", func(t *testing.T) {
			update := &TestUnique{
				Name: "UpdateMatching Name",
			}
			err = store.Insert(badgerhold.NextSequence(), update)

			if err != nil {
				t.Fatalf("Inserting record for updatematching Unique testing failed: %s", err)
			}

			err = store.UpdateMatching(TestUnique{}, badgerhold.Where(badgerhold.Key).Eq(update.Key),
				func(r interface{}) error {
					record, ok := r.(*TestUnique)
					if !ok {
						return fmt.Errorf("Record isn't the correct type!  Got %T",
							r)
					}

					record.Name = item.Name

					return nil
				})
			if err != badgerhold.ErrUniqueExists {
				t.Fatalf("Duplicate record did not result in a unique constraint error: "+
					"Expected %s, Got %s", badgerhold.ErrUniqueExists, err)
			}

		})

		t.Run("Delete", func(t *testing.T) {
			err = store.Delete(item.Key, TestUnique{})
			if err != nil {
				t.Fatalf("Error deleting record for unique testing %s", err)
			}

			err = store.Insert(badgerhold.NextSequence(), item)
			if err != nil {
				t.Fatalf("Error inserting duplicate record that has been previously removed: %s", err)
			}
		})

	})
}

func TestIssue46ConcurrentIndexInserts(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		var wg sync.WaitGroup

		for i := range testData {
			wg.Add(1)
			go func(gt *testing.T, i int) {
				defer wg.Done()
				err := store.Insert(testData[i].Key, testData[i])
				if err != nil {
					gt.Fatalf("Error inserting test data for find test: %s", err)
				}
			}(t, i)
		}
		wg.Wait()
	})
}

func TestIssue46ConcurrentIndexUpserts(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		var wg sync.WaitGroup

		for i := range testData {
			wg.Add(1)
			go func(gt *testing.T, i int) {
				defer wg.Done()
				err := store.Upsert(testData[i].Key, testData[i])
				if err != nil {
					gt.Fatalf("Error inserting test data for find test: %s", err)
				}
			}(t, i)
		}
		wg.Wait()
	})
}

func TestIssue46ConcurrentIndexUpdate(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		insertTestData(t, store)
		var wg sync.WaitGroup

		for i := range testData {
			wg.Add(1)
			go func(gt *testing.T, i int) {
				defer wg.Done()
				err := store.Update(testData[i].Key, testData[i])
				if err != nil {
					gt.Fatalf("Error inserting test data for find test: %s", err)
				}
			}(t, i)
		}
		wg.Wait()
	})
}

func TestIssue46ConcurrentIndexUpdateMatching(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		insertTestData(t, store)
		var wg sync.WaitGroup

		for i := range testData {
			wg.Add(1)
			go func(gt *testing.T, i int) {
				defer wg.Done()
				err := store.UpdateMatching(ItemTest{}, badgerhold.Where(badgerhold.Key).
					Eq(testData[i].Key), func(r interface{}) error {
					record, ok := r.(*ItemTest)
					if !ok {
						return fmt.Errorf("Record isn't the correct type!  Got %T",
							r)
					}
					record.Name = record.Name

					return nil
				})
				if err != nil {
					gt.Fatalf("Error updating test data for test: %s", err)
				}
			}(t, i)
		}
		wg.Wait()
	})
}

type CustomStorer struct{ Name string }

func (i *CustomStorer) Type() string { return "CustomStorer" }
func (i *CustomStorer) Indexes() map[string]badgerhold.Index {
	return map[string]badgerhold.Index{
		"Name": {
			IndexFunc: func(_ string, value interface{}) ([]byte, error) {
				return nil, errors.New("IndexFunc error")
			},
			Unique: false,
		},
	}
}

func TestInsertUpdateIndexError(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		data := &CustomStorer{
			Name: "Test Name",
		}
		err := store.Insert("testKey", data)
		if err == nil || err.Error() != "IndexFunc error" {
			t.Fatalf("Insert didn't fail! Expected IndexFunc error got %s", err)
		}
	})
}
