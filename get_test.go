// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold_test

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/timshannon/badgerhold/v4"
)

func TestGet(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for get test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from badgerhold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}
	})
}

func TestIssue36(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		type Tag1 struct {
			ID uint64 `badgerholdKey`
		}

		type Tag2 struct {
			ID uint64 `badgerholdKey:"Key"`
		}

		type Tag3 struct {
			ID uint64 `badgerhold:"key"`
		}

		type Tag4 struct {
			ID uint64 `badgerholdKey:""`
		}

		data1 := []*Tag1{{}, {}, {}}
		for i := range data1 {
			ok(t, store.Insert(badgerhold.NextSequence(), data1[i]))
			equals(t, uint64(i), data1[i].ID)
		}

		data2 := []*Tag2{{}, {}, {}}
		for i := range data2 {
			ok(t, store.Insert(badgerhold.NextSequence(), data2[i]))
			equals(t, uint64(i), data2[i].ID)
		}

		data3 := []*Tag3{{}, {}, {}}
		for i := range data3 {
			ok(t, store.Insert(badgerhold.NextSequence(), data3[i]))
			equals(t, uint64(i), data3[i].ID)
		}

		data4 := []*Tag4{{}, {}, {}}
		for i := range data4 {
			ok(t, store.Insert(badgerhold.NextSequence(), data4[i]))
			equals(t, uint64(i), data4[i].ID)
		}

		// Get
		for i := range data1 {
			get1 := &Tag1{}
			ok(t, store.Get(data1[i].ID, get1))
			equals(t, data1[i], get1)
		}

		for i := range data2 {
			get2 := &Tag2{}
			ok(t, store.Get(data2[i].ID, get2))
			equals(t, data2[i], get2)
		}

		for i := range data3 {
			get3 := &Tag3{}
			ok(t, store.Get(data3[i].ID, get3))
			equals(t, data3[i], get3)
		}

		for i := range data4 {
			get4 := &Tag4{}
			ok(t, store.Get(data4[i].ID, get4))
			equals(t, data4[i], get4)
		}

		// Find

		for i := range data1 {
			var find1 []*Tag1
			ok(t, store.Find(&find1, badgerhold.Where(badgerhold.Key).Eq(data1[i].ID)))
			assert(t, len(find1) == 1, "incorrect rows returned")
			equals(t, find1[0], data1[i])
		}

		for i := range data2 {
			var find2 []*Tag2
			ok(t, store.Find(&find2, badgerhold.Where(badgerhold.Key).Eq(data2[i].ID)))
			assert(t, len(find2) == 1, "incorrect rows returned")
			equals(t, find2[0], data2[i])
		}

		for i := range data3 {
			var find3 []*Tag3
			ok(t, store.Find(&find3, badgerhold.Where(badgerhold.Key).Eq(data3[i].ID)))
			assert(t, len(find3) == 1, "incorrect rows returned")
			equals(t, find3[0], data3[i])
		}

		for i := range data4 {
			var find4 []*Tag4
			ok(t, store.Find(&find4, badgerhold.Where(badgerhold.Key).Eq(data4[i].ID)))
			assert(t, len(find4) == 1, "incorrect rows returned")
			equals(t, find4[0], data4[i])
		}
	})
}

func TestTxGetBadgerError(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for TxGet test: %s", err)
		}

		txn := store.Badger().NewTransaction(false)
		txn.Discard()

		result := &ItemTest{}
		err = store.TxGet(txn, key, result)
		if err != badger.ErrDiscardedTxn {
			t.Fatalf("TxGet didn't fail! Expected %s got %s", badger.ErrDiscardedTxn, err)
		}
	})
}
