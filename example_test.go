// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/timshannon/badgerhold"
)

type Item struct {
	ID       int
	Category string `badgerholdIndex:"Category"`
	Created  time.Time
}

func Example() {
	data := []Item{
		Item{
			ID:       0,
			Category: "blue",
			Created:  time.Now().Add(-4 * time.Hour),
		},
		Item{
			ID:       1,
			Category: "red",
			Created:  time.Now().Add(-3 * time.Hour),
		},
		Item{
			ID:       2,
			Category: "blue",
			Created:  time.Now().Add(-2 * time.Hour),
		},
		Item{
			ID:       3,
			Category: "blue",
			Created:  time.Now().Add(-20 * time.Minute),
		},
	}

	dir := tempdir()
	defer os.RemoveAll(dir)

	options := badgerhold.DefaultOptions
	options.Dir = dir
	options.ValueDir = dir
	store, err := badgerhold.Open(options)
	defer store.Close()

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	// insert the data in one transaction

	err = store.Badger().Update(func(tx *badger.Txn) error {
		for i := range data {
			err := store.TxInsert(tx, data[i].ID, data[i])
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	// Find all items in the blue category that have been created in the past hour
	var result []Item

	err = store.Find(&result, badgerhold.Where("Category").Eq("blue").And("Created").Ge(time.Now().Add(-1*time.Hour)))

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	fmt.Println(result[0].ID)
	// Output: 3

}
