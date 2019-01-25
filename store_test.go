// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package badgerhold_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/timshannon/badgerhold"
)

func TestOpen(t *testing.T) {
	opt := badgerhold.DefaultOptions
	opt.Dir = tempdir()
	opt.ValueDir = opt.Dir
	store, err := badgerhold.Open(opt)
	if err != nil {
		t.Fatalf("Error opening %s: %s", opt.Dir, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	err = store.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll(opt.Dir)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBadger(t *testing.T) {
	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
		b := store.Badger()
		if b == nil {
			t.Fatalf("Badger is null in badgerhold")
		}
	})
}

// func TestRemoveIndex(t *testing.T) {
// 	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
// 		insertTestData(t, store)
// 		var item ItemTest

// 		err := store.RemoveIndex(item, "Category")
// 		if err != nil {
// 			t.Fatalf("Error removing index %s", err)
// 		}
// 	})
// }

// func TestReIndex(t *testing.T) {
// 	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
// 		insertTestData(t, store)
// 		var item ItemTest

// 		iName := indexName("ItemTest", "Category")

// 		err := store.RemoveIndex(item, "Category")
// 		if err != nil {
// 			t.Fatalf("Error removing index %s", err)
// 		}

// 		err = store.Badger().View(func(tx *badger.Tx) error {
// 			if tx.Bucket(iName) != nil {
// 				return fmt.Errorf("Index %s wasn't removed!", iName)
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		err = store.ReIndex(&item, nil)
// 		if err != nil {
// 			t.Fatalf("Error reindexing store: %v", err)
// 		}

// 		err = store.Badger().View(func(tx *badger.Tx) error {
// 			if tx.Bucket(iName) == nil {
// 				return fmt.Errorf("Index %s wasn't rebuilt!", iName)
// 			}
// 			return nil
// 		})

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 	})
// }

// func TestIndexExists(t *testing.T) {
// 	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
// 		insertTestData(t, store)
// 		err := store.Badger().View(func(tx *badger.Tx) error {
// 			if !store.IndexExists(tx, "ItemTest", "Category") {
// 				return fmt.Errorf("Index %s doesn't exist!", "ItemTest:Category")
// 			}
// 			return nil
// 		})

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 	})
// }

// type ItemTestClone ItemTest

// func TestReIndexWithCopy(t *testing.T) {
// 	testWrap(t, func(store *badgerhold.Store, t *testing.T) {
// 		insertTestData(t, store)

// 		var item ItemTestClone

// 		iName := indexName("ItemTestClone", "Category")

// 		err := store.ReIndex(&item, []byte("ItemTest"))
// 		if err != nil {
// 			t.Fatalf("Error reindexing store: %v", err)
// 		}

// 		err = store.Badger().View(func(tx *badger.Tx) error {
// 			if tx.Bucket(iName) == nil {
// 				return fmt.Errorf("Index %s wasn't rebuilt!", iName)
// 			}
// 			return nil
// 		})

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 	})
// }

func TestAlternateEncoding(t *testing.T) {
	opt := badgerhold.DefaultOptions
	opt.Dir = tempdir()
	opt.ValueDir = opt.Dir
	opt.Encoder = json.Marshal
	opt.Decoder = json.Unmarshal
	store, err := badgerhold.Open(opt)

	if err != nil {
		t.Fatalf("Error opening %s: %s", opt.Dir, err)
	}
	defer os.RemoveAll(opt.Dir)
	defer store.Close()

	insertTestData(t, store)

	tData := testData[3]

	var result []ItemTest

	store.Find(&result, badgerhold.Where(badgerhold.Key).Eq(tData.Key))

	if len(result) != 1 {
		if testing.Verbose() {
			t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), 1, result)
		}
		t.Fatalf("Find result count is %d wanted %d.", len(result), 1)
	}

	if !result[0].equal(&tData) {
		t.Fatalf("Results not equal! Wanted %v, got %v", tData, result[0])
	}

}

func TestGetUnknownType(t *testing.T) {
	opt := badgerhold.DefaultOptions
	opt.Dir = tempdir()
	opt.ValueDir = opt.Dir
	opt.Encoder = json.Marshal
	opt.Decoder = json.Unmarshal
	store, err := badgerhold.Open(opt)
	if err != nil {
		t.Fatalf("Error opening %s: %s", opt.Dir, err)
	}

	defer os.RemoveAll(opt.Dir)
	defer store.Close()

	type test struct {
		Test string
	}

	var result test
	err = store.Get("unknownKey", &result)
	if err != badgerhold.ErrNotFound {
		t.Errorf("Expected error of type ErrNotFound, not %T", err)
	}
}

// utilities

// testWrap creates a temporary database for testing and closes and cleans it up when
// completed.
func testWrap(t *testing.T, tests func(store *badgerhold.Store, t *testing.T)) {
	opt := testOptions()
	store, err := badgerhold.Open(opt)
	if err != nil {
		t.Fatalf("Error opening %s: %s", opt.Dir, err)
	}

	tests(store, t)
	err = store.Close()
	if err != nil {
		t.Fatalf("Error closing store: %s", err)
	}
	err = os.RemoveAll(opt.Dir)
	if err != nil {
		t.Fatalf("Error cleaning up store dir %s: %s", opt.Dir, err)
	}
}

type emptyLogger struct{}

func (e emptyLogger) Errorf(msg string, data ...interface{})   {}
func (e emptyLogger) Infof(msg string, data ...interface{})    {}
func (e emptyLogger) Warningf(msg string, data ...interface{}) {}

func testOptions() badgerhold.Options {
	opt := badgerhold.DefaultOptions
	opt.Dir = tempdir()
	opt.ValueDir = opt.Dir
	opt.Logger = emptyLogger{}
	return opt
}

// tempdir returns a temporary dir path.
func tempdir() string {
	name, err := ioutil.TempDir("", "badgerhold-")
	if err != nil {
		panic(err)
	}
	return name
}
