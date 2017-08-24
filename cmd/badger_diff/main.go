package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/y"
	"github.com/dgraph-io/dgraph/protos"
)

func main() {

	badgerA := flag.String("a", "", "directory for badger A")
	badgerB := flag.String("b", "", "directory for badger B")
	flag.Parse()

	kvA := StartBadger(*badgerA)
	kvB := StartBadger(*badgerB)
	// Don't close the KVs we're only reading.

	itA := kvA.NewIterator(badger.DefaultIteratorOptions)
	itB := kvB.NewIterator(badger.DefaultIteratorOptions)
	itA.Seek(nil)
	itB.Seek(nil)

	exit := 0

	for itA.Valid() && itB.Valid() {
		itemA := itA.Item()
		itemB := itB.Item()
		keyCmp := bytes.Compare(itemA.Key(), itemB.Key())

		if keyCmp == 0 {
			if bytes.Compare(itemA.Value(), itemB.Value()) != 0 ||
				itemA.UserMeta() != itemB.UserMeta() {
				valueMismatch(itemA, itemB)
				exit = 1
			}
			itA.Next()
			itB.Next()
		} else if keyCmp < 0 {
			keyMismatch("A", itemA)
			exit = 1
			itA.Next()
		} else {
			keyMismatch("B", itemB)
			exit = 1
			itB.Next()
		}

	}
	for itA.Valid() {
		exit = 1
		keyMismatch("A", itA.Item())
		itA.Next()
	}
	for itB.Valid() {
		exit = 1
		keyMismatch("B", itB.Item())
		itB.Next()
	}

	os.Exit(exit)
}

func StartBadger(dir string) *badger.KV {
	opt := badger.DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	kv, err := badger.NewKV(&opt)
	y.Check(err)
	return kv
}

func valueMismatch(itemA, itemB *badger.KVItem) {
	fmt.Printf(`
Equal keys have different values:
K:
%vV(A) %d:
%v%v
V(B) %d:
%v%v
`,
		hex.Dump(itemA.Key()),
		itemA.UserMeta(),
		hex.Dump(itemA.Value()),
		niceValue(itemA.Value()),
		itemB.UserMeta(),
		hex.Dump(itemB.Value()),
		niceValue(itemB.Value()),
	)
}

func keyMismatch(label string, item *badger.KVItem) {
	fmt.Printf(`
Key present in one KV store but not the other:
K(%s):
%vV(%s) %d:
%v%v
`,
		label,
		hex.Dump(item.Key()),
		label,
		item.UserMeta(),
		hex.Dump(item.Value()),
		niceValue(item.Value()),
	)
}

func niceValue(v []byte) string {

	var pl protos.PostingList
	err := pl.Unmarshal(v)
	if err == nil {
		return fmt.Sprintf("Pretty: %+v", pl)
	}

	return "Pretty: unknown conversion"
}
