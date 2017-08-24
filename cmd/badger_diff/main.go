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

// TODO: This project should really be in the dgraph repo, since it contains
// intimate knowledge of the value types in dgraph. E.g. posting lists.

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

	countA := 0
	countB := 0

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
			countA++
			countB++
		} else if keyCmp < 0 {
			keyMismatch("A", itemA)
			exit = 1
			itA.Next()
			countA++
		} else {
			keyMismatch("B", itemB)
			exit = 1
			itB.Next()
			countB++
		}

	}
	for itA.Valid() {
		exit = 1
		keyMismatch("A", itA.Item())
		itA.Next()
		countA++
	}
	for itB.Valid() {
		exit = 1
		keyMismatch("B", itB.Item())
		itB.Next()
		countB++
	}

	fmt.Println("\nSummary:")
	fmt.Printf("Num keys(A): %d\n", countA)
	fmt.Printf("Num keys(B): %d\n", countB)

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
%v%vV(B) %d:
%v%v`,
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
%v%v`,
		label,
		hex.Dump(item.Key()),
		label,
		item.UserMeta(),
		hex.Dump(item.Value()),
		niceValue(item.Value()),
	)
}

func niceValue(v []byte) string {

	var result string

	var pl protos.PostingList
	err := pl.Unmarshal(v)
	if err == nil {
		result += fmt.Sprintf("Pretty: %+v\n", pl)
	}

	var su protos.SchemaUpdate
	err = su.Unmarshal(v)
	if err == nil {
		result += fmt.Sprintf("Pretty: %+v\n", su)
	}

	if result == "" {
		return "Pretty: unknown conversion"
	}
	return result
}
