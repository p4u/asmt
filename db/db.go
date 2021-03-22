/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

/*
Package db is an wrapper of key-value database implementations. Currently, this supports badgerdb (https://github.com/dgraph-io/badger).

Basic Usage

You can create database using a newdb func like this
 database := NewDB(BadgerImpl, "./test")

A first argument is a backend db type to use, and a second is a root directory to store db files.
After creating db, you can write, read or delete single key-value using funcs in DB interface.
 // write data
 database.Set([]byte("key"), []byte("val"))

 // read data
 read := Get([]byte("key"))

 // delete data
 database.Delete([]byte("key"))

Transaction

A Transaction is a bulk set of operations to ensure atomic success or fail.
 // create a new transaction
 tx := database.NewTX(true)

 // reserve writing
 tx.Set([]byte("keyA"), []byte("valA"))
 tx.Set([]byte("keyB"), []byte("valB"))

 // Get will return a value reserved to write in this transaction
 mustBeValA := tx.Get([]byte("keyA"))

 // Perform writing
 tx.Commit()
If you want to cancel and discard operations in tx, then you must call Discard() func to prevent a memory leack
 // If you create a tx, but do not commit, than you have to call this
 tx.Discard()

Iterator

An iteractor provides a way to get all keys sequentially.
 // create an iterator that covers all range
 for iter := database.Iterator(nil, nil); iter.Valid(); iter.Next() {
	 // print each key-value pair
	 fmt.Printf("%s = %s", string(iter.Key()), string(iter.Value()))
 }

You can find more detail usages at a db_test.go file
*/
package db

import (
	"fmt"

	"github.com/aergoio/aergo-lib/log"
)

var dbImpls = map[ImplType]dbConstructor{}
var logger *extendedLog

func registorDBConstructor(dbimpl ImplType, constructor dbConstructor) {
	dbImpls[dbimpl] = constructor
}

// NewDB creates new database or load existing database in the directory
func NewDB(dbimpltype ImplType, dir string) DB {
	logger = &extendedLog{Logger: log.NewLogger("db")}
	db, err := dbImpls[dbimpltype](dir)

	if err != nil {
		panic(fmt.Sprintf("Fail to Create New DB: %v", err))
	}

	return db
}

func convNilToBytes(byteArray []byte) []byte {
	if byteArray == nil {
		return []byte{}
	}
	return byteArray
}
