/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newLevelDB(dir)
	}
	registorDBConstructor(LevelImpl, dbConstructor)
}

func newLevelDB(dir string) (DB, error) {
	dbPath := filepath.Join(dir, "data.db")

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}

	database := &levelDB{
		db: db,
	}
	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*levelDB)(nil)

type levelDB struct {
	db *leveldb.DB
}

func (db *levelDB) Type() string {
	return "leveldb"
}

func (db *levelDB) Set(key, value []byte) {
	key = convNilToBytes(key)
	value = convNilToBytes(value)

	err := db.db.Put(key, value, &opt.WriteOptions{Sync: true})

	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *levelDB) Delete(key []byte) {
	key = convNilToBytes(key)

	err := db.db.Delete(key, &opt.WriteOptions{Sync: true})

	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
}

func (db *levelDB) Get(key []byte) []byte {
	key = convNilToBytes(key)
	res, err := db.db.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return []byte{}
		}
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	return res
}

func (db *levelDB) Exist(key []byte) bool {
	res, _ := db.db.Has(key, nil)
	return res
}

func (db *levelDB) Close() {
	db.db.Close()
}

func (db *levelDB) NewTx() Transaction {
	batch := new(leveldb.Batch)

	return &levelTransaction{db, batch, false, false}
}

func (db *levelDB) NewBulk() Bulk {
	batch := new(leveldb.Batch)

	return &levelBulk{db, batch, false, false}
}

//=========================================================
// Transaction Implementation
//=========================================================

type levelTransaction struct {
	db        *levelDB
	tx        *leveldb.Batch
	isDiscard bool
	isCommit  bool
}

/*
func (transaction *levelTransaction) Get(key []byte) []byte {
	panic(fmt.Sprintf("DO not support"))
}
*/

func (transaction *levelTransaction) Set(key, value []byte) {
	transaction.tx.Put(key, value)
}

func (transaction *levelTransaction) Delete(key []byte) {
	transaction.tx.Delete(key)
}

func (transaction *levelTransaction) Commit() {
	if transaction.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if transaction.isCommit {
		panic("Commit occures two times")
	}
	err := transaction.db.db.Write(transaction.tx, &opt.WriteOptions{Sync: true})
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	transaction.isCommit = true
}

func (transaction *levelTransaction) Discard() {
	transaction.isDiscard = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type levelBulk struct {
	db        *levelDB
	tx        *leveldb.Batch
	isDiscard bool
	isCommit  bool
}

func (bulk *levelBulk) Set(key, value []byte) {
	bulk.tx.Put(key, value)
}

func (bulk *levelBulk) Delete(key []byte) {
	bulk.tx.Delete(key)
}

func (bulk *levelBulk) Flush() {
	// do the same behavior that a transaction commit does
	// db.write internally will handle large transaction
	if bulk.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if bulk.isCommit {
		panic("Commit occures two times")
	}

	err := bulk.db.db.Write(bulk.tx, &opt.WriteOptions{Sync: true})
	if err != nil {
		panic(fmt.Sprintf("Database Error: %v", err))
	}
	bulk.isCommit = true
}

func (bulk *levelBulk) DiscardLast() {
	bulk.isDiscard = true
}

//=========================================================
// Iterator Implementation
//=========================================================

type levelIterator struct {
	start     []byte
	end       []byte
	reverse   bool
	iter      iterator.Iterator
	isInvalid bool
}

func (db *levelDB) Iterator(start, end []byte) Iterator {
	var reverse bool

	// if end is bigger then start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	iter := db.db.NewIterator(nil, nil)

	if reverse {
		if start == nil {
			iter.Last()
		} else {
			valid := iter.Seek(start)
			if valid {
				soakey := iter.Key()
				if bytes.Compare(start, soakey) < 0 {
					iter.Prev()
				}
			} else {
				iter.Last()
			}
		}
	} else {
		if start == nil {
			iter.First()
		} else {
			iter.Seek(start)
		}
	}
	return &levelIterator{
		iter:      iter,
		start:     start,
		end:       end,
		reverse:   reverse,
		isInvalid: false,
	}
}

func (iter *levelIterator) Next() {
	if iter.Valid() {
		if iter.reverse {
			iter.iter.Prev()
		} else {
			iter.iter.Next()
		}
	} else {
		panic("Iterator is Invalid")
	}
}

func (iter *levelIterator) Valid() bool {

	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	if err := iter.iter.Error(); err != nil {
		panic(err)
	}

	// If source is invalid, invalid.
	if !iter.iter.Valid() {
		iter.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var end = iter.end
	var key = iter.iter.Key()

	if iter.reverse {
		if end != nil && bytes.Compare(key, end) <= 0 {
			iter.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			iter.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

func (iter *levelIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Error(); err != nil {
		panic(err)
	}

	originalKey := iter.iter.Key()

	key = make([]byte, len(originalKey))
	copy(key, originalKey)

	return key
}

func (iter *levelIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is invalid")
	} else if err := iter.iter.Error(); err != nil {
		panic(err)
	}
	originalValue := iter.iter.Value()

	value = make([]byte, len(originalValue))
	copy(value, originalValue)

	return value
}
