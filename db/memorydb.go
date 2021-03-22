/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package db

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"os"
	"path"
	"sort"
	"sync"
)

// This function is always called first
func init() {
	dbConstructor := func(dir string) (DB, error) {
		return newMemoryDB(dir)
	}
	registorDBConstructor(MemoryImpl, dbConstructor)
}

func newMemoryDB(dir string) (DB, error) {
	var db map[string][]byte

	filePath := path.Join(dir, "database")

	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file) //
		err = decoder.Decode(&db)

		if err != nil {
			return nil, err
		}
	}

	file.Close()

	if db == nil {
		db = make(map[string][]byte)
	}

	database := &memorydb{
		db:  db,
		dir: filePath,
	}

	return database, nil
}

//=========================================================
// DB Implementation
//=========================================================

// Enforce database and transaction implements interfaces
var _ DB = (*memorydb)(nil)

type memorydb struct {
	lock sync.Mutex
	db   map[string][]byte
	dir  string
}

func (db *memorydb) Type() string {
	return "memorydb"
}

func (db *memorydb) Set(key, value []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	db.db[string(key)] = value
}

func (db *memorydb) Delete(key []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	delete(db.db, string(key))
}

func (db *memorydb) Get(key []byte) []byte {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	return db.db[string(key)]
}

func (db *memorydb) Exist(key []byte) bool {
	db.lock.Lock()
	defer db.lock.Unlock()

	key = convNilToBytes(key)

	_, ok := db.db[string(key)]

	return ok
}

func (db *memorydb) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()

	file, err := os.OpenFile(db.dir, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(db.db)
	}
	file.Close()
}

func (db *memorydb) NewTx() Transaction {

	return &memoryTransaction{
		db:        db,
		opList:    list.New(),
		isDiscard: false,
		isCommit:  false,
	}
}

func (db *memorydb) NewBulk() Bulk {

	return &memoryBulk{
		db:        db,
		opList:    list.New(),
		isDiscard: false,
		isCommit:  false,
	}
}

//=========================================================
// Transaction Implementation
//=========================================================

type memoryTransaction struct {
	txLock    sync.Mutex
	db        *memorydb
	opList    *list.List
	isDiscard bool
	isCommit  bool
}

type txOp struct {
	isSet bool
	key   []byte
	value []byte
}

func (transaction *memoryTransaction) Set(key, value []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	transaction.opList.PushBack(&txOp{true, key, value})
}

func (transaction *memoryTransaction) Delete(key []byte) {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	key = convNilToBytes(key)

	transaction.opList.PushBack(&txOp{false, key, nil})
}

func (transaction *memoryTransaction) Commit() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	if transaction.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if transaction.isCommit {
		panic("Commit occures two times")
	}

	db := transaction.db

	db.lock.Lock()
	defer db.lock.Unlock()

	for e := transaction.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.db[string(op.key)] = op.value
		} else {
			delete(db.db, string(op.key))
		}
	}

	transaction.isCommit = true
}

func (transaction *memoryTransaction) Discard() {
	transaction.txLock.Lock()
	defer transaction.txLock.Unlock()

	transaction.isDiscard = true
}

//=========================================================
// Bulk Implementation
//=========================================================

type memoryBulk struct {
	txLock    sync.Mutex
	db        *memorydb
	opList    *list.List
	isDiscard bool
	isCommit  bool
}

func (bulk *memoryBulk) Set(key, value []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)
	value = convNilToBytes(value)

	bulk.opList.PushBack(&txOp{true, key, value})
}

func (bulk *memoryBulk) Delete(key []byte) {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	key = convNilToBytes(key)

	bulk.opList.PushBack(&txOp{false, key, nil})
}

func (bulk *memoryBulk) Flush() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	if bulk.isDiscard {
		panic("Commit after dicard tx is not allowed")
	} else if bulk.isCommit {
		panic("Commit occures two times")
	}

	db := bulk.db

	db.lock.Lock()
	defer db.lock.Unlock()

	for e := bulk.opList.Front(); e != nil; e = e.Next() {
		op := e.Value.(*txOp)
		if op.isSet {
			db.db[string(op.key)] = op.value
		} else {
			delete(db.db, string(op.key))
		}
	}

	bulk.isCommit = true
}

func (bulk *memoryBulk) DiscardLast() {
	bulk.txLock.Lock()
	defer bulk.txLock.Unlock()

	bulk.isDiscard = true
}

//=========================================================
// Iterator Implementation
//=========================================================

type memoryIterator struct {
	start     []byte
	end       []byte
	reverse   bool
	keys      []string
	isInvalid bool
	cursor    int
	db        *memorydb
}

func isKeyInRange(key []byte, start []byte, end []byte, reverse bool) bool {
	if reverse {
		if start != nil && bytes.Compare(start, key) < 0 {
			return false
		}
		if end != nil && bytes.Compare(key, end) <= 0 {
			return false
		}
		return true
	}

	if bytes.Compare(key, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(end, key) <= 0 {
		return false
	}
	return true

}

func (db *memorydb) Iterator(start, end []byte) Iterator {
	db.lock.Lock()
	defer db.lock.Unlock()

	var reverse bool

	// if end is bigger then start, then reverse order
	if bytes.Compare(start, end) == 1 {
		reverse = true
	} else {
		reverse = false
	}

	var keys sort.StringSlice

	for key := range db.db {
		if isKeyInRange([]byte(key), start, end, reverse) {
			keys = append(keys, key)
		}
	}
	if reverse {
		sort.Sort(sort.Reverse(keys))
	} else {
		sort.Strings(keys)
	}

	return &memoryIterator{
		start:     start,
		end:       end,
		reverse:   reverse,
		isInvalid: false,
		keys:      keys,
		cursor:    0,
		db:        db,
	}
}

func (iter *memoryIterator) Next() {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	iter.cursor++
}

func (iter *memoryIterator) Valid() bool {
	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	return 0 <= iter.cursor && iter.cursor < len(iter.keys)
}

func (iter *memoryIterator) Key() (key []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	return []byte(iter.keys[iter.cursor])
}

func (iter *memoryIterator) Value() (value []byte) {
	if !iter.Valid() {
		panic("Iterator is Invalid")
	}

	key := []byte(iter.keys[iter.cursor])

	return iter.db.Get(key)
}
