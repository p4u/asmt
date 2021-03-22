package db

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/guptarohit/asciigraph"
)

type testKeyType int64

// Simple execution cmd
// go test -run=XXX -bench=. -benchmem -cpuprofile=cpu.out -memprofile=mem.out -timeout 20m -benchtime=1m

var valueLen = 256
var testSetSize = int64(10000000)
var batchSize = 100

// parameters for drawing graph
var graphCountingPeriod = 10000
var graphWidth = 120
var graphHeigh = 20

var deviceType = "ssd"

func init() {
	rand.Seed(time.Now().Unix())
}

func int64ToBytes(i testKeyType) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func printGraph(statics list.List) {
	if statics.Len() == 0 {
		return
	}
	// convert list to slice
	var staticsSlice = make([]float64, statics.Len())
	i := 0
	for e := statics.Front(); e != nil; e = e.Next() {
		staticsSlice[i] = e.Value.(float64)
		i++
	}

	graph := asciigraph.Plot(staticsSlice, asciigraph.Width(graphWidth), asciigraph.Height(graphHeigh))
	fmt.Println(graph)
}

func dirSizeKB(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size / 1024, err
}

func BenchmarkRandomWR(b *testing.B) {

	// generate a common random data set
	internal := map[testKeyType][]byte{}
	for i := 0; i < int(testSetSize); i++ {
		// generate a random value
		token := make([]byte, valueLen)
		internal[testKeyType(i)] = token
	}

	for dbType, dbConstructors := range dbImpls {

		// create db
		dbName := string(dbType)
		tmpDir, _ := ioutil.TempDir("", dbName)
		defer os.RemoveAll(tmpDir)

		dbInstance, _ := dbConstructors(tmpDir)
		var numOfWrite int64
		var idx testKeyType
		var statics list.List
		var startTime time.Time

		fmt.Printf("[%s]\ntestset_size: %d\nkey_len: %d\nval_len: %d\n",
			dbName, testSetSize, reflect.TypeOf(idx).Size(), valueLen)
		fmt.Println("device type: ssd")

		// write only
		b.Run(dbName+"-write", func(b *testing.B) {
			var i int
			for i = 0; i < b.N; i++ {
				if i != 0 && i%graphCountingPeriod == 0 {
					startTime = time.Now()
				}
				idx = testKeyType((int64(rand.Int()) % testSetSize)) // pick a random key
				rand.Read(internal[idx])                             // generate a random data

				dbInstance.Set(
					int64ToBytes(testKeyType(idx)),
					internal[idx],
				)
				if i != 0 && i%graphCountingPeriod == 0 {
					endTime := time.Now().Sub(startTime).Seconds()
					statics.PushBack(endTime)
				}
			}
			numOfWrite += int64(i)
		})

		printGraph(statics)
		statics.Init()

		// read only
		b.Run(dbName+"-read", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if i != 0 && i%graphCountingPeriod == 0 {
					startTime = time.Now()
				}
				idx = testKeyType((int64(rand.Int()) % testSetSize))
				originalVal := internal[idx]

				retrievedVal := dbInstance.Get(int64ToBytes(testKeyType(idx)))

				if len(retrievedVal) != 0 {
					if len(retrievedVal) != valueLen {
						b.Errorf("Expected length %X for %v, got %X",
							valueLen, idx, len(retrievedVal))
						break
					} else if !bytes.Equal(retrievedVal, originalVal) {
						b.Errorf("Expected %v for %v, got %v",
							originalVal, idx, retrievedVal)
						break
					}
				}

				if i != 0 && i%graphCountingPeriod == 0 {
					endTime := time.Now().Sub(startTime).Seconds()
					statics.PushBack(endTime)
				}
			}
		})

		printGraph(statics)
		statics.Init()

		// write and read
		b.Run(dbName+"-write-read", func(b *testing.B) {
			var i int
			for i = 0; i < b.N; i++ {
				if i != 0 && i%graphCountingPeriod == 0 {
					startTime = time.Now()
				}
				idx = testKeyType(int64(rand.Int()) % testSetSize) // pick a random key
				rand.Read(internal[idx])                           // generate a random data

				dbInstance.Set(
					int64ToBytes(testKeyType(idx)),
					internal[idx],
				)

				originalVal := internal[idx]

				retrievedVal := dbInstance.Get(int64ToBytes(testKeyType(idx)))

				if len(retrievedVal) != 0 {
					if len(retrievedVal) != valueLen {
						b.Errorf("Expected length %X for %v, got %X",
							valueLen, idx, len(retrievedVal))
						break
					} else if !bytes.Equal(retrievedVal, originalVal) {
						b.Errorf("Expected %v for %v, got %v",
							originalVal, idx, retrievedVal)
						break
					}
				}

				if i != 0 && i%graphCountingPeriod == 0 {
					endTime := time.Now().Sub(startTime).Seconds()
					statics.PushBack(endTime)
				}
			}
			numOfWrite += int64(i)
		})

		printGraph(statics)
		statics.Init()

		// close
		dbInstance.Close()

		size, err := dirSizeKB(tmpDir)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("* Total size of %s db: %v kb, Size of 1 write: %v byte\n", dbName, size, size*1024/numOfWrite)
		}
	}
}

func BenchmarkRandomBatchWR(b *testing.B) {

	// generate a common random data set
	internal := map[testKeyType][]byte{}
	for i := 0; i < int(testSetSize); i++ {
		// generate a random value
		token := make([]byte, valueLen)
		internal[testKeyType(i)] = token
	}

	for dbType, dbConstructors := range dbImpls {

		// create db
		dbName := string(dbType)
		tmpDir, _ := ioutil.TempDir("", dbName)
		defer os.RemoveAll(tmpDir)

		dbInstance, _ := dbConstructors(tmpDir)
		var numOfWrite int64
		var idx testKeyType
		var statics list.List
		var startTime time.Time

		fmt.Printf("[%s]\ntestset_size: %d\nkey_len: %d\nval_len: %d\n",
			dbName, testSetSize, reflect.TypeOf(idx).Size(), valueLen)
		fmt.Println("device type: " + deviceType)
		fmt.Printf("batch size: %d\n", batchSize)

		// write only
		b.Run(dbName+"-batch-write", func(b *testing.B) {
			var i int

			for i = 0; i < b.N; i++ {
				if i != 0 && i%graphCountingPeriod == 0 {
					startTime = time.Now()
				}
				tx := dbInstance.NewTx()

				for j := 0; j < batchSize; j++ {
					idx = testKeyType((int64(rand.Int()) % testSetSize)) // pick a random key
					rand.Read(internal[idx])                             // generate a random data

					tx.Set(
						int64ToBytes(testKeyType(idx)),
						internal[idx],
					)
				}
				tx.Commit()
				if i != 0 && i%graphCountingPeriod == 0 {
					endTime := time.Now().Sub(startTime).Seconds()
					statics.PushBack(endTime)
				}
			}
			numOfWrite += int64(i)
		})

		// print a graph
		printGraph(statics)
		statics.Init()

		// read only
		b.Run(dbName+"-read", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if i != 0 && i%graphCountingPeriod == 0 {
					startTime = time.Now()
				}

				idx = testKeyType((int64(rand.Int()) % testSetSize))
				originalVal := internal[idx]

				retrievedVal := dbInstance.Get(int64ToBytes(testKeyType(idx)))

				if len(retrievedVal) != 0 {
					if len(retrievedVal) != valueLen {
						b.Errorf("Expected length %X for %v, got %X",
							valueLen, idx, len(retrievedVal))
						break
					} else if !bytes.Equal(retrievedVal, originalVal) {
						b.Errorf("Expected %v for %v, got %v",
							originalVal, idx, retrievedVal)
						break
					}
				}
				if i != 0 && i%graphCountingPeriod == 0 {
					endTime := time.Now().Sub(startTime).Seconds()
					statics.PushBack(endTime)
				}
			}
		})

		// close
		dbInstance.Close()

		printGraph(statics)

		size, err := dirSizeKB(tmpDir)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("* Total size of %s db: %v kb, Size of 1 write: %v byte\n", dbName, size, size*1024/numOfWrite)
		}
	}
}

/*
[Refined Test Result]
goos: windows
goarch: amd64
pkg: github.com/aergoio/aergo-lib/db

[badgerdb-ssd-single]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
BenchmarkRandomWR/badgerdb-write-12 	            30000000	     29536 ns/op	    4983 B/op	      84 allocs/op
BenchmarkRandomWR/badgerdb-read-12  	            20000000	     47739 ns/op	   13154 B/op	      60 allocs/op
BenchmarkRandomWR/badgerdb-write-read-12         	20000000	     41990 ns/op	   10317 B/op	     140 allocs/op
* Total size of badgerdb db: 14544473 kb, Size of 1 write: 286 byte

[leveldb-ssd-single]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
BenchmarkRandomWR/leveldb-write-12               	 1000000	   1006445 ns/op	     532 B/op	       5 allocs/op
BenchmarkRandomWR/leveldb-read-12                	50000000	     18941 ns/op	    1160 B/op	      17 allocs/op
BenchmarkRandomWR/leveldb-write-read-12          	 1000000	    986440 ns/op	    1272 B/op	      11 allocs/op
* Total size of leveldb db: 567386 kb, Size of 1 write: 250 byte

[badgerdb-ssd-batch100]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
batch size: 100
BenchmarkRandomBatchWR/badgerdb-batch-write-12   	 2000000	    612737 ns/op	  383751 B/op	    4576 allocs/op
BenchmarkRandomBatchWR/badgerdb-read-12          	20000000	     53298 ns/op	   22177 B/op	      71 allocs/op
* Total size of badgerdb db: 74546912 kb, Size of 1 write: 25359 byte

[leveldb-ssd-batch100]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
batch size: 100
BenchmarkRandomBatchWR/leveldb-batch-write-12    	  300000	   6778458 ns/op	  127466 B/op	     399 allocs/op
BenchmarkRandomBatchWR/leveldb-read-12           	 5000000	    141176 ns/op	    4547 B/op	      34 allocs/op
* Total size of leveldb db: 2540271 kb, Size of 1 write: 8388 byte

[badgerdb-ssd-batch1000]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
batch size: 1000
BenchmarkRandomBatchWR1000/badgerdb-batch-write-12         	  200000	   5507060 ns/op	 3504344 B/op	   42743 allocs/op
BenchmarkRandomBatchWR1000/badgerdb-read-12                	20000000	     52636 ns/op	   21867 B/op	      73 allocs/op
* Total size of badgerdb db: 51315516 kb, Size of 1 write: 250103 byte

[leveldb-ssd-batch100]
testset_size: 10000000
key_len: 8
val_len: 256
device type: ssd
batch size: 1000
BenchmarkRandomBatchWR1000/leveldb-batch-write-12   	  	   20000	  70431430 ns/op	 1081956 B/op	    3906 allocs/op
BenchmarkRandomBatchWR1000/leveldb-read-12                 	 5000000	    138074 ns/op	    4194 B/op	      33 allocs/op
* Total size of leveldb db: 2529393 kb, Size of 1 write: 86046 byte

[badgerdb-hdd-single]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
BenchmarkHddRandomWR/badgerdb-write-12                     	30000000	     27901 ns/op	    4809 B/op	      82 allocs/op
BenchmarkHddRandomWR/badgerdb-read-12                      	20000000	     45765 ns/op	   14321 B/op	      68 allocs/op
BenchmarkHddRandomWR/badgerdb-write-read-12                 20000000	     39404 ns/op	    8693 B/op	     124 allocs/op
* Total size of badgerdb db: 14697136 kb, Size of 1 write: 289 byte

[leveldb-hdd-single]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
BenchmarkHddRandomWR/leveldb-write-12                      	   50000	  20975160 ns/op	     551 B/op	       4 allocs/op
BenchmarkHddRandomWR/leveldb-read-12                       100000000	     10375 ns/op	     988 B/op	      14 allocs/op
BenchmarkHddRandomWR/leveldb-write-read-12                 	   50000	  21144257 ns/op	     994 B/op	       9 allocs/op
* Total size of leveldb db: 31940 kb, Size of 1 write: 272 byte

[badgerdb-hdd-batch100]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
batch size: 100
BenchmarkHddRandomBatchWR/badgerdb-batch-write-12          	 1000000	    675351 ns/op	  329398 B/op	    4007 allocs/op
BenchmarkHddRandomBatchWR/badgerdb-read-12                 	10000000	     87933 ns/op	   22713 B/op	      85 allocs/op
* Total size of badgerdb db: 24285947 kb, Size of 1 write: 24620 byte

[leveldb-hdd-batch100]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
batch size: 100
BenchmarkHddRandomBatchWR/leveldb-batch-write-12           	   30000	  45769594 ns/op	  136783 B/op	     399 allocs/op
BenchmarkHddRandomBatchWR/leveldb-read-12                  	20000000	     30579 ns/op	    3386 B/op	      19 allocs/op
* Total size of leveldb db: 878403 kb, Size of 1 write: 22430 byte

[badgerdb-hdd-batch1000]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
batch size: 1000
BenchmarkHddRandomBatchWR1000/badgerdb-batch-write-12      	  200000	   6599637 ns/op	 3724911 B/op	   43392 allocs/op
BenchmarkHddRandomBatchWR1000/badgerdb-read-12             	10000000	     99201 ns/op	   31710 B/op	     105 allocs/op
* Total size of badgerdb db: 51157857 kb, Size of 1 write: 249335 byte

[leveldb-hdd-batch1000]
testset_size: 10000000
key_len: 8
val_len: 256
device type: hdd
batch size: 1000
BenchmarkHddRandomBatchWR1000/leveldb-batch-write-12       	   10000	 209360868 ns/op	 1027546 B/op	    3363 allocs/op
BenchmarkHddRandomBatchWR1000/leveldb-read-12              	10000000	     87929 ns/op	    3971 B/op	      29 allocs/op
* Total size of leveldb db: 1689120 kb, Size of 1 write: 171236 byte

PASS
ok  	github.com/aergoio/aergo-lib/db	33617.451s
*/
