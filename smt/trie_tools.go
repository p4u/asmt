/**
 *  @file
 *  @copyright defined in aergo/LICENSE.txt
 */

package trie

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/p4u/asmt/db"
)

// LoadCache loads the first layers of the merkle tree given a root
// This is called after a node restarts so that it doesnt become slow with db reads
// LoadCache also updates the Root with the given root.
func (s *Trie) LoadCache(root []byte) error {
	if s.db.Store == nil {
		return fmt.Errorf("DB not connected to trie")
	}
	s.db.liveCache = make(map[Hash][][]byte)
	ch := make(chan error, 1)
	s.loadCache(root, nil, 0, s.TrieHeight, ch)
	s.Root = root
	return <-ch
}

// loadCache loads the first layers of the merkle tree given a root
func (s *Trie) loadCache(root []byte, batch [][]byte, iBatch, height int, ch chan<- (error)) {
	if height < s.CacheHeightLimit || len(root) == 0 {
		ch <- nil
		return
	}
	if height%4 == 0 {
		// Load the node from db
		s.db.lock.Lock()
		dbval := s.db.Store.Get(root[:HashLength])
		s.db.lock.Unlock()
		if len(dbval) == 0 {
			ch <- fmt.Errorf("the trie node %x is unavailable in the disk db, db may be corrupted", root)
			return
		}
		//Store node in cache.
		var node Hash
		copy(node[:], root)
		batch = s.parseBatch(dbval)
		s.db.liveMux.Lock()
		s.db.liveCache[node] = batch
		s.db.liveMux.Unlock()
		iBatch = 0
		if batch[0][0] == 1 {
			// if height == 0 this will also return
			ch <- nil
			return
		}
	}
	if iBatch != 0 && batch[iBatch][HashLength] == 1 {
		// Check if node is a leaf node
		ch <- nil
	} else {
		// Load subtree
		lnode, rnode := batch[2*iBatch+1], batch[2*iBatch+2]

		lch := make(chan error, 1)
		rch := make(chan error, 1)
		go s.loadCache(lnode, batch, 2*iBatch+1, height-1, lch)
		go s.loadCache(rnode, batch, 2*iBatch+2, height-1, rch)
		if err := <-lch; err != nil {
			ch <- err
			return
		}
		if err := <-rch; err != nil {
			ch <- err
			return
		}
		ch <- nil
	}
}

// Get fetches the value of a key by going down the current trie root.
func (s *Trie) Get(key []byte) ([]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.atomicUpdate = false
	return s.get(s.Root, key, nil, 0, s.TrieHeight)
}

// GetWithRoot fetches the value of a key by going down for the specified root.
func (s *Trie) GetWithRoot(key []byte, root []byte) ([]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.atomicUpdate = false
	if root == nil {
		root = s.Root
	}
	return s.get(root, key, nil, 0, s.TrieHeight)
}

// get fetches the value of a key given a trie root
func (s *Trie) get(root, key []byte, batch [][]byte, iBatch, height int) ([]byte, error) {
	if len(root) == 0 {
		// the trie does not contain the key
		return nil, nil
	}
	// Fetch the children of the node
	batch, iBatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, iBatch, batch)
	if err != nil {
		return nil, err
	}
	if isShortcut {
		if bytes.Equal(lnode[:HashLength], key) {
			return rnode[:HashLength], nil
		}
		// also returns nil if height 0 is not a shortcut
		return nil, nil
	}
	if bitIsSet(key, s.TrieHeight-height) {
		return s.get(rnode, key, batch, 2*iBatch+2, height-1)
	}
	return s.get(lnode, key, batch, 2*iBatch+1, height-1)
}

// WalkResult contains the key and value obtained with a Walk() operation
type WalkResult struct {
	Value []byte
	Key   []byte
}

// Walk finds all the trie stored values from left to right and calls callback.
// If callback returns a number diferent from 0, the walk will stop, else it will continue.
func (s *Trie) Walk(root []byte, callback func(*WalkResult) int32) error {
	walkc := make(chan *WalkResult)
	s.lock.RLock()
	defer s.lock.RUnlock()
	if root == nil {
		root = s.Root
	}
	s.atomicUpdate = false
	finishedWalk := make(chan (bool), 1)
	stop := int32(0)
	wg := sync.WaitGroup{} // WaitGroup to avoid Walk() return before all callback executions are finished.
	go func() {
		for {
			select {
			case <-finishedWalk:
				return
			case value := <-walkc:
				stopCallback := callback(value)
				wg.Done()
				// In order to avoid data races we need to check the current value of stop, while at the
				// same time we store our callback value. If our callback value is 0 means that we have
				// override the previous non-zero value, so we need to restore it.
				if cv := atomic.SwapInt32(&stop, stopCallback); cv != 0 || stopCallback != 0 {
					if stopCallback == 0 {
						atomic.StoreInt32(&stop, cv)
					}
					// We need to return (instead of break) in order to stop iterating if some callback returns non zero
					return
				}
			}
		}
	}()
	err := s.walk(walkc, &stop, root, nil, 0, s.TrieHeight, &wg)
	finishedWalk <- true
	wg.Wait()
	return err
}

// walk fetches the value of a key given a trie root
func (s *Trie) walk(walkc chan (*WalkResult), stop *int32, root []byte, batch [][]byte, ibatch, height int, wg *sync.WaitGroup) error {
	if len(root) == 0 || atomic.LoadInt32(stop) != 0 {
		// The sub tree is empty or stop walking
		return nil
	}
	// Fetch the children of the node
	batch, ibatch, lnode, rnode, isShortcut, err := s.loadChildren(root, height, ibatch, batch)
	if err != nil {
		return err
	}
	if isShortcut {
		wg.Add(1)
		walkc <- &WalkResult{Value: rnode[:HashLength], Key: lnode[:HashLength]}
		return nil
	}
	// Go left
	if err := s.walk(walkc, stop, lnode, batch, 2*ibatch+1, height-1, wg); err != nil {
		return err
	}
	// Go Right
	if err := s.walk(walkc, stop, rnode, batch, 2*ibatch+2, height-1, wg); err != nil {
		return err
	}
	return nil
}

// TrieRootExists returns true if the root exists in Database.
func (s *Trie) TrieRootExists(root []byte) bool {
	s.db.lock.RLock()
	dbval := s.db.Store.Get(root)
	s.db.lock.RUnlock()
	return len(dbval) != 0
}

// Commit stores the updated nodes to disk.
// Commit should be called for every block otherwise past tries
// are not recorded and it is not possible to revert to them
// (except if AtomicUpdate is used, which records every state).
func (s *Trie) Commit() error {
	if s.db.Store == nil {
		return fmt.Errorf("DB not connected to trie")
	}
	// NOTE The tx interface doesnt handle ErrTxnTooBig
	txn := s.db.Store.NewTx().(DbTx)
	s.StageUpdates(txn)
	txn.(db.Transaction).Commit()
	return nil
}

// StageUpdates requires a database transaction as input
// Unlike Commit(), it doesnt commit the transaction
// the database transaction MUST be commited otherwise the
// state ROOT will not exist.
func (s *Trie) StageUpdates(txn DbTx) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Commit the new nodes to database, clear updatedNodes and store the Root in pastTries for reverts.
	if !s.atomicUpdate {
		// if previously AtomicUpdate was called, then past tries is already updated
		s.updatePastTries()
	}
	s.db.commit(&txn)

	s.db.updatedNodes = make(map[Hash][][]byte)
	s.prevRoot = s.Root
}

// Stash rolls back the changes made by previous updates
// and loads the cache from before the rollback.
func (s *Trie) Stash(rollbackCache bool) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.Root = s.prevRoot
	if rollbackCache {
		// Making a temporary liveCache requires it to be copied, so it's quicker
		// to just load the cache from DB if a block state root was incorrect.
		s.db.liveCache = make(map[Hash][][]byte)
		ch := make(chan error, 1)
		s.loadCache(s.Root, nil, 0, s.TrieHeight, ch)
		err := <-ch
		if err != nil {
			return err
		}
	} else {
		s.db.liveCache = make(map[Hash][][]byte)
	}
	s.db.updatedNodes = make(map[Hash][][]byte)
	// also stash past tries created by Atomic update
	for i := len(s.pastTries) - 1; i >= 0; i-- {
		if bytes.Equal(s.pastTries[i], s.Root) {
			break
		} else {
			// remove from past tries
			s.pastTries = s.pastTries[:len(s.pastTries)-1]
		}
	}
	return nil
}
