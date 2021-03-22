package asmtree

import (
	"bytes"
	"fmt"
	"path"
	"sync/atomic"
	"time"

	"go.vocdoni.io/dvote/censustree"
	"go.vocdoni.io/dvote/log"

	"git.sr.ht/~sircmpwn/go-bare"
	"github.com/p4u/asmt/db"
	asmt "github.com/p4u/asmt/smt"
)

// We use go-bare for export/import the trie. In order to support
// big census (up to 8 Million entries) we need to increase the maximums.
const bareMaxArrayLength uint64 = 1024 * 1014 * 8 // 8 Million

const bareMaxUnmarshalBytes uint64 = 1024 * 1024 * 200 // 200 MiB

type Tree struct {
	Tree           *asmt.Trie
	db             db.DB
	public         uint32
	lastAccessUnix int64 // a unix timestamp, used via sync/atomic
	size           uint64
	snapshotRoot   []byte // if not nil, this trie is considered an inmutable snapshot
	snapshotSize   uint64
}

type Proof struct {
	Bitmap   []byte
	Length   int
	Siblings [][]byte
	Value    []byte
}

type exportElement struct {
	Key   []byte `bare:"key"`
	Value []byte `bare:"value"`
}

type exportData struct {
	Elements []exportElement `bare:"elements"`
}

const (
	MaxKeySize   = 256
	MaxValueSize = 256
	dbRootPrefix = "this is the last root for the SMT tree"
)

// NewTree initializes a new AergoSMT tree following the censustree.Tree interface specification.
func NewTree(name, storageDir string) (censustree.Tree, error) {
	tr := &Tree{}
	err := tr.Init(name, storageDir)
	return tr, err
}

// newTree opens or creates a merkle tree under the given storage.
func newTree(name, storageDir string) (*asmt.Trie, db.DB, error) {
	dir := path.Join(storageDir, name)
	log.Debugf("creating new tree on %s", dir)
	d := db.NewDB(db.LevelImpl, dir)
	root := d.Get([]byte(dbRootPrefix))
	tr := asmt.NewTrie(root, asmt.Hasher, d)
	if root != nil {
		if err := tr.LoadCache(root); err != nil {
			return nil, nil, err
		}
	}
	return tr, d, nil
}

// Init initializes a new asmt tree
func (t *Tree) Init(name, storageDir string) error {
	var err error
	t.Tree, t.db, err = newTree(name, storageDir)
	t.updateAccessTime()
	t.size = 0
	return err
}

func (t *Tree) MaxKeySize() int {
	return MaxKeySize
}

// LastAccess returns the last time the Tree was accessed, in the form of a unix
// timestamp.
func (t *Tree) LastAccess() int64 {
	return atomic.LoadInt64(&t.lastAccessUnix)
}

func (t *Tree) updateAccessTime() {
	atomic.StoreInt64(&t.lastAccessUnix, time.Now().Unix())
}

// Publish makes a merkle tree available for queries.
// Application layer should check IsPublish() before considering the Tree available.
func (t *Tree) Publish() {
	atomic.StoreUint32(&t.public, 1)
}

// UnPublish makes a merkle tree not available for queries
func (t *Tree) UnPublish() {
	atomic.StoreUint32(&t.public, 0)
}

// IsPublic returns true if the tree is available
func (t *Tree) IsPublic() bool {
	return atomic.LoadUint32(&t.public) == 1
}

// Commit saves permanently the tree on disk
func (t *Tree) Commit() error {
	if t.snapshotRoot != nil {
		return fmt.Errorf("cannot commit to a snapshot trie")
	}
	err := t.Tree.Commit()
	if err != nil {
		return err
	}
	t.db.Set([]byte(dbRootPrefix), t.Root())
	return nil
}

// Add adds a new claim to the merkle tree
// A claim is composed of two parts: index and value
//  1.index is mandatory, the data will be used for indexing the claim into to merkle tree
//  2.value is optional, the data will not affect the indexing
func (t *Tree) Add(index, value []byte) error {
	t.updateAccessTime()
	if t.snapshotRoot != nil {
		return fmt.Errorf("cannot add to a snapshot trie")
	}
	if len(index) < 4 {
		return fmt.Errorf("index too small (%d), minimum size is 4 bytes", len(index))
	}
	if len(value) > MaxValueSize {
		return fmt.Errorf("index or value claim data too big")
	}
	_, err := t.Tree.Update([][]byte{asmt.Hasher(index)}, [][]byte{asmt.Hasher(value)})
	if err != nil {
		return err
	}
	atomic.StoreUint64(&t.size, 0) // TBD: improve this
	return t.Commit()
}

// AddBatch adds a list of indexes and values.
// The commit to disk is executed only once.
// The values slince could be empty or as long as indexes.
func (t *Tree) AddBatch(indexes, values [][]byte) ([]int, error) {
	var wrongIndexes []int
	t.updateAccessTime()
	if t.snapshotRoot != nil {
		return wrongIndexes, fmt.Errorf("cannot add to a snapshot trie")
	}
	if len(values) > 0 && len(indexes) != len(values) {
		return wrongIndexes, fmt.Errorf("indexes and values have different size")
	}
	var hashedIndexes [][]byte
	var hashedValues [][]byte
	var value []byte
	for i, key := range indexes {
		if len(key) < 4 {
			wrongIndexes = append(wrongIndexes, i)
			continue
		}
		value = nil
		if len(values) > 0 {
			if len(values[i]) > MaxValueSize {
				wrongIndexes = append(wrongIndexes, i)
				continue
			}
			value = values[i]
		}
		hashedIndexes = append(hashedIndexes, asmt.Hasher(key))
		hashedValues = append(hashedValues, asmt.Hasher(value))
	}
	_, err := t.Tree.Update(hashedIndexes, hashedValues)
	if err != nil {
		return wrongIndexes, err
	}
	atomic.StoreUint64(&t.size, 0) // TBD: improve this
	return wrongIndexes, t.Commit()
}

// Get returns the value of a key
func (t *Tree) Get(key []byte) []byte { // Do something with error
	var value []byte
	if t.snapshotRoot != nil {
		value, _ = t.Tree.GetWithRoot(key, t.snapshotRoot)
	} else {
		value, _ = t.Tree.Get(key)
	}
	return value
}

// GenProof generates a merkle tree proof that can be later used on CheckProof() to validate it.
func (t *Tree) GenProof(index, value []byte) ([]byte, error) {
	t.updateAccessTime()
	var err error
	var ap [][]byte
	var pvalue []byte
	var bitmap []byte
	var length int
	if t.snapshotRoot != nil {
		bitmap, ap, length, _, _, pvalue, err = t.Tree.MerkleProofCompressedR(
			asmt.Hasher(index),
			t.snapshotRoot)
		if err != nil {
			return nil, err
		}
	} else {
		bitmap, ap, length, _, _, pvalue, err = t.Tree.MerkleProofCompressed(
			asmt.Hasher(index))
		if err != nil {
			return nil, err
		}
	}
	//if !included {
	//	return nil, fmt.Errorf("not included")
	//}
	if !bytes.Equal(pvalue, asmt.Hasher(value)) {
		return nil, fmt.Errorf("incorrect value on genProof")
	}
	return bare.Marshal(&Proof{Bitmap: bitmap, Length: length, Siblings: ap, Value: pvalue})
}

// CheckProof validates a merkle proof and its data.
func (t *Tree) CheckProof(index, value, root, mproof []byte) (bool, error) {
	t.updateAccessTime()
	p := Proof{}
	if err := bare.Unmarshal(mproof, &p); err != nil {
		return false, err
	}
	if !bytes.Equal(p.Value, asmt.Hasher(value)) {
		return false, fmt.Errorf("values mismatch %x != %x", p.Value, asmt.Hasher(value))
	}
	if root != nil {
		return t.Tree.VerifyInclusionWithRootC(
			root,
			p.Bitmap,
			asmt.Hasher(index),
			p.Value,
			p.Siblings,
			p.Length), nil
	}
	if t.snapshotRoot != nil {
		return t.Tree.VerifyInclusionWithRootC(
			t.snapshotRoot,
			p.Bitmap,
			asmt.Hasher(index),
			p.Value,
			p.Siblings,
			p.Length), nil
	}
	return t.Tree.VerifyInclusionC(
		p.Bitmap,
		asmt.Hasher(index),
		p.Value,
		p.Siblings,
		p.Length), nil
}

// Root returns the current root hash of the merkle tree
func (t *Tree) Root() []byte {
	t.updateAccessTime()
	if t.snapshotRoot != nil {
		return t.snapshotRoot
	}
	return t.Tree.Root
}

// Dump returns the whole merkle tree serialized in a format that can be used on Import.
// Byte seralization is performed using bare message protocol, it is a 40% size win over JSON.
func (t *Tree) Dump(root []byte) ([]byte, error) {
	t.updateAccessTime()
	if root == nil && t.snapshotRoot != nil {
		root = t.snapshotRoot
	}
	dump := exportData{}
	t.iterateWithRoot(root, nil, func(k, v []byte) bool {
		ee := exportElement{Key: make([]byte, len(k)), Value: make([]byte, len(v))}
		// Copy elements since it's not safe to hold on to the []byte values from Iterate
		copy(ee.Key, k[:])
		copy(ee.Value, v[:])
		dump.Elements = append(dump.Elements, ee)
		return false
	})
	bare.MaxArrayLength(bareMaxArrayLength)
	bare.MaxUnmarshalBytes(bareMaxUnmarshalBytes)

	return bare.Marshal(&dump)
}

// String returns a human readable representation of the tree.
func (t *Tree) String() string {
	s := bytes.Buffer{}
	t.iterate(t.snapshotRoot, func(k, v []byte) bool {
		s.WriteString(fmt.Sprintf("%x => %x\n", k, v))
		return false
	})
	return s.String()
}

// Size returns the number of leaf nodes on the merkle tree.
// TO-DO: root is currently ignored
func (t *Tree) Size(root []byte) (int64, error) {
	if t.snapshotRoot != nil {
		return int64(t.snapshotSize), nil
	}
	return int64(t.count()), nil
}

// DumpPlain returns the entire list of added claims for a specific root hash.
// First return parametre are the indexes and second the values.
// If root is not specified, the last one is used.
func (t *Tree) DumpPlain(root []byte) ([][]byte, [][]byte, error) {
	var indexes, values [][]byte
	var err error
	t.updateAccessTime()

	t.iterateWithRoot(root, nil, func(k, v []byte) bool {
		indexes = append(indexes, k)
		values = append(values, v)
		return false
	})

	return indexes, values, err
}

// ImportDump imports a partial or whole tree previously exported with Dump()
func (t *Tree) ImportDump(data []byte) error {
	t.updateAccessTime()
	if t.snapshotRoot != nil {
		return fmt.Errorf("cannot import to a snapshot")
	}
	census := new(exportData)
	bare.MaxArrayLength(bareMaxArrayLength)
	bare.MaxUnmarshalBytes(bareMaxUnmarshalBytes)
	if err := bare.Unmarshal(data, census); err != nil {
		return fmt.Errorf("importdump cannot unmarshal data: %w", err)
	}
	keys := [][]byte{}
	values := [][]byte{}
	for _, ee := range census.Elements {
		keys = append(keys, ee.Key)
		values = append(values, ee.Value)
	}
	_, err := t.Tree.Update(keys, values)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&t.size, 0) // TBD: improve this
	return t.Commit()
}

// Snapshot returns a Tree instance of a exiting merkle root.
// A Snapshot cannot be modified.
func (t *Tree) Snapshot(root []byte) (censustree.Tree, error) {
	exist, err := t.HashExists(root)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("root %x does not exist, cannot build snapshot", root)
	}
	return &Tree{Tree: t.Tree, public: t.public, snapshotRoot: root, snapshotSize: t.count()}, nil
}

func (t *Tree) Close() error {
	t.db.Close()
	return nil
}

// HashExists checks if a hash exists as a node in the merkle tree
func (t *Tree) HashExists(hash []byte) (bool, error) {
	t.updateAccessTime()
	return t.Tree.TrieRootExists(hash), nil
}

func (t *Tree) count() uint64 {
	if v := atomic.LoadUint64(&t.size); v != 0 {
		return v
	}
	counter := uint64(0)
	if err := t.Tree.Walk(t.snapshotRoot, func(*asmt.WalkResult) int32 {
		counter++
		return 0
	}); err != nil {
		return 0
	}
	atomic.StoreUint64(&t.size, counter)
	return counter
}

func (t *Tree) iterate(prefix []byte, callback func(key, value []byte) bool) {
	t.Tree.Walk(t.snapshotRoot, func(v *asmt.WalkResult) int32 {
		if callback(v.Key, v.Value) {
			return 1
		} else {
			return 0
		}
	})
}

func (t *Tree) iterateWithRoot(root, prefix []byte, callback func(key, value []byte) bool) {
	t.Tree.Walk(root, func(v *asmt.WalkResult) int32 {
		if callback(v.Key, v.Value) {
			return 1
		} else {
			return 0
		}
	})
}
