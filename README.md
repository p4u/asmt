# asmt

asmt is a GoLang sparse merkle tree. 

A forked of [Aergo SMT](https://github.com/aergoio/aergo/tree/develop/pkg/trie) with minor style code modifications 
and some new methods: Walk() WalkWithRoot() and GetWithRoot().

This module includes an abstraction layer to make the SMT more developer friendly with a very easy to use API:

+ Init(name, dataDir string) error
+ LastAccess() (timestamp int64)
+ Add(key, value []byte) error
+ AddBatch(keys, values [][]byte) (failedIndexes []int, err error)
+ GenProof(key, value []byte) (mproof []byte, err error)
+ CheckProof(key, value, root, mproof []byte) (included bool, err error)
+ Root() []byte
+ HashExists(hash []byte) (bool, error)
+ Dump(root []byte) (data []byte, err error) // Dump must return a format compatible with ImportDump
+ DumpPlain(root []byte) (keys [][]byte, values [][]byte, err error)
+ ImportDump(data []byte) error
+ Size(root []byte) (int64, error)
+ Snapshot(root []byte) (Tree, error) // Snapshot is a frozen trie on a specific Root hash that cannot be modified

