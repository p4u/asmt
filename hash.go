package trie

import (
	"golang.org/x/crypto/blake2b"
)

// Hasher exports default hash function for trie
var Hasher = func(data ...[]byte) []byte {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	//hasher := sha256.New()
	for i := 0; i < len(data); i++ {
		hasher.Write(data[i])
	}
	return hasher.Sum(nil)
}
