package shardis

import (
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

// Crc uses crc32 to hash given string
func Crc(key string) string {
	crc32q := crc32.MakeTable(0xD5828281)
	hashed := crc32.Checksum([]byte(key), crc32q)
	return fmt.Sprintf("%x", hashed)
}

// MdFive uses md5 to hash given string
func MdFive(key string) string {
	hashed := md5.Sum([]byte(key))
	return fmt.Sprintf("%x", hashed)
}

// ShaOne uses sha1 to hash given string
func ShaOne(key string) string {
	hashed := sha1.Sum([]byte(key))
	return fmt.Sprintf("%x", hashed)
}

var (
	//HashMethods maps the name to implementation of a hashing method
	HashMethods = map[string]interface{}{
		"crc32": Crc,
		"md5":   MdFive,
		"sha1":  ShaOne,
	}
)

// HashRing represents a ring of Redis instance involved in Shardis
type HashRing struct {
	HashMethod string
	Nodes      []string
	Replicas   int
	Ring       map[string]string
	SortedKeys []string
}

// NewHashRing initilizes a new HashRing object
func NewHashRing(nodes []string, hashMethod string, replicas int) (*HashRing, error) {
	hashRing := &HashRing{
		HashMethod: hashMethod,
		Nodes:      make([]string, 0),
		Replicas:   replicas,
		Ring:       make(map[string]string),
		SortedKeys: make([]string, 0),
	}
	for _, node := range nodes {
		hashRing.AddNode(node)
	}

	return hashRing, nil
}

// AddNode add a new node to the HashRing given the node name
func (hashRing *HashRing) AddNode(node string) error {
	hashRing.Nodes = append(hashRing.Nodes, node)
	for i := 0; i < hashRing.Replicas; i++ {
		method, ok := HashMethods[hashRing.HashMethod]
		if !ok {
			return errors.New("Hash Method not exist")
		}

		ringKey := method.(func(string) string)(fmt.Sprintf("%s:%d", node, i))
		hashRing.Ring[ringKey] = node
		hashRing.SortedKeys = append(hashRing.SortedKeys, ringKey)
	}
	sort.Strings(hashRing.SortedKeys)
	return nil
}

// RemoveNode removes the given node from HashRing
func (hashRing *HashRing) RemoveNode(node string) error {
	var idx int
	for i := 0; i < len(hashRing.Nodes); i++ {
		if strings.Compare(hashRing.Nodes[i], node) == 0 {
			idx = i
		}
	}
	hashRing.Nodes = append(hashRing.Nodes[:idx], hashRing.Nodes[idx+1:]...)

	keySlice := sort.StringSlice(hashRing.SortedKeys)
	for i := 0; i < hashRing.Replicas; i++ {
		method, ok := HashMethods[hashRing.HashMethod]
		if !ok {
			return errors.New("Hash Method not exist")
		}
		ringKey := method.(func(string) string)(fmt.Sprintf("%s:%d", node, i))

		delete(hashRing.Ring, ringKey)

		pos := sort.SearchStrings(keySlice, ringKey)
		keySlice = append(keySlice[:pos], keySlice[pos+1:]...)
	}
	hashRing.SortedKeys = keySlice
	return nil
}

// GetNode gets the
func (hashRing *HashRing) GetNode(key string) string {
	node, _ := hashRing.getNode(key)
	return node
}

// getNode is helper function for GetNode
func (hashRing *HashRing) getNode(key string) (string, int) {
	if len(hashRing.Nodes) == 0 {
		return "", -1
	}

	method := HashMethods[hashRing.HashMethod]
	ringKey := method.(func(string) string)(key)
	node := hashRing.Ring[ringKey]

	keySlice := sort.StringSlice(hashRing.SortedKeys)
	idx := sort.SearchStrings(keySlice, ringKey)

	return node, idx
}

// min gets the minimum between two integers
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
