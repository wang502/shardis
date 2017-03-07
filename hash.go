package shardis

import (
    "errors"
    "fmt"
    "hash/crc32"
    "crypto/md5"
    "crypto/sha1"
    "sort"
    "strings"

)

func Crc(key string) string{
    crc32q := crc32.MakeTable(0xD5828281)
    hashed := crc32.Checksum([]byte(key), crc32q)
    return fmt.Sprintf("%x", hashed)
}

func MdFive(key string) string{
    hashed := md5.Sum([]byte(key))
    return fmt.Sprintf("%x", hashed)
}

func ShaOne(key string) string {
    hashed := sha1.Sum([]byte(key))
    return fmt.Sprintf("%x", hashed)
}

var (
    HashMethods = map[string]interface{}{
                    "crc32": Crc,
                    "md5": MdFive,
                    "sha1": ShaOne,
                }
)
type HashRing struct {
    HashMethod string
    Nodes []string
    Replicas int
    Ring map[string]string
    SortedKeys []string
}

func NewHashRing(nodes []string, hashMethod string, replicas int) (*HashRing, error){
    hashRing := &HashRing{
                    HashMethod: hashMethod,
                    Nodes: make([]string, 0),
                    Replicas: replicas,
                    Ring: make(map[string]string),
                    SortedKeys: make([]string, 0),
                }
    for i, node := range nodes {
        hashRing.AddNode(i, node)
    }

    return hashRing, nil
}

func (hashRing *HashRing) AddNode(idx int, node string) error {
    hashRing.Nodes = append(hashRing.Nodes, node)
    for i:=0; i<hashRing.Replicas; i++{
        method, ok:= HashMethods[hashRing.HashMethod]
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

func (hashRing *HashRing) RemoveNode(node string) error {
    var idx int
    for i:=0; i<len(hashRing.Nodes); i++ {
        if strings.Compare(hashRing.Nodes[i], node) == 0 {
            idx = i
        }
    }
    hashRing.Nodes = append(hashRing.Nodes[:idx], hashRing.Nodes[idx+1:]...)

    keySlice := sort.StringSlice(hashRing.SortedKeys)
    for i:=0; i<hashRing.Replicas; i++ {
      method, ok:= HashMethods[hashRing.HashMethod]
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

func (hashRing *HashRing) GetNode(key string) string {
    node, _ := hashRing.getNode(key)
    return node
}

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

func min(x, y int) int {
    if x < y {
        return x
    }
    return y
}
