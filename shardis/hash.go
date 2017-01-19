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

func Md(key string) string{
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
                    "md5": Md,
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
                    Nodes: make([]string, len(nodes)),
                    Replicas: replicas,
                    Ring: make(map[string]string),
                    SortedKeys: make([]string, replicas * len(nodes)),
                }
    for i, node := range nodes {
        hashRing.AddNode(i, node)
    }

    return hashRing, nil
}

func (hashRing *HashRing) AddNode(idx int, node string) error {
    hashRing.Nodes[idx] = node
    for i:=0; i<hashRing.Replicas; i++{
        method, ok:= HashMethods[hashRing.HashMethod]
        if !ok {
            return errors.New("Hash Method not exist")
        }
        ringKey := method.(func(string) string)(fmt.Sprintf("%s:%s", node, i))
        hashRing.Ring[ringKey] = node
        hashRing.SortedKeys[idx] = ringKey
    }

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

    for i:=0; i<hashRing.Replicas; i++ {
      method, ok:= HashMethods[hashRing.HashMethod]
      if !ok {
          return errors.New("Hash Method not exist")
      }
      ringKey := method.(func(string) string)(fmt.Sprintf("%s:%s", node, i))

      delete(hashRing.Ring, ringKey)
      pos := sort.SearchStrings(hashRing.SortedKeys, ringKey)
      hashRing.SortedKeys = append(hashRing.SortedKeys[:pos], hashRing.SortedKeys[pos+1:]...)
    }

    return nil
}
