package main

import (
    "flag"
    "log"
    "github.com/wang502/shardis/shardis"
)

func main(){
    confPath := flag.String("c", "config.json", "path to configuration file")
    config, err := shardis.InitConfig(*confPath)
    if err != nil {
        log.Printf(err.Error())
    }
    log.Println(*config)

    shard, _ := shardis.New(config)
    log.Println(shard)

    hashedKey := shardis.ShaOne("redis shard")
    log.Println(hashedKey)

    hashRing, _ := shardis.NewHashRing([]string{"name1", "name2"}, "md5", 10)
    log.Println(hashRing.Ring)

    hashRing.RemoveNode("name1")
    log.Println(hashRing.Ring)

    node := hashRing.GetNode("name2:9")
    log.Println(node)

    serverName := shard.GetServerName("abcd{name2:9}efg")
    log.Println(serverName)
}
